// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chi

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi/cmd_queue"
	common "github.com/altinity/clickhouse-operator/pkg/controller/common"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/poller"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/poller/domain"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/schemer"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

const (
	// replicationCatchUpPassTimeout bounds how long one reconcile pass waits for a host to catch
	// up. It is not a budget for the whole catch-up - the replica fetches from its peers whether
	// or not the operator is watching - so expiry costs nothing but the wait, and the CR is
	// re-enqueued to resume it.
	replicationCatchUpPassTimeout = 15 * time.Minute
	// replicationCatchUpRetryDelay spaces out those retries so a replica that never converges
	// re-checks periodically instead of spinning.
	replicationCatchUpRetryDelay = 1 * time.Minute
)

var (
	// errReplicationCatchUpNotFinished is returned when the per-pass wait expired with the host
	// still behind. It is distinct from a hard failure: the caller keeps the host out of the
	// Service and schedules another pass rather than aborting the reconcile.
	errReplicationCatchUpNotFinished = errors.New("host has not caught up within this reconcile pass")
)

// waitForIPAddresses waits for all pods to get IP address assigned
func (w *worker) waitForIPAddresses(ctx context.Context, cr *api.ClickHouseInstallation) {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted. CR polling IP: %s ", cr.GetName())
		return
	}

	if cr.IsStopped() {
		// No need to wait for stopped CHI
		return
	}

	l := w.a.V(1).M(cr)
	l.F().S().Info("wait for IP addresses to be assigned to all pods")

	// Let's limit polling time
	start := time.Now()
	timeout := 1 * time.Minute

	w.c.poll(ctx, cr, func(c *api.ClickHouseInstallation, e error) bool {
		// TODO fix later
		// status IPs list can be empty
		// Instead of doing in status:
		// 	podIPs := c.getPodsIPs(chi)
		//	cur.EnsureStatus().SetPodIPs(podIPs)
		// and here
		// c.Status.GetPodIPs()
		podIPs := w.c.getPodsIPs(ctx, cr)
		if len(podIPs) >= len(c.Status.GetPods()) {
			l.Info("all IP addresses are in place")
			// Stop polling
			return false
		}
		if time.Since(start) > timeout {
			l.Warning("not all IP addresses are in place but time has elapsed")
			// Stop polling
			return false
		}

		l.Info("still waiting - not all IP addresses are in place yet")

		// Continue polling
		return true
	})
}

// excludeHost excludes host from ClickHouse clusters if required
func (w *worker) excludeHost(ctx context.Context, host *api.Host) bool {
	log.V(1).M(host).F().S().Info("exclude host start")
	defer log.V(1).M(host).F().E().Info("exclude host end")

	if !w.shouldExcludeHost(ctx, host) {
		w.a.V(1).
			M(host).F().
			Info("No need to exclude host from cluster. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return false
	}

	w.a.V(1).
		M(host).F().
		Info("Exclude host from cluster. Host/shard/cluster: %d/%d/%s",
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)

	_ = w.excludeHostFromService(ctx, host)
	w.descendHostInClickHouseCluster(ctx, host)
	//w.excludeHostFromClickHouseCluster(ctx, host)
	return true
}

// completeQueries wait for running queries to complete
func (w *worker) completeQueries(ctx context.Context, host *api.Host) error {
	log.V(1).M(host).F().S().Info("complete queries start")
	defer log.V(1).M(host).F().E().Info("complete queries end")

	if w.shouldWaitQueries(host) {
		return w.waitHostHasNoActiveQueries(ctx, host)
	}

	return nil
}

// shouldIncludeHost determines whether host to be included into cluster after reconcile
func (w *worker) shouldIncludeHost(host *api.Host) bool {
	switch {
	case host.IsStopped():
		// No need to include stopped host
		return false
	}
	return true
}

// shouldWaitReplicationHost determines whether host to waited for replication lag to catch-up
func (w *worker) shouldWaitReplicationHost(host *api.Host) bool {
	switch {
	case host.IsStopped():
		w.a.V(1).
			M(host).F().
			Info("Host is stopped, no need to wait for replication to catch up. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return false

	case host.IsTroubleshoot():
		w.a.V(1).
			M(host).F().
			Info("Host is in troubleshoot, no need to wait for replication to catch up. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return false

	case host.GetShard().HostsCount() == 1:
		w.a.V(1).
			M(host).F().
			Info("Host is the only host in the shard (means no replication), no need to wait for replication to catch up. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return false

	case host.IsForceReplicaCatchUp():
		w.a.V(1).
			M(host).F().
			Info("Force replica catch-up after data loss. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return true

	case host.IsFirstInCluster():
		w.a.V(1).
			M(host).F().
			Info("Host is the first on the cluster, no need to wait for replication to catch up. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return false

	case chop.Config().Reconcile.Host.Wait.Replicas.All.IsTrue():
		w.a.V(1).
			M(host).F().
			Info("All replicas are explicitly requested to wait for replication to catch-up")
		return true

	case chop.Config().Reconcile.Host.Wait.Replicas.New.IsTrue():
		// New replicas have personal catch-up requirements
		if host.GetReconcileAttributes().GetStatus().Is(types.ObjectStatusCreated) {
			w.a.V(1).
				M(host).F().
				Info("New replicas are explicitly requested to wait for replication to catch-up and this is a new host replica ")
			return true
		}

		// This is not a new replica.
		// But this replica may have incomplete replication catch-up job still

		// Whether replication is listed as caught-up earlier
		if host.HasListedReplicaCaughtUp(w.c.namer.Name(interfaces.NameFQDN, host)) {
			w.a.V(1).
				M(host).F().
				Info("Replica is already listed as caught, no need to catch-up again")
			return false
		}

		// Host was seen before, but replication is not listed as caught-up, need to finish the replication
		w.a.V(1).
			M(host).F().
			Info("Host replica has never reached caught-up status, need to wait for replication to commence")
		return true
	}

	w.a.V(1).
		M(host).F().
		Info("Host replica is in unidentified replication position - report no need to catch-up ")
	return false
}

func healthWindowStep(counter int, ok bool, threshold int) (int, bool) {
	if !ok {
		return 0, false
	}
	counter++
	return counter, counter >= threshold
}

func onSoftTimeout(onTimeout string) (advance bool, pushMarker bool, err error) {
	if strings.EqualFold(onTimeout, api.CatchUpOnTimeoutProceed) {
		return true, false, nil
	}
	return false, false, common.ErrCRUDAbort
}

// includeHost includes host back into all activities - such as cluster, service, etc
func (w *worker) includeHost(ctx context.Context, host *api.Host) error {
	w.a.V(1).
		M(host).F().
		Info("Include host into cluster. Host/shard/cluster: %d/%d/%s",
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)

	catchUpGateEnabled := chop.Config().Reconcile.Host.Wait.Replicas.CatchUp.IsEnabled()
	// Catch up FIRST, ascend afterwards. A host that was excluded is still carrying the low
	// priority descendHostInClickHouseCluster gave it, so distributed queries keep preferring its
	// up-to-date peers for the duration of the wait. (A host that was never excluded - a brand new
	// one, or one the shard-safety guard declined to drain - is at normal priority throughout;
	// ordering only matters for the excluded case.) The ascend is unconditional so a host whose
	// catch-up failed still returns to normal priority in this pass: a conditional ascend would
	// leave it deprioritized until some later pass regenerates the common ConfigMap, and once the
	// CR reaches Completed the reconcile early-exit means that may be a long way off.
	err := w.catchReplicationLag(ctx, host)
	w.ascendHostInClickHouseCluster(ctx, host)
	if err == nil {
		w.a.V(1).
			M(host).F().
			Info("Replication lag is fine - include host into cluster. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		_ = w.includeHostIntoService(ctx, host)
	} else {
		w.a.V(1).
			M(host).F().
			Warning("Will NOT include host into cluster due to replication lag. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		if catchUpGateEnabled {
			return err
		}
	}

	return nil
}

// excludeHostFromService
func (w *worker) excludeHostFromService(ctx context.Context, host *api.Host) error {
	_ = w.c.ctrlLabeler.DeleteReadyMarkOnPodAndService(ctx, host)
	return nil
}

// includeHostIntoService
func (w *worker) includeHostIntoService(ctx context.Context, host *api.Host) error {
	_ = w.c.ctrlLabeler.SetReadyMarkOnPodAndService(ctx, host)
	return nil
}

// excludeHostFromClickHouseCluster excludes host from ClickHouse configuration
func (w *worker) excludeHostFromClickHouseCluster(ctx context.Context, host *api.Host) {
	w.a.V(1).
		M(host).F().
		Info("going to exclude host. Host/shard/cluster: %d/%d/%s",
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)

	// Specify in options to exclude this host from ClickHouse config file
	host.GetCR().GetRuntime().LockCommonConfig()
	host.GetReconcileAttributes().SetExclude()
	_ = w.reconcileConfigMapCommon(ctx, host.GetCR(), w.options())
	host.GetCR().GetRuntime().UnlockCommonConfig()

	if !w.shouldWaitExcludeHost(host) {
		return
	}
	// Wait for ClickHouse to pick-up the change
	_ = w.waitHostIsNotInCluster(ctx, host)
}

// includeHostIntoClickHouseCluster includes host into ClickHouse configuration
func (w *worker) includeHostIntoClickHouseCluster(ctx context.Context, host *api.Host) {
	w.a.V(1).
		M(host).F().
		Info("going to include host. Host/shard/cluster: %d/%d/%s",
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)

	// Specify in options to add this host into ClickHouse config file
	host.GetCR().GetRuntime().LockCommonConfig()
	host.GetReconcileAttributes().UnsetExclude()
	_ = w.reconcileConfigMapCommon(ctx, host.GetCR(), w.options())
	host.GetCR().GetRuntime().UnlockCommonConfig()

	if !w.shouldWaitIncludeHostIntoClickHouseCluster(host) {
		w.a.V(1).
			M(host).F().
			Info("No need to wait neither for host to be included in CH cluster nor to catch replication lag. "+
				"Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return
	}

	w.a.V(1).
		M(host).F().
		Info("Wait for host to be included into ClickHouse cluster. Wait for ClickHouse to pick-up the change. "+
			"Host/shard/cluster: %d/%d/%s",
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
	_ = w.waitHostIsInCluster(ctx, host)
}

// descendHostInClickHouseCluster
func (w *worker) descendHostInClickHouseCluster(ctx context.Context, host *api.Host) {
	w.a.V(1).
		M(host).F().
		Info("going to descent host. Host/shard/cluster: %d/%d/%s",
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)

	// Specify in options to exclude this host from ClickHouse config file
	host.GetCR().GetRuntime().LockCommonConfig()
	host.GetReconcileAttributes().SetLowPriority()
	_ = w.reconcileConfigMapCommon(ctx, host.GetCR(), w.options())
	host.GetCR().GetRuntime().UnlockCommonConfig()
	w.task.WaitForConfigMapPropagation(ctx, host)
}

// ascendHostInClickHouseCluster
func (w *worker) ascendHostInClickHouseCluster(ctx context.Context, host *api.Host) {
	w.a.V(1).
		M(host).F().
		Info("going to ascend host. Host/shard/cluster: %d/%d/%s",
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)

	// Specify in options to add this host into ClickHouse config file
	host.GetCR().GetRuntime().LockCommonConfig()
	host.GetReconcileAttributes().UnsetLowPriority()
	_ = w.reconcileConfigMapCommon(ctx, host.GetCR(), w.options())
	host.GetCR().GetRuntime().UnlockCommonConfig()
	w.task.WaitForConfigMapPropagation(ctx, host)
}

// catchReplicationLag
func (w *worker) catchReplicationLag(ctx context.Context, host *api.Host) error {
	if !w.shouldWaitReplicationHost(host) {
		w.a.V(1).
			M(host).F().
			Info("No need to wait to catch replication lag. "+
				"Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return nil
	}

	w.a.V(1).
		M(host).F().
		WithEvent(host.GetCR(), a.EventActionReconcile, a.EventReasonReconcileInProgress).
		Info("Wait for host to catch replication lag - START "+
			"Host/shard/cluster: %d/%d/%s",
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)

	// Host is alive but catching up - add to monitoring so metrics are collected during the wait
	w.addHostToMonitoring(host)

	var err error
	if chop.Config().Reconcile.Host.Wait.Replicas.CatchUp.IsEnabled() {
		var caughtUp bool
		caughtUp, err = w.runReplicaCatchUpGate(ctx, host)
		if err == nil {
			w.a.V(1).
				M(host).F().
				WithEvent(host.GetCR(), a.EventActionReconcile, replicaCatchUpGateEventReason(caughtUp)).
				Info("Wait for host to catch replication lag - %s "+
					"Host/shard/cluster: %d/%d/%s",
					replicaCatchUpGateResultLabel(caughtUp),
					host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName,
				)
		} else {
			w.a.V(1).
				M(host).F().
				WithEvent(host.GetCR(), a.EventActionReconcile, a.EventReasonReconcileFailed).
				Info("Wait for host to catch replication lag - FAILED "+
					"Host/shard/cluster: %d/%d/%s"+
					"err: %v ",
					host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName,
					err,
				)
		}
		return err
	}

	err = w.waitHostHasNoReplicationDelay(ctx, host)
	if err == nil {
		w.a.V(1).
			M(host).F().
			WithEvent(host.GetCR(), a.EventActionReconcile, a.EventReasonReconcileCompleted).
			Info("Wait for host to catch replication lag - COMPLETED "+
				"Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName,
			)

		host.GetCR().IEnsureStatus().PushHostReplicaCaughtUp(w.c.namer.Name(interfaces.NameFQDN, host))
	} else if errors.Is(err, errReplicationCatchUpNotFinished) {
		// Ran out of pass time, not a failure. Leave the host out of the Service - it is knowingly
		// behind - and schedule another pass to resume the wait, so this releases the reconcile
		// worker instead of holding it until the replica converges.
		w.scheduleReplicationCatchUpRetry(host)
	} else {
		w.a.V(1).
			M(host).F().
			WithEvent(host.GetCR(), a.EventActionReconcile, a.EventReasonReconcileFailed).
			Info("Wait for host to catch replication lag - FAILED "+
				"Host/shard/cluster: %d/%d/%s"+
				"err: %v ",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName,
				err,
			)
	}

	return err
}

func (w *worker) runReplicaCatchUpGate(ctx context.Context, host *api.Host) (bool, error) {
	catchUpConfig := chop.Config().Reconcile.Host.Wait.Replicas.CatchUp
	clusterSchemer := w.ensureClusterSchemer(host)
	hostFQDN := w.c.namer.Name(interfaces.NameFQDN, host)
	deadline := catchUpGateDeadline(catchUpConfig.GetTimeout())

	failSoft := func(reason string) (bool, error) {
		advance, _, err := onSoftTimeout(catchUpConfig.GetOnTimeout())
		if advance {
			w.a.M(host).F().Warning("sync gate %s; proceeding without caught-up marker (onTimeout=proceed)", reason)
		}
		return false, err
	}
	classifyErr := func(err error) (bool, error) {
		if err == nil {
			return false, nil
		}
		if contextError := ctx.Err(); contextError != nil {
			return false, contextError
		}
		if errors.Is(err, schemer.ErrGateDeadline) {
			return failSoft("timed out")
		}
		return false, err
	}

	if err := clusterSchemer.HostAsyncLoadBarrier(ctx, host, deadline); err != nil {
		return classifyErr(err)
	}
	replicatedObjects, err := clusterSchemer.PeerReplicatedObjectCount(ctx, host, deadline)
	if err != nil {
		return classifyErr(err)
	}
	if replicatedObjects == 0 {
		host.GetCR().IEnsureStatus().PushHostReplicaCaughtUp(hostFQDN)
		return true, nil
	}
	if err := clusterSchemer.HostSyncReplicatedObjects(ctx, host, deadline); err != nil {
		return classifyErr(err)
	}

	healthCounter := 0
	for {
		ok, hardFail, healthErr := w.catchUpHealthOK(ctx, host, deadline)
		if healthErr != nil {
			return classifyErr(healthErr)
		}

		remaining := time.Until(deadline)
		var done bool
		var hardDeadline bool
		healthCounter, done, hardDeadline = catchUpGateHealthStep(healthCounter, ok, hardFail, catchUpConfig.GetSuccessThreshold(), remaining)
		if hardDeadline {
			return false, catchUpGateHardFailError(host)
		}
		if done {
			host.GetCR().IEnsureStatus().PushHostReplicaCaughtUp(hostFQDN)
			return true, nil
		}

		if remaining <= 0 {
			return failSoft("health window not satisfied")
		}
		sleepDuration := time.Duration(catchUpConfig.GetPollInterval()) * time.Second
		if sleepDuration > remaining {
			sleepDuration = remaining
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(sleepDuration):
			if hardFail && !time.Now().Before(deadline) {
				return false, catchUpGateHardFailError(host)
			}
		}
	}
}

func catchUpGateHealthStep(counter int, ok bool, hardFail bool, threshold int, remaining time.Duration) (int, bool, bool) {
	if hardFail {
		return 0, false, remaining <= 0
	}
	nextCounter, done := healthWindowStep(counter, ok, threshold)
	return nextCounter, done, false
}

func catchUpGateHardFailError(host *api.Host) error {
	return fmt.Errorf("host %s readonly or session-expired; refusing to advance", host.GetName())
}

func replicaCatchUpGateEventReason(caughtUp bool) string {
	if caughtUp {
		return a.EventReasonReconcileCompleted
	}
	return a.EventReasonReconcileProceed
}

func replicaCatchUpGateResultLabel(caughtUp bool) string {
	if caughtUp {
		return "COMPLETED"
	}
	return "PROCEEDED without caught-up marker"
}

// catchUpGateDeadline turns the configured budget into an absolute deadline. The caller passes
// GetTimeout(), which substitutes the default for a nil or non-positive value, so the budget is
// always positive - the gate has no unbounded mode.
func catchUpGateDeadline(timeoutSeconds int) time.Time {
	return time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
}

// scheduleReplicationCatchUpRetry re-enqueues the CR so a later pass resumes a catch-up that did
// not finish within replicationCatchUpPassTimeout. Mirrors the stuck-host recovery scheduler:
// the queue coalesces by handle, so repeated scheduling cannot pile up work.
func (w *worker) scheduleReplicationCatchUpRetry(host *api.Host) {
	// NewReconcileCHI takes the concrete CHI; GetCR() is the shared interface, and CHK has no
	// catch-up wait, so a failed assertion simply means there is nothing to re-enqueue.
	cr, ok := host.GetCR().(*api.ClickHouseInstallation)
	if !ok || (cr == nil) {
		return
	}

	w.a.V(1).
		M(host).F().
		WithEvent(cr, a.EventActionReconcile, a.EventReasonReplicationCatchUpRescheduled).
		Warning("Host has not caught up within %s - left out of the service, re-enqueue in %s. Host/shard/cluster: %d/%d/%s",
			replicationCatchUpPassTimeout, replicationCatchUpRetryDelay,
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName,
		)

	scheduled := cr
	time.AfterFunc(replicationCatchUpRetryDelay, func() {
		w.c.enqueueObject(cmd_queue.NewReconcileCHI(cmd_queue.ReconcileAdd, nil, scheduled))
	})
}

// shouldExcludeHost determines whether host to be excluded from cluster before reconcile
func (w *worker) shouldExcludeHost(ctx context.Context, host *api.Host) bool {
	switch {
	case host.IsStopped():
		w.a.V(1).
			M(host).F().
			Info("Host is stopped, no need to exclude stopped host. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return false

	case host.IsTroubleshoot():
		w.a.V(1).
			M(host).F().
			Info("Host is in troubleshoot, no need to exclude stopped host. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return false

	case host.GetShard().HostsCount() == 1:
		w.a.V(1).
			M(host).F().
			Info("Host is the only host in the shard (means no replication), no need to exclude. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return false

	// Image upgrades defer SQL restart to the StatefulSet rollout (see shouldForceRestartHost /
	// isImageChangeRequested), but the host still goes down and must be drained first
	case w.isImageChangeRequested(host):
		if !w.isHostHealthyForReconcile(ctx, host) {
			w.a.V(1).M(host).F().Info(
				"Host image change but host is unhealthy - skip exclude. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
			return false
		}
		if !w.isShardSafeToDisruptHost(ctx, host) {
			w.a.V(1).M(host).F().Warning(
				"Host image change needs no exclude: shard has no other healthy replica, rollout will be deferred. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
			return false
		}
		w.a.V(1).
			M(host).F().
			Info("Host image change via STS rollout, need to exclude. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return true

	case w.shouldForceRestartHost(ctx, host):
		if !w.isHostHealthyForReconcile(ctx, host) {
			w.a.V(1).M(host).F().Info(
				"Host requires restart but is unhealthy - skip exclude. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
			return false
		}
		// Do not drain a host we are about to defer. reconcileHostStatefulSet skips the
		// restart when the shard has no other healthy replica, and it returns before
		// reconcileHostIncludeIntoAllActivities - so a host drained here would lose its
		// ready label, drop out of the CHI/cluster/shard Service endpoints, and have
		// nothing left in the pass to put it back. On a shard whose only other replica is
		// down that empties the entrypoint Service while the pod is up and serving.
		if !w.isShardSafeToDisruptHost(ctx, host) {
			w.a.V(1).M(host).F().Warning(
				"Host restart needs no exclude: shard has no other healthy replica, restart will be deferred. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
			return false
		}
		w.a.V(1).
			M(host).F().
			Info("Host should be restarted, need to exclude. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return true

	case host.GetReconcileAttributes().GetStatus().Is(types.ObjectStatusRequested):
		w.a.V(1).
			M(host).F().
			Info("Host is a new one, no need to exclude. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return false

	case host.GetReconcileAttributes().GetStatus().Is(types.ObjectStatusSame):
		w.a.V(1).
			M(host).F().
			Info("Host is the same, would not be updated, no need to exclude. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return false
	}

	shouldExcludeHost := false
	w.a.V(1).
		M(host).F().
		Info("No explicit case on whether host should be excluded - going default %t. Host/shard/cluster: %d/%d/%s",
			shouldExcludeHost,
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)

	return shouldExcludeHost
}

// shouldWaitExcludeHost determines whether reconciler should wait for the host to be excluded from cluster
func (w *worker) shouldWaitExcludeHost(host *api.Host) bool {
	// Check CHI settings
	switch {
	case host.GetCR().GetReconcile().IsReconcilingPolicyWait():
		w.a.V(1).
			M(host).F().
			Info("IsReconcilingPolicyWait() need to wait to exclude host. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return true
	case host.GetCR().GetReconcile().IsReconcilingPolicyNoWait():
		w.a.V(1).
			M(host).F().
			Info("IsReconcilingPolicyNoWait() need NOT to wait to exclude host. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return false
	}

	w.a.V(1).
		M(host).F().
		Info("wait to exclude host fallback to operator's settings. Host/shard/cluster: %d/%d/%s",
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
	return chop.Config().Reconcile.Host.Wait.Exclude.Value()
}

// shouldWaitQueries determines whether reconciler should wait for the host to complete running queries
func (w *worker) shouldWaitQueries(host *api.Host) bool {
	switch {
	case host.GetReconcileAttributes().GetStatus().Is(types.ObjectStatusRequested):
		w.a.V(1).
			M(host).F().
			Info("No need to wait for queries to complete on a host, host is a new one. "+
				"Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return false
	case chop.Config().Reconcile.Host.Wait.Queries.Value():
		w.a.V(1).
			M(host).F().
			Info("Will wait for queries to complete on a host according to CHOp config '.reconcile.host.wait.queries' setting. "+
				"Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return true
	case host.GetCR().GetReconcile().IsReconcilingPolicyWait():
		w.a.V(1).
			M(host).F().
			Info("Will wait for queries to complete on a host according to CHI 'reconciling.policy' setting. "+
				"Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return true
	}

	w.a.V(1).
		M(host).F().
		Info("Will NOT wait for queries to complete on a host. "+
			"Host/shard/cluster: %d/%d/%s",
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
	return false
}

// shouldWaitIncludeHostIntoClickHouseCluster determines whether reconciler should wait for the host to be included into cluster
func (w *worker) shouldWaitIncludeHostIntoClickHouseCluster(host *api.Host) bool {
	status := host.GetReconcileAttributes().GetStatus()
	switch {
	case status.Is(types.ObjectStatusRequested):
		return false
	case status.Is(types.ObjectStatusCreated):
		return false
	case status.Is(types.ObjectStatusSame):
		// The same host was not modified and no need to wait it to be included - it already is
		return false
	case host.GetShard().HostsCount() == 1:
		// No need to wait one-host-shard
		return false
	case host.GetCR().GetReconcile().IsReconcilingPolicyWait():
		// Check CHI settings - explicitly requested to wait
		return true
	case host.GetCR().GetReconcile().IsReconcilingPolicyNoWait():
		// Check CHI settings - explicitly requested to not wait
		return false
	}

	// Fallback to operator's settings
	return chop.Config().Reconcile.Host.Wait.Include.Value()
}

// waitHostIsInCluster
func (w *worker) waitHostIsInCluster(ctx context.Context, host *api.Host) error {
	return domain.PollHost(ctx, host, w.ensureClusterSchemer(host).IsHostInCluster)
}

// waitHostIsNotInCluster
func (w *worker) waitHostIsNotInCluster(ctx context.Context, host *api.Host) error {
	return domain.PollHost(ctx, host, func(ctx context.Context, host *api.Host) bool {
		return !w.ensureClusterSchemer(host).IsHostInCluster(ctx, host)
	})
}

// waitHostHasNoActiveQueries
func (w *worker) waitHostHasNoActiveQueries(ctx context.Context, host *api.Host) error {
	return domain.PollHost(ctx, host, w.doesHostHaveNoRunningQueries)
}

// waitHostHasNoReplicationDelay waits until the host reports a replication lag within the
// configured limit, for at most replicationCatchUpPassTimeout.
//
// The bound is per reconcile pass, not a budget for the whole catch-up: the replica fetches from
// its peers regardless of whether the operator is watching, so giving up here loses no progress.
// On expiry the caller leaves the host out of the Service - it is knowingly behind - and
// re-enqueues the CR, so a slow replica converges over several passes while a replica that can
// never converge stays visible instead of holding a reconcile worker for good.
func (w *worker) waitHostHasNoReplicationDelay(ctx context.Context, host *api.Host) error {
	err := domain.PollHost(ctx, host, w.doesHostHaveNoReplicationDelay, &poller.Options{Timeout: replicationCatchUpPassTimeout})
	if err != nil {
		return err
	}
	// The poller reports a cancelled context as success, and QueryHostInt answers a cancelled
	// context with a delay of 0, so without this check an interrupted reconcile would look like
	// a host that caught up - and the caller would persist that verdict.
	if util.IsContextDone(ctx) {
		return common.ErrCRUDAbort
	}
	// Poll() also returns nil when it simply ran out of time, so re-check the predicate: without
	// this an expired wait is indistinguishable from a host that caught up.
	if !w.doesHostHaveNoReplicationDelay(ctx, host) {
		return errReplicationCatchUpNotFinished
	}
	return nil
}

// waitHostRestart
func (w *worker) waitHostRestart(ctx context.Context, host *api.Host, restartCounters map[string]int) error {
	return domain.PollHost(ctx, host, func(ctx context.Context, host *api.Host) bool {
		return w.isPodRestarted(ctx, host, restartCounters)
	})
}

// waitHostIsReady
func (w *worker) waitHostIsReady(ctx context.Context, host *api.Host) error {
	return domain.PollHost(ctx, host, w.isPodReady)
}

// waitHostIsStarted
func (w *worker) waitHostIsStarted(ctx context.Context, host *api.Host) error {
	return domain.PollHost(ctx, host, w.isPodStarted)
}

// waitHostIsRunning
func (w *worker) waitHostIsRunning(ctx context.Context, host *api.Host) error {
	return domain.PollHost(ctx, host, w.isPodRunning)
}
