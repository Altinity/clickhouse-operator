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
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"time"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/poller/domain"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func (w *worker) waitForIPAddresses(ctx context.Context, chi *api.ClickHouseInstallation) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}
	if chi.IsStopped() {
		// No need to wait for stopped CHI
		return
	}
	w.a.V(1).M(chi).F().S().Info("wait for IP addresses to be assigned to all pods")
	start := time.Now()
	w.c.poll(ctx, chi, func(c *api.ClickHouseInstallation, e error) bool {
		// TODO fix later
		// status IPs list can be empty
		// Instead of doing in status:
		// 	podIPs := c.getPodsIPs(chi)
		//	cur.EnsureStatus().SetPodIPs(podIPs)
		// and here
		// c.Status.GetPodIPs()
		podIPs := w.c.getPodsIPs(chi)
		if len(podIPs) >= len(c.Status.GetPods()) {
			// Stop polling
			w.a.V(1).M(c).Info("all IP addresses are in place")
			return false
		}
		if time.Now().Sub(start) > 1*time.Minute {
			// Stop polling
			w.a.V(1).M(c).Warning("not all IP addresses are in place but time has elapsed")
			return false
		}
		// Continue polling
		w.a.V(1).M(c).Warning("still waiting - not all IP addresses are in place yet")
		return true
	})
}

// excludeHost excludes host from ClickHouse clusters if required
func (w *worker) excludeHost(ctx context.Context, host *api.Host) bool {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return false
	}

	log.V(1).M(host).F().S().Info("exclude host start")
	defer log.V(1).M(host).F().E().Info("exclude host end")

	if !w.shouldExcludeHost(host) {
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

// shouldIncludeHost determines whether host to be included into cluster after reconciling
func (w *worker) shouldIncludeHost(host *api.Host) bool {
	switch {
	case host.IsStopped():
		// No need to include stopped host
		return false
	}
	return true
}

func (w *worker) shouldWaitReplicationHost(host *api.Host) bool {
	switch {
	case chop.Config().Reconcile.Host.Wait.Replicas.All.Value():
		// All replicas are explicitly requested to wait for replication to catch-up
		return true

	case chop.Config().Reconcile.Host.Wait.Replicas.New.Value():
		// New replicas are explicitly requested to wait for replication to catch-up.
		if host.GetReconcileAttributes().GetStatus().Is(types.ObjectStatusCreated) {
			// This is a new replica - certainly need to catch-up
			return true
		}

		// This is not a new replica, it may have incomplete replication catch-up job still

		if host.HasListedReplicaCaughtUp(w.c.namer.Name(interfaces.NameFQDN, host)) {
			// Replica is already listed as caught, no need to catch-up again
			return false
		}

		// Replica has never reached caught-up status, need to wait ffor replication
		return true
	}

	return false
}

// includeHost includes host back back into ClickHouse clusters
func (w *worker) includeHost(ctx context.Context, host *api.Host) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	if !w.shouldIncludeHost(host) {
		w.a.V(1).
			M(host).F().
			Info("No need to include host into cluster. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return nil
	}

	w.a.V(1).
		M(host).F().
		Info("Include host into cluster. Host/shard/cluster: %d/%d/%s",
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)

	// w.includeHostIntoClickHouseCluster(ctx, host)
	w.ascendHostInClickHouseCluster(ctx, host)
	w.catchReplicationLag(ctx, host)
	_ = w.includeHostIntoService(ctx, host)

	return nil
}

// excludeHostFromService
func (w *worker) excludeHostFromService(ctx context.Context, host *api.Host) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	_ = w.c.ctrlLabeler.DeleteReadyMarkOnPodAndService(ctx, host)
	return nil
}

// includeHostIntoService
func (w *worker) includeHostIntoService(ctx context.Context, host *api.Host) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	_ = w.c.ctrlLabeler.SetReadyMarkOnPodAndService(ctx, host)
	return nil
}

// excludeHostFromClickHouseCluster excludes host from ClickHouse configuration
func (w *worker) excludeHostFromClickHouseCluster(ctx context.Context, host *api.Host) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

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
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

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

func (w *worker) descendHostInClickHouseCluster(ctx context.Context, host *api.Host) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

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

func (w *worker) ascendHostInClickHouseCluster(ctx context.Context, host *api.Host) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

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

func (w *worker) catchReplicationLag(ctx context.Context, host *api.Host) {
	if !w.shouldWaitReplicationHost(host) {
		w.a.V(1).
			M(host).F().
			Info("No need to wait to catch replication lag. "+
				"Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return
	}

	w.a.V(1).
		M(host).F().
		Info("Wait for host to catch replication lag - START "+
			"Host/shard/cluster: %d/%d/%s",
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)

	_ = w.waitHostHasNoReplicationDelay(ctx, host)

	w.a.V(1).
		M(host).F().
		Info("Wait for host to catch replication lag - COMPLETED "+
			"Host/shard/cluster: %d/%d/%s",
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)

	host.GetCR().IEnsureStatus().PushHostReplicaCaughtUp(w.c.namer.Name(interfaces.NameFQDN, host))
}

// shouldExcludeHost determines whether host to be excluded from cluster before reconciling
func (w *worker) shouldExcludeHost(host *api.Host) bool {
	switch {
	case host.IsStopped():
		w.a.V(1).
			M(host).F().
			Info("Host is stopped, no need to exclude stopped host. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return false
	case host.GetShard().HostsCount() == 1:
		w.a.V(1).
			M(host).F().
			Info("Host is the only host in the shard (means no replication), no need to exclude. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return false
	case w.shouldForceRestartHost(host):
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

	w.a.V(1).
		M(host).F().
		Info("Host should be excluded. Host/shard/cluster: %d/%d/%s",
			host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)

	return true
}

// shouldWaitExcludeHost determines whether reconciler should wait for the host to be excluded from cluster
func (w *worker) shouldWaitExcludeHost(host *api.Host) bool {
	// Check CHI settings
	switch {
	case host.GetCR().GetReconciling().IsReconcilingPolicyWait():
		w.a.V(1).
			M(host).F().
			Info("IsReconcilingPolicyWait() need to wait to exclude host. Host/shard/cluster: %d/%d/%s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return true
	case host.GetCR().GetReconciling().IsReconcilingPolicyNoWait():
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
	case host.GetCR().GetReconciling().IsReconcilingPolicyWait():
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
	case host.GetCR().GetReconciling().IsReconcilingPolicyWait():
		// Check CHI settings - explicitly requested to wait
		return true
	case host.GetCR().GetReconciling().IsReconcilingPolicyNoWait():
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

// waitHostHasNoReplicationDelay
func (w *worker) waitHostHasNoReplicationDelay(ctx context.Context, host *api.Host) error {
	return domain.PollHost(ctx, host, w.doesHostHaveNoReplicationDelay)
}

// waitHostRestart
func (w *worker) waitHostRestart(ctx context.Context, host *api.Host, start map[string]int) error {
	return domain.PollHost(ctx, host, func(ctx context.Context, host *api.Host) bool {
		return w.isPodRestarted(ctx, host, start)
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
