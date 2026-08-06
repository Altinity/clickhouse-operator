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
	"time"

	core "k8s.io/api/core/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	commonConfig "github.com/altinity/clickhouse-operator/pkg/model/common/config"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// timeToStart specifies time that operator does not accept changes
const timeToStart = 1 * time.Minute

// isJustStarted checks whether worked just started
func (w *worker) isJustStarted() bool {
	return time.Since(w.start) < timeToStart
}

func (w *worker) isPodCrushed(ctx context.Context, host *api.Host) bool {
	if pod, err := w.c.kube.Pod().Get(ctx, host); err == nil {
		return k8s.PodHasCrushedContainers(pod)
	}
	return true
}

func (w *worker) isPodReady(ctx context.Context, host *api.Host) bool {
	if pod, err := w.c.kube.Pod().Get(ctx, host); err == nil {
		return !k8s.PodHasNotReadyContainers(pod)
	}
	return false
}

// isPodSustainedNotReady reports whether the host's pod is currently Ready=False AND
// has been so for at least `threshold`. Returns false for pods whose failure mode is
// already being handled by kubelet (ImagePullBackOff, CrashLoopBackOff, Pending, etc.)
// so the operator does not race kubelet on its own recovery path.
func (w *worker) isPodSustainedNotReady(ctx context.Context, host *api.Host, threshold time.Duration) bool {
	if threshold <= 0 {
		// Threshold of 0/negative means "feature disabled"
		return false
	}
	pod, err := w.c.kube.Pod().Get(ctx, host)
	if err != nil || pod == nil {
		return false
	}
	if podIsInKubeletFailureMode(pod) {
		return false
	}
	return podIsSustainedNotReady(pod, threshold, time.Now())
}

// podIsSustainedNotReady is the pure inner predicate of isPodSustainedNotReady,
// extracted so it can be exercised without a kube client. Returns true iff the pod
// has a PodReady condition that is currently not True and whose LastTransitionTime
// is at least `threshold` in the past relative to `now`.
func podIsSustainedNotReady(pod *core.Pod, threshold time.Duration, now time.Time) bool {
	if pod == nil || threshold <= 0 {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type != core.PodReady {
			continue
		}
		if cond.Status == core.ConditionTrue {
			return false
		}
		// Status is False or Unknown. Treat both as "not ready"
		if cond.LastTransitionTime.IsZero() {
			return false
		}
		return now.Sub(cond.LastTransitionTime.Time) >= threshold
	}
	// No PodReady condition at all.
	return false
}

// kubeletDrivenWaitingReasons is the set of container Waiting.Reason values that
// indicate kubelet is already actively recovering the pod and a parallel
// operator-driven StatefulSet rollout would just race kubelet.
var kubeletDrivenWaitingReasons = map[string]struct{}{
	"CrashLoopBackOff":           {},
	"ImagePullBackOff":           {},
	"ErrImagePull":               {},
	"InvalidImageName":           {},
	"CreateContainerError":       {},
	"RunContainerError":          {},
	"ContainerCannotRun":         {},
	"CreateContainerConfigError": {},
}

// podIsInKubeletFailureMode reports whether the pod is in a state where kubelet
// (or the kube-scheduler) is already handling the failure: not yet scheduled,
// in Pending phase, or any container in a kubelet-driven waiting reason.
// In those states an operator-driven reconcile would race kubelet without value.
func podIsInKubeletFailureMode(pod *core.Pod) bool {
	if pod == nil {
		return false
	}
	if pod.Status.Phase == core.PodPending {
		return true
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting == nil {
			continue
		}
		if _, hit := kubeletDrivenWaitingReasons[cs.State.Waiting.Reason]; hit {
			return true
		}
	}
	for _, cs := range pod.Status.InitContainerStatuses {
		if cs.State.Waiting == nil {
			continue
		}
		if _, hit := kubeletDrivenWaitingReasons[cs.State.Waiting.Reason]; hit {
			return true
		}
	}
	return false
}

func (w *worker) isPodStarted(ctx context.Context, host *api.Host) bool {
	if pod, err := w.c.kube.Pod().Get(ctx, host); err == nil {
		return k8s.PodHasAllContainersStarted(pod)
	}
	return false
}

func (w *worker) isPodRunning(ctx context.Context, host *api.Host) bool {
	if pod, err := w.c.kube.Pod().Get(ctx, host); err == nil {
		return k8s.PodPhaseIsRunning(pod)
	}
	return false
}

func (w *worker) isPodOK(ctx context.Context, host *api.Host) bool {
	if pod, err := w.c.kube.Pod().Get(ctx, host); err == nil {
		return k8s.IsPodOK(pod)
	}
	return false
}

// isHostHealthyForReconcile is true when host is safe to count as a live peer for
// shard-safety checks. Stopped/troubleshoot hosts are intentionally unavailable
// and do not block disruption of a peer.
func (w *worker) isHostHealthyForReconcile(ctx context.Context, host *api.Host) bool {
	if host == nil {
		return false
	}
	if host.IsStopped() || host.IsTroubleshoot() {
		return true
	}
	return w.c != nil && w.c.kube != nil &&
		w.isPodRunning(ctx, host) &&
		w.isPodReady(ctx, host) &&
		!w.isPodCrushed(ctx, host)
}

func (w *worker) hasUnhealthyHosts(ctx context.Context, cr *api.ClickHouseInstallation) bool {
	if cr == nil {
		return false
	}
	found := false
	cr.WalkHosts(func(host *api.Host) error {
		if !w.isHostHealthyForReconcile(ctx, host) {
			found = true
		}
		return nil
	})
	return found
}

func (w *worker) syncHealthOK(ctx context.Context, host *api.Host, deadline time.Time) (ok bool, hardFail bool, err error) {
	clusterSchemer := w.ensureClusterSchemer(host)
	readHealth := func(read func(context.Context, *api.Host) (int, error)) (int, bool, error) {
		if contextError := ctx.Err(); contextError != nil {
			return 0, false, contextError
		}
		queryCtx, cancel := context.WithDeadline(ctx, deadline)
		defer cancel()
		healthValue, queryErr := read(queryCtx, host)
		if contextError := ctx.Err(); contextError != nil {
			return 0, false, contextError
		}
		if queryCtx.Err() != nil || errors.Is(queryErr, context.DeadlineExceeded) {
			return 0, true, nil
		}
		if queryErr != nil {
			return 0, false, queryErr
		}
		return healthValue, false, nil
	}

	readonly, notReady, err := readHealth(clusterSchemer.HostMaxIsReadonly)
	if err != nil || notReady {
		return false, false, err
	}
	sessionExpired, notReady, err := readHealth(clusterSchemer.HostMaxIsSessionExpired)
	if err != nil || notReady {
		return false, false, err
	}
	replicaDelay, notReady, err := readHealth(clusterSchemer.HostMaxReplicaDelay)
	if err != nil || notReady {
		return false, false, err
	}
	if readonly != 0 || sessionExpired != 0 {
		return false, true, nil
	}
	return replicaDelay <= chop.Config().Reconcile.Host.Wait.Replicas.Delay.IntValue(), false, nil
}

// isOperatorIPTheSame reports whether the operator pod IP still matches the
// CHOpIP persisted on the CR from the previous reconcile. A changed IP must
// force reconcile so clickhouse-operator user networks/host_regexp are refreshed.
// Compare against prevCHOpIP captured before buildCR — normalize/fillStatus
// overwrites status.chop-ip with the current operator IP.
func (w *worker) isOperatorIPTheSame(prevCHOpIP string) bool {
	ip, _ := chop.GetRuntimeParam(deployment.OPERATOR_POD_IP)
	return ip == prevCHOpIP
}

// isShardSafeToDisruptHost is true when host may be excluded/restarted/rolled
// without taking down the last healthy replica in its shard.
func (w *worker) isShardSafeToDisruptHost(ctx context.Context, host *api.Host) bool {
	if host == nil {
		return true
	}
	return shardHasHealthyPeer(host.GetShard(), host, func(peer *api.Host) bool {
		return w.isHostHealthyForReconcile(ctx, peer)
	})
}

// shardHasHealthyPeer reports whether the shard holds a host other than the given one that
// healthy() accepts. The shard is passed in rather than resolved from the host so the rule is
// pure apart from the injected probe, and therefore exercisable without a kube client or a
// fully wired CR.
func shardHasHealthyPeer(shard api.IShard, host *api.Host, healthy func(*api.Host) bool) bool {
	if (shard == nil) || (host == nil) || (shard.HostsCount() <= 1) {
		return true
	}
	found := false
	shard.WalkHosts(func(peer *api.Host) error {
		// Compare pointers: WalkHosts yields the live hosts of the shard, and host names are
		// user-overridable, so a name compare could mistake a same-named replica for self.
		if (peer != nil) && (peer != host) && healthy(peer) {
			found = true
		}
		return nil
	})
	return found
}

// hostDisruptionWouldDegradeShard is true when this pass would restart or roll the host while
// its shard has no other healthy replica to serve meanwhile (#1704).
//
// Must be called after PrepareHostStatefulSetWithStatus: ObjectStatusSame is assigned only
// there, and without it every pre-existing host looks disruptive - which would withhold even
// a non-disruptive reconcile from a converged host.
func (w *worker) hostDisruptionWouldDegradeShard(ctx context.Context, host *api.Host) bool {
	if (host == nil) || host.IsStopped() || host.IsTroubleshoot() {
		return false
	}
	if host.GetReconcileAttributes().GetStatus().Is(types.ObjectStatusRequested) {
		// Brand new host - there is no pod to take down.
		return false
	}
	// Not Same means the StatefulSet is about to change, so the pod rolls. A force restart
	// takes the pod down even when the StatefulSet itself is unchanged.
	willDisrupt := !host.GetReconcileAttributes().GetStatus().Is(types.ObjectStatusSame) ||
		w.shouldForceRestartHost(ctx, host)
	if !willDisrupt {
		return false
	}
	// Only a host that is still serving needs protecting - an already-down host must be free
	// to recover, which is what makes recovery-first ordering work.
	return w.isHostHealthyForReconcile(ctx, host) && !w.isShardSafeToDisruptHost(ctx, host)
}

func (w *worker) isPodRestarted(ctx context.Context, host *api.Host, initialRestartCounters map[string]int) bool {
	curRestartCounters, _ := w.c.kube.Pod().(interfaces.IKubePodEx).GetRestartCounters(ctx, host)
	return !util.MapsAreTheSame(initialRestartCounters, curRestartCounters)
}

func (w *worker) doesHostHaveNoRunningQueries(ctx context.Context, host *api.Host) bool {
	n, _ := w.ensureClusterSchemer(host).HostActiveQueriesNum(ctx, host)
	log.V(1).Info("active queries %d host: %s", n, host.GetName())
	return n <= 1
}

func (w *worker) doesHostHaveNoReplicationDelay(ctx context.Context, host *api.Host) bool {
	delay, _ := w.ensureClusterSchemer(host).HostMaxReplicaDelay(ctx, host)
	log.V(1).Info("replication lag %d host: %s", delay, host.GetName())
	return delay <= chop.Config().Reconcile.Host.Wait.Replicas.Delay.IntValue()
}

// areUsableOldAndNew checks whether there are old and new usable
func (w *worker) areUsableOldAndNew(old, new *api.ClickHouseInstallation) bool {
	if old == nil {
		return false
	}
	if new == nil {
		return false
	}
	return true
}

// isAfterFinalizerInstalled checks whether we are just installed finalizer
func (w *worker) isAfterFinalizerInstalled(old, new *api.ClickHouseInstallation) bool {
	if !w.areUsableOldAndNew(old, new) {
		return false
	}

	finalizerIsInstalled := len(old.Finalizers) == 0 && len(new.Finalizers) > 0
	return w.isGenerationTheSame(old, new) && finalizerIsInstalled
}

// isGenerationTheSame checks whether old and new CHI have the same generation
func (w *worker) isGenerationTheSame(old, new *api.ClickHouseInstallation) bool {
	if !w.areUsableOldAndNew(old, new) {
		return false
	}

	return old.GetGeneration() == new.GetGeneration()
}

// getRemoteServersGeneratorOptions build base set of RemoteServersOptions
func (w *worker) getRemoteServersGeneratorOptions() *commonConfig.HostSelector {
	// Base model specifies to exclude:
	// 1. all newly added hosts
	// 2. all explicitly excluded hosts
	//
	// Excluding newly-added (ObjectStatusRequested) hosts is DELIBERATE, not incidental: a host
	// whose StatefulSet does not exist yet is an unreachable cluster member, and remote_servers
	// lives in the single COMMON ConfigMap mounted by every pod. Advertising a not-yet-created
	// host would hand every existing pod a cluster definition pointing at a host that cannot be
	// reached, breaking cluster-wide operations during the reconcile window (existing replicas'
	// Distributed queries, ON CLUSTER DDL, and the operator's own clusterAllReplicas/remote()
	// schema-migration and health queries). The new host is added to remote_servers only in the
	// final phase, once its StatefulSet exists. Do NOT drop this to "seed" the full topology into
	// the preliminary ConfigMap: because the ConfigMap is shared, that necessarily re-advertises
	// the not-yet-created host to existing pods. Newly-added hosts recover their cluster-dependent
	// objects via the post-publish restart in restartNewlyAddedHosts, never by seeding.
	return commonConfig.NewHostSelector().ExcludeReconcileAttributes(
		types.NewReconcileAttributes().
			SetStatus(types.ObjectStatusRequested).
			SetExclude(),
	)
}

// options build FilesGeneratorOptionsClickHouse
func (w *worker) options() *config.FilesGeneratorOptions {
	opts := w.getRemoteServersGeneratorOptions()
	w.a.Info("RemoteServersOptions: %s", opts)
	return config.NewFilesGeneratorOptions().SetRemoteServersOptions(opts)
}
