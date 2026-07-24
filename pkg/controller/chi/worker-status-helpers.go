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
	"time"

	core "k8s.io/api/core/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
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

// isShardSafeToDisruptHost is true when host may be excluded/restarted/rolled
// without taking down the last healthy replica in its shard.
func (w *worker) isShardSafeToDisruptHost(ctx context.Context, host *api.Host) bool {
	if host == nil || host.GetShard() == nil || host.GetShard().HostsCount() <= 1 {
		return true
	}
	safe := false
	host.GetShard().WalkHosts(func(peer *api.Host) error {
		if peer != nil && peer.GetName() != host.GetName() && w.isHostHealthyForReconcile(ctx, peer) {
			safe = true
		}
		return nil
	})
	return safe
}

// hostMayRequireDisruption is true when reconcile is expected to restart or roll the host.
func (w *worker) hostMayRequireDisruption(ctx context.Context, host *api.Host) bool {
	if host == nil || host.IsStopped() || host.IsTroubleshoot() {
		return false
	}
	if w.shouldForceRestartHost(ctx, host) {
		return true
	}
	status := host.GetReconcileAttributes().GetStatus()
	return !status.Is(types.ObjectStatusRequested) && !status.Is(types.ObjectStatusSame)
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
