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

package chk

import (
	"context"
	"fmt"
	"time"

	apps "k8s.io/api/apps/v1"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Raft / ensemble safety policy for CHK (#2069).
//
// Owns rolling-vs-bootstrap classification, STS wait probes, quorum disrupt gate,
// recovery-first host ordering, and membership settle delays. The reconciler
// calls into these helpers; it should not restate the policy inline.
//
// Before disrupting a host STS:
//
//  1. snapshotHostEnsemble — freeze rolling vs bootstrap (live Ready, not ancestor)
//  2. prepareStsReconcileOptsWaitSection — Ready wait iff rolling
//  3. ensureQuorumSafeToDisruptHost — wait/defer if disrupt would break Raft majority
//
// hostDisruptionWouldBreakQuorum is the tested predicate used inside the façade.
// verifyHostEnsembleMembership is the extension point for a fuller Raft barrier
// (committed /keeper/config + mntr, as in PR #2041).

const (
	defaultQuorumDisruptPollInterval = 5 * time.Second
	defaultQuorumDisruptWaitTimeout  = 2 * time.Minute
)

// hostEnsembleSnapshot captures ensemble state before any host disruption.
// rolling must not be re-derived after force-restart — ReadyReplicas drops to 0.
type hostEnsembleSnapshot struct {
	rolling    bool
	members    int
	readyCount int
}

// chkStatefulSetFallback aborts the reconcile on STS create/update wait failure.
// DefaultFallback returns ErrCRUDIgnore, which lets the host loop recreate the
// next replica while the previous one never rejoined — the #2069 failure mode.
type chkStatefulSetFallback struct{}

func newChkStatefulSetFallback() *chkStatefulSetFallback {
	return &chkStatefulSetFallback{}
}

func (f *chkStatefulSetFallback) OnStatefulSetCreateFailed(ctx context.Context, host *api.Host) common.ErrorCRUD {
	return common.ErrCRUDAbort
}

func (f *chkStatefulSetFallback) OnStatefulSetUpdateFailed(
	ctx context.Context,
	oldStatefulSet *apps.StatefulSet,
	host *api.Host,
	sts interfaces.IKubeSTS,
) common.ErrorCRUD {
	return common.ErrCRUDAbort
}

// raftQuorumSize is Raft majority for an ensemble of n members (n/2 + 1).
func raftQuorumSize(members int) int {
	if members <= 0 {
		return 0
	}
	return members/2 + 1
}

// countReadyEnsembleMembers counts Keeper hosts whose StatefulSet reports
// ReadyReplicas > 0 (each host is typically a 1-replica STS).
//
// countReadyEnsembleMembersFn, when set on the worker, overrides live lookup
// (tests inject fixed Ready counts).
func (w *worker) countReadyEnsembleMembers(ctx context.Context, cr api.ICustomResource) int {
	if w.countReadyEnsembleMembersFn != nil {
		return w.countReadyEnsembleMembersFn(ctx, cr)
	}
	if cr == nil {
		return 0
	}
	ready := 0
	_ = cr.WalkHosts(func(host *api.Host) error {
		sts := host.Runtime.CurStatefulSet
		if sts == nil && w.c != nil {
			sts, _ = w.c.kube.STS().Get(ctx, host)
		}
		if sts != nil && sts.Status.ReadyReplicas > 0 {
			ready++
		}
		return nil
	})
	return ready
}

// snapshotHostEnsemble records rolling vs bootstrap before disrupting a host.
func (w *worker) snapshotHostEnsemble(ctx context.Context, host *api.Host) hostEnsembleSnapshot {
	if host == nil || host.GetCR() == nil {
		return hostEnsembleSnapshot{}
	}
	cr := host.GetCR()
	n := cr.HostsCount()
	ready := w.countReadyEnsembleMembers(ctx, cr)
	return hostEnsembleSnapshot{
		rolling:    n <= 1 || ready >= raftQuorumSize(n),
		members:    n,
		readyCount: ready,
	}
}

// refreshQuorumSnapshotCounts updates live Ready counts for an in-flight wait.
// rolling is intentionally frozen — it was captured before any disruption.
func (w *worker) refreshQuorumSnapshotCounts(ctx context.Context, host *api.Host, snap *hostEnsembleSnapshot) {
	if host == nil || snap == nil || !snap.rolling {
		return
	}
	if w.c != nil {
		host.Runtime.CurStatefulSet, _ = w.c.kube.STS().Get(ctx, host)
	}
	if cr := host.GetCR(); cr != nil {
		snap.readyCount = w.countReadyEnsembleMembers(ctx, cr)
	}
}

func (w *worker) quorumDisruptPollInterval() time.Duration {
	if w.quorumDisruptPollOverride > 0 {
		return w.quorumDisruptPollOverride
	}
	return defaultQuorumDisruptPollInterval
}

func (w *worker) quorumDisruptWaitTimeout() time.Duration {
	if w.quorumDisruptWaitOverride > 0 {
		return w.quorumDisruptWaitOverride
	}
	return defaultQuorumDisruptWaitTimeout
}

// ensureQuorumSafeToDisruptHost is the reconciler façade for the Raft disrupt gate.
// Call after PrepareHostStatefulSetWithStatus so ObjectStatusSame is assigned.
// Returns nil when safe (or not a rolling multi-member disrupt); ErrCRUDDeferred
// after waiting up to the budget without quorum headroom.
func (w *worker) ensureQuorumSafeToDisruptHost(
	ctx context.Context,
	host *api.Host,
	opts *statefulset.ReconcileOptions,
	snap *hostEnsembleSnapshot,
) error {
	if snap == nil || !snap.rolling || snap.members <= 1 {
		return nil
	}
	if !w.hostDisruptionWouldBreakQuorum(ctx, host, opts, *snap) {
		return nil
	}

	w.a.V(1).M(host).F().Info(
		"Waiting for Raft quorum headroom before disrupting host %s (ready=%d quorum=%d)",
		host.GetName(), snap.readyCount, raftQuorumSize(snap.members),
	)

	deadline := time.Now().Add(w.quorumDisruptWaitTimeout())
	for time.Now().Before(deadline) {
		if util.WaitContextDoneOrTimeout(ctx, w.quorumDisruptPollInterval()) {
			return ctx.Err()
		}
		w.refreshQuorumSnapshotCounts(ctx, host, snap)
		if !w.hostDisruptionWouldBreakQuorum(ctx, host, opts, *snap) {
			w.a.V(1).M(host).F().Info(
				"Raft quorum headroom available — proceeding with host %s disruption (ready=%d)",
				host.GetName(), snap.readyCount,
			)
			return nil
		}
	}

	w.a.V(1).M(host).F().
		WithEvent(host.GetCR(), a.EventActionReconcile, a.EventReasonHostReconcileDeferredShardSafety).
		Warning(
			"Deferring host StatefulSet reconcile: disrupting %s would drop below Raft quorum (%s)",
			host.GetName(), quorumDisruptDeferMessage(host, *snap),
		)
	return common.ErrCRUDDeferred
}

// isHostHealthyForReconcile is true when the host counts as live for recovery-first
// ordering and quorum headroom. Stopped/troubleshoot hosts are intentionally
// unavailable and are ordered after recovery hosts (CHI #1704).
func (w *worker) isHostHealthyForReconcile(ctx context.Context, host *api.Host) bool {
	if host == nil {
		return false
	}
	if host.IsStopped() || host.IsTroubleshoot() {
		return true
	}
	sts := host.Runtime.CurStatefulSet
	if sts == nil && w.c != nil {
		sts, _ = w.c.kube.STS().Get(ctx, host)
	}
	return sts != nil && sts.Status.ReadyReplicas > 0
}

// hostContributesReady reports whether this host currently counts toward live quorum.
func hostContributesReady(host *api.Host) bool {
	if host == nil || host.Runtime.CurStatefulSet == nil {
		return false
	}
	return host.Runtime.CurStatefulSet.Status.ReadyReplicas > 0
}

// ensembleQuorumSafeAfterDisrupt reports whether remaining Ready members would still
// meet quorum if this host were disrupted. Pure — snap counts are frozen before disrupt.
func ensembleQuorumSafeAfterDisrupt(snap hostEnsembleSnapshot, host *api.Host) bool {
	if !snap.rolling || snap.members <= 1 {
		return true
	}
	remaining := snap.readyCount
	if hostContributesReady(host) {
		remaining--
	}
	return remaining >= raftQuorumSize(snap.members)
}

// hostDisruptionWouldBreakQuorum is true when this pass would disrupt a Ready host and
// drop the ensemble below Raft quorum (#2069).
//
// Must be called after PrepareHostStatefulSetWithStatus — ObjectStatusSame is assigned only there.
func (w *worker) hostDisruptionWouldBreakQuorum(
	ctx context.Context,
	host *api.Host,
	opts *statefulset.ReconcileOptions,
	snap hostEnsembleSnapshot,
) bool {
	if host == nil || host.IsStopped() {
		return false
	}
	if host.GetReconcileAttributes().GetStatus().Is(types.ObjectStatusRequested) {
		return false
	}
	willDisrupt := !host.GetReconcileAttributes().GetStatus().Is(types.ObjectStatusSame) ||
		w.shouldForceRestartHost(ctx, host) ||
		(opts != nil && opts.ForceRecreate())
	if !willDisrupt {
		return false
	}
	return hostContributesReady(host) && !ensembleQuorumSafeAfterDisrupt(snap, host)
}

func quorumDisruptDeferMessage(host *api.Host, snap hostEnsembleSnapshot) string {
	remaining := snap.readyCount
	if hostContributesReady(host) {
		remaining--
	}
	return fmt.Sprintf(
		"ready=%d remaining=%d quorum=%d",
		snap.readyCount, remaining, raftQuorumSize(snap.members),
	)
}

// verifyHostEnsembleMembership is the extension point for Raft membership
// verification after a host joins in rolling mode. Currently a no-op: STS Ready
// wait already ran. Implement committed-config / leader sync barriers here when
// adopting the fuller rescale design from PR #2041.
func (w *worker) verifyHostEnsembleMembership(ctx context.Context, host *api.Host) error {
	_ = ctx
	_ = host
	return nil
}

// prepareStsReconcileOptsWaitSection sets STS launch waits for Keeper.
// rolling comes from snapshotHostEnsemble before any disruption.
func (w *worker) prepareStsReconcileOptsWaitSection(
	host *api.Host,
	opts *statefulset.ReconcileOptions,
	rolling bool,
) *statefulset.ReconcileOptions {
	if opts == nil {
		opts = statefulset.NewReconcileStatefulSetOptions()
	}
	probes := host.GetCluster().GetReconcile().Host.Wait.Probes

	if probes.GetStartup().IsTrue() || !rolling {
		opts = opts.SetWaitUntilStarted()
		w.a.V(1).M(host).F().Warning("Setting option SetWaitUntilStarted")
	}

	switch {
	case rolling && !probes.GetReadiness().IsFalse():
		opts = opts.SetWaitUntilReady()
		w.a.V(1).M(host).F().Warning("Setting option SetWaitUntilReady (Keeper must become Ready)")
	case !rolling:
		w.a.V(1).M(host).F().Info("Skip WaitUntilReady — bootstrap / resume-from-stopped / recovery")
	}

	return opts
}

// membershipSettleDelay is a best-effort pause after publishing membership
// changes so Raft can settle:
//   - same host count → no delay
//   - upscale → 30s
//   - downscale → 120s (survivors still need time after raft_configuration shrink;
//     peer purge later adds another 1m in clean())
func (w *worker) membershipSettleDelay(cr *apiChk.ClickHouseKeeperInstallation) time.Duration {
	if cr == nil {
		return 0
	}
	ancestorHosts := 0
	if ancestor := cr.GetAncestor(); ancestor != nil {
		ancestorHosts = ancestor.HostsCount()
	}
	currentHosts := cr.HostsCount()

	switch {
	case currentHosts < ancestorHosts:
		return 120 * time.Second
	case currentHosts > ancestorHosts:
		return 30 * time.Second
	default:
		return 0
	}
}

// shardHostsRecoveryFirst returns shard hosts with not-ready replicas first, then
// ready ones — same ordering as CHI reconcileShardWithHosts (#1704).
func shardHostsRecoveryFirst(shard api.IShard, healthy func(*api.Host) bool) []*api.Host {
	if shard == nil {
		return nil
	}
	var recovery, rollout []*api.Host
	shard.WalkHosts(func(host *api.Host) error {
		if healthy(host) {
			rollout = append(rollout, host)
		} else {
			recovery = append(recovery, host)
		}
		return nil
	})
	return append(recovery, rollout...)
}
