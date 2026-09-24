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
	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Raft / ensemble safety policy for CHK.
//
// Owns rolling-vs-bootstrap classification, STS wait probes, quorum disrupt gate,
// recovery-first host ordering, and membership settle delays. The reconciler
// calls into these helpers; it should not restate the policy inline.
//
// Before disrupting a host STS:
//
//  1. snapshotHostEnsemble — freeze rolling vs bootstrap (live Ready, not ancestor)
//  2. prepareStsReconcileOptsWaitSection — Ready wait when rolling AND the host has joined,
//     unless readiness is explicitly disabled
//  3. ensureQuorumSafeToDisruptHost — wait/defer if disrupt would break Raft majority
//
// hostDisruptionWouldBreakQuorum is the tested predicate used inside the façade.
// verifyHostEnsembleMembership is the extension point for a fuller Raft barrier
// (committed /keeper/config + mntr).

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
// next replica while the previous one never rejoined — the fan-out this package prevents.
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

// raftFaultTolerantMinMembers is the smallest ensemble that can lose a member and still hold a
// majority: quorum(1)=1 and quorum(2)=2, so below 3 there is no headroom to protect. Gating those
// sizes defers every roll forever instead of preserving availability the user still has.
const raftFaultTolerantMinMembers = 3

// ensembleHasQuorumHeadroom reports whether the gate can ever pass for this ensemble size.
func ensembleHasQuorumHeadroom(members int) bool {
	return members >= raftFaultTolerantMinMembers
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
func (w *worker) countReadyEnsembleMembers(ctx context.Context, cr api.ICustomResource) (int, error) {
	if w.countReadyEnsembleMembersFn != nil {
		return w.countReadyEnsembleMembersFn(ctx, cr), nil
	}
	if cr == nil {
		return 0, nil
	}
	ready := 0
	var firstErr error
	_ = cr.WalkHosts(func(host *api.Host) error {
		if w.c == nil {
			return nil
		}
		// Always re-read. fillCurSTS froze every peer's CurStatefulSet at reconcile start, so a
		// cached read can never observe the peer recovery this wait exists to wait for.
		sts, err := w.c.kube.STS().Get(ctx, host)
		if err != nil {
			// NotFound is an answer, not a failure: the host simply has no StatefulSet yet.
			// Every host of a fresh CHK is in that state, and so is every host added by a
			// scale-up, so treating it as an error would fail the reconcile that is supposed
			// to create them. It counts as not-Ready, which is exactly what it is.
			if apiErrors.IsNotFound(err) {
				// Gone means gone: leaving the cached object in place would keep reporting a
				// deleted peer as Ready to hostContributesReady and isHostHealthyForReconcile.
				host.Runtime.CurStatefulSet = nil
			} else if firstErr == nil {
				firstErr = err
			}
			return nil
		}
		host.Runtime.CurStatefulSet = sts
		if sts.Status.ReadyReplicas > 0 {
			ready++
		}
		return nil
	})
	return ready, firstErr
}

// snapshotHostEnsemble records rolling vs bootstrap before disrupting a host.
func (w *worker) snapshotHostEnsemble(ctx context.Context, host *api.Host) (hostEnsembleSnapshot, error) {
	if host == nil || host.GetCR() == nil {
		return hostEnsembleSnapshot{}, nil
	}
	cr := host.GetCR()
	// Count members and Ready over the SAME set. Live Raft still runs the ancestor's members
	// until clean() purges them, so a downscale must gate at the ancestor's quorum - but the
	// Ready tally has to span that same set, or a 3->1 downscale would compare 1 Ready against
	// quorum(3) and force the pass to bootstrap, disabling the gate and the Ready wait alike.
	ensemble := quorumSizingEnsemble(cr)
	n := ensemble.HostsCount()
	ready, err := w.countReadyEnsembleMembers(ctx, ensemble)
	if err == nil {
		// Refresh the reconciled host too. When the ensemble is the ancestor its hosts are
		// separately normalized objects, so the walk above never touches the host that
		// hostContributesReady() reads - and this snapshot feeds the FIRST gate evaluation,
		// which for most passes is the only one. Left stale, a host that recovered since
		// fillCurSTS reads as not-contributing and the gate skips a disrupt that does break
		// quorum.
		err = w.refreshHostStatefulSet(ctx, host)
	}
	if err != nil {
		// Fail the pass rather than degrade: an undercount flips rolling to false, which turns
		// off both this gate and the Ready wait - the unsafe direction, and exactly the fan-out
		// this gate exists to prevent. The Get already runs under GetWithRetry, so reaching here
		// means a sustained outage worth a normal error requeue.
		return hostEnsembleSnapshot{}, err
	}
	return hostEnsembleSnapshot{
		rolling:    n <= 1 || ready >= raftQuorumSize(n),
		members:    n,
		readyCount: ready,
	}, nil
}

// refreshQuorumSnapshotCounts updates live Ready counts for an in-flight wait.
// rolling is intentionally frozen — it was captured before any disruption.
func (w *worker) refreshQuorumSnapshotCounts(ctx context.Context, host *api.Host, snap *hostEnsembleSnapshot) error {
	if host == nil || snap == nil || !snap.rolling {
		return nil
	}
	cr := host.GetCR()
	if cr == nil {
		return nil
	}
	// Count over the same set snapshotHostEnsemble used, so readyCount stays comparable with
	// the frozen members. countReadyEnsembleMembers re-reads every host, this one included, so
	// readyCount tracks live peer recovery while rolling stays frozen at its pre-disrupt value.
	ready, err := w.countReadyEnsembleMembers(ctx, quorumSizingEnsemble(cr))
	if err != nil {
		return err
	}
	// Refresh the reconciled host explicitly. When the ensemble is the ancestor its hosts are
	// separately normalized objects - different *api.Host pointers - so the tally above never
	// touches the host hostContributesReady() reads. Left stale, the decrement can disagree
	// with the count it is subtracting from and the gate fails open.
	if err := w.refreshHostStatefulSet(ctx, host); err != nil {
		return err
	}
	snap.readyCount = ready
	return nil
}

// refreshHostStatefulSet re-reads one host's StatefulSet. A missing StatefulSet is not an
// error - see countReadyEnsembleMembers.
func (w *worker) refreshHostStatefulSet(ctx context.Context, host *api.Host) error {
	if w.c == nil || host == nil {
		return nil
	}
	sts, err := w.c.kube.STS().Get(ctx, host)
	if err != nil {
		if apiErrors.IsNotFound(err) {
			host.Runtime.CurStatefulSet = nil
			return nil
		}
		return err
	}
	host.Runtime.CurStatefulSet = sts
	return nil
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
	if snap == nil || !snap.rolling || !ensembleHasQuorumHeadroom(snap.members) {
		return nil
	}
	if !w.hostDisruptionWouldBreakQuorum(ctx, host, opts, *snap) {
		return nil
	}

	// The budget spans the pass, not the host. The first gated host still gets the whole
	// allowance, so a transient blip is absorbed exactly as before; later hosts share what is
	// left, and once it is gone they defer immediately. This bounds the WAIT only - the refusal
	// below is unconditional, so a disrupt that would break quorum is still never allowed.
	remaining := w.quorumDisruptWaitTimeout() - w.quorumWaitSpent
	if remaining <= 0 {
		w.a.V(1).M(host).F().Info(
			"Raft quorum wait budget for this pass is spent - deferring host %s without waiting",
			host.GetName(),
		)
		return w.deferQuorumDisrupt(host, *snap)
	}

	w.a.V(1).M(host).F().Info(
		"Waiting for Raft quorum headroom before disrupting host %s (ready=%d quorum=%d budget=%s)",
		host.GetName(), snap.readyCount, raftQuorumSize(snap.members), remaining,
	)

	waitStart := time.Now()
	// Charge the pass however this returns - proceeded, deferred, errored or cancelled. A
	// successful wait is charged too, on purpose: refunding it would let a flapping peer burn
	// unbounded worker time, which is the whole thing being rationed. Only THIS wait is
	// budgeted - membershipSettleDelay and the StatefulSet launch waits are separate.
	defer func() { w.quorumWaitSpent += time.Since(waitStart) }()

	deadline := waitStart.Add(remaining)
	for time.Now().Before(deadline) {
		// Never sleep past the deadline. Polling a fixed interval regardless of what is left
		// overshoots the budget by up to one interval - 5s by default. That is once per pass, not
		// per host, since the overshooting host exhausts the budget and the rest take the
		// early-defer branch above; bounded, but it is the budget's own ceiling, so honour it.
		if util.WaitContextDoneOrTimeout(ctx, min(w.quorumDisruptPollInterval(), time.Until(deadline))) {
			return ctx.Err()
		}
		// Re-check cancellation explicitly. Once the clamp can hand WaitContextDoneOrTimeout a
		// non-positive duration, both of its select cases are ready at once and Go picks between
		// them at random, so a cancelled context is reported as cancelled only about half the
		// time. Without this the loop would go on to poll the apiserver under a dead context and
		// surface that Get's failure instead of ctx.Err().
		if util.IsContextDone(ctx) {
			return ctx.Err()
		}
		if err := w.refreshQuorumSnapshotCounts(ctx, host, snap); err != nil {
			return err
		}
		if !w.hostDisruptionWouldBreakQuorum(ctx, host, opts, *snap) {
			w.a.V(1).M(host).F().Info(
				"Raft quorum headroom available — proceeding with host %s disruption (ready=%d)",
				host.GetName(), snap.readyCount,
			)
			return nil
		}
	}

	return w.deferQuorumDisrupt(host, *snap)
}

// deferQuorumDisrupt announces the refusal and returns the soft-defer sentinel. Split out so the
// budget-exhausted path and the waited-and-still-unsafe path report identically: from the user's
// side both mean "this host was not touched because the ensemble could not afford it".
func (w *worker) deferQuorumDisrupt(host *api.Host, snap hostEnsembleSnapshot) error {
	w.a.V(1).M(host).F().
		WithEvent(host.GetCR(), a.EventActionReconcile, a.EventReasonHostReconcileDeferredShardSafety).
		Warning(
			"Deferring host StatefulSet reconcile: disrupting %s would drop below Raft quorum (%s)",
			host.GetName(), quorumDisruptDeferMessage(host, snap),
		)
	return common.ErrCRUDDeferred
}

// isHostHealthyForReconcile is true when the host counts as live for recovery-first
// ordering and quorum headroom. Stopped/troubleshoot hosts are intentionally
// unavailable and are ordered after recovery hosts, as the CHI shard reconciler does.
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

// quorumSizingEnsemble returns the host set to size Raft quorum on - the denominator behind
// members, raftQuorumSize and the disrupt gate.
//
// Usually that is the ensemble live Raft is actually running, i.e. the last reconciled (ancestor)
// set. It is NOT always: when the ancestor is too small to tolerate a loss there is no quorum to
// protect, and this falls back to the desired set (see the last paragraph). So read the result as
// "what to size quorum on", never as "what is currently running".
//
// Membership is static. The generator emits keeper_server/raft_configuration as a plain config
// section, and enable_reconfiguration is shipped explicitly disabled
// (config/chk/keeper_config.d/01-keeper-03-enable-reconfig.xml), so a running Keeper holds the
// membership it started with. Publishing the ConfigMap for a scale-up therefore does NOT admit the new
// servers to the running Raft - they join only as the existing pods roll onto the new config.
//
// Sizing growth on the desired set inflates the denominator against a membership that does not
// exist yet, and the damage is in the unsafe direction: a 3->5 with one member already down
// counts 2 Ready against quorum(5)=3, so rolling goes false, and a bootstrap pass switches off
// both this gate and the Ready wait - the unguarded fan-out this gate exists to prevent. Shrink is
// the same story from the other side: departing peers keep voting until clean() purges them.
//
// An ancestor too small to tolerate a loss is not a quorum worth protecting, so fall back to the
// desired set there. That also keeps growth classified as bootstrap: a 1->3 sized at 1 would read
// rolling (members<=1 is rolling unconditionally), putting a Ready wait on the one EXISTING host
// while the ensemble it must reach quorum with is still being created. The new hosts are already
// safe either way - joinedEnsemble below denies them the Ready wait.
func quorumSizingEnsemble(cr api.ICustomResource) api.ICustomResource {
	if cr == nil {
		return nil
	}
	if ensembleHasQuorumHeadroom(cr.GetAncestor().HostsCount()) {
		return cr.GetAncestor()
	}
	return cr
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
	if !snap.rolling || !ensembleHasQuorumHeadroom(snap.members) {
		return true
	}
	remaining := snap.readyCount
	if hostContributesReady(host) {
		remaining--
	}
	return remaining >= raftQuorumSize(snap.members)
}

// hostDisruptionWouldBreakQuorum is true when this pass would disrupt a Ready host and
// drop the ensemble below Raft quorum.
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
// adopting a fuller rescale design.
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

	// rolling describes the ENSEMBLE; the Ready wait is a per-HOST decision, and a host that is
	// not yet a member of the live ensemble must never carry it. Keeper membership is static, so
	// a brand-new peer cannot reach /ready until the existing members roll onto the config that
	// admits it - and recovery-first ordering reconciles the new hosts before any of them do.
	// Waiting there can only expire into chkStatefulSetFallback's ErrCRUDAbort, wedging an upscale
	// from a live ensemble (3->5 and larger; a 2->3 is instead held by the gate, which sizes on
	// the desired 3 and refuses to disrupt either live member). The quorum gate keeps reading
	// rolling unchanged; only this consumer needs the per-host narrowing, which is what the
	// code before this gate expressed as GetReadiness().IsTrue() && host.HasAncestor().
	//
	// HasAncestor() alone is not enough. It resolves through .status.normalizedCompleted, which
	// only a fully successful pass stamps, so an ensemble whose first reconcile never finished -
	// a deferral, an error, an operator killed mid-pass - is live but ancestor-less, and dropping
	// the Ready wait there would unserialize a genuine rolling update. On a 2-member CHK it,
	// recovery-first ordering and chkStatefulSetFallback are the whole of the protection, since
	// the quorum gate has no headroom to engage below raftFaultTolerantMinMembers. A host already
	// reporting Ready is a member whatever the status says, while a host this pass is adding has
	// no StatefulSet at all and so fails both terms. CurStatefulSet was refreshed moments ago by
	// snapshotHostEnsemble.
	joinedEnsemble := host.HasAncestor() || hostContributesReady(host)

	// A host outside the live ensemble still has to wait to START. The code before this gate spelled
	// this `probes.GetStartup().IsTrue() || !host.HasAncestor()`; narrowing it to !rolling alone
	// would leave a scale-up host with startup:"false" waiting for nothing whatsoever, and the
	// host loop would move on before this Keeper had even begun booting.
	if probes.GetStartup().IsTrue() || !rolling || !joinedEnsemble {
		opts = opts.SetWaitUntilStarted()
		w.a.V(1).M(host).F().Warning("Setting option SetWaitUntilStarted")
	}

	switch {
	case rolling && joinedEnsemble && !probes.GetReadiness().IsFalse():
		opts = opts.SetWaitUntilReady()
		w.a.V(1).M(host).F().Warning("Setting option SetWaitUntilReady (Keeper must become Ready)")
	case rolling && !joinedEnsemble:
		w.a.V(1).M(host).F().Info("Skip WaitUntilReady — host has not joined the live ensemble yet")
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
	// GetAncestor() returns a typed nil and HostsCount() walks via WalkHosts(), which
	// guards a nil receiver - no explicit nil check needed.
	ancestorHosts := cr.GetAncestor().HostsCount()
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

// shardHostsRecoveryFirst returns shard hosts with not-ready replicas first, then ready ones —
// same ordering as CHI reconcileShardWithHosts. The partition is STABLE: within each
// group hosts keep their declaration order, which is what lets tests assert an exact sequence.
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
