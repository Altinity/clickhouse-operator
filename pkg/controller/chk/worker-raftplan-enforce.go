// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package chk

import (
	"context"
	"fmt"
	"os"
	"time"

	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/altinity/clickhouse-operator/pkg/controller/common/poller"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/chk/raftplan"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
)

// raftPlanEnforceEnabled runs the Raft planner by default in this test
// branch, so the standard e2e operator install needs no environment changes.
// Setting CHOP_CHK_RAFTPLAN_ENFORCE=0 is an optional emergency rollback;
// the existing preliminary staging, publication, waits, and rollback remain.
func raftPlanEnforceEnabled() bool {
	return os.Getenv("CHOP_CHK_RAFTPLAN_ENFORCE") != "0"
}

// raftPlanAdmissionVerdict is the pure-to-worker boundary. An unknown
// membership observation cannot authorize creation of another Keeper.
func raftPlanAdmissionVerdict(snapshot raftplan.Snapshot, id raftplan.HostID) (raftplan.AdmissionPlan, error) {
	plan := raftplan.PlanAdmission(snapshot)
	if plan.Reason == raftplan.ObservationUnknown {
		return plan, fmt.Errorf("Raft admission state unknown for %s: %s", id, plan.Reason)
	}
	if !plan.MayCreate[id] {
		return plan, common.ErrCRUDDeferred
	}
	// The existing publisher still writes membership immediately before
	// StatefulSet creation. Ensure the planner includes the admitted host.
	for _, included := range plan.IncludedHosts {
		if included == id {
			return plan, nil
		}
	}
	return plan, fmt.Errorf("Raft planner allowed creating %s without publishing its membership", id)
}

// enforceRaftPlanAdmission replaces deferUnreadyStagedAdmit for staged
// 1->N/2->3 joins only. Existing members still reconcile while the next
// joiner is held so they can adopt the already published Raft config.
func (w *worker) enforceRaftPlanAdmission(ctx context.Context, host *api.Host) error {
	if !hostIsStagedScaleUpJoin(host) || w.c == nil {
		return nil
	}
	// Keep an already created STS published, including after an interrupted
	// Create. Never re-admit it or roll its membership back.
	own, err := w.stagedJoinerStatefulSet(ctx, host)
	if err != nil {
		return err
	}
	if own != nil {
		return nil
	}
	if host.IsStopped() {
		return errSkipStagedAdmit
	}
	cr, ok := host.GetCR().(*apiChk.ClickHouseKeeperInstallation)
	if !ok {
		return fmt.Errorf("Raft admission host %s has no CHK", host.GetName())
	}
	observed, err := w.observeRaftPlanAdmission(ctx, cr)
	if err != nil {
		return fmt.Errorf("observe Raft admission for %s: %w", host.GetName(), err)
	}
	id := raftPlanHostID(host)
	state, exists := observed.Hosts[id]
	if !exists || state.StatefulSet == raftplan.PresenceUnknown {
		return fmt.Errorf("Raft admission observation missing StatefulSet state for %s", id)
	}
	if state.StatefulSet == raftplan.PresencePresent {
		return nil // another observation saw the StatefulSet appear
	}
	plan, err := raftPlanAdmissionVerdict(observed, id)
	if err == common.ErrCRUDDeferred {
		// "Not Ready" covers two very different situations: a Pod that has
		// started but is waiting for Raft quorum (keep deferring), and one
		// that has never started (e.g. ImagePullBackOff). In the latter case
		// a repeated soft deferral can mask the StatefulSet startup timeout.
		// The deadline is based on live Kubernetes creation timestamps so
		// it survives controller restarts and repeated reconcile passes.
		if plan.Reason == raftplan.WaitingForJoiner && plan.BlockedBy != "" {
			expired, checkErr := w.stagedJoinerStartupExpired(ctx, cr, plan.BlockedBy)
			if checkErr != nil {
				return checkErr
			}
			if expired {
				w.a.M(host).F().Warning(
					"Raft staged joiner %s never started within StatefulSet timeout; aborting rather than deferring %s",
					plan.BlockedBy, id,
				)
				return common.ErrCRUDAbort
			}
		}
		w.a.WithEvent(host.GetCR(), a.EventActionReconcile, a.EventReasonReconcileInProgress).
			WithAction(host.GetCR()).M(host).F().Info(
				"Raft planner defers admitting %s: reason=%s blockedBy=%s",
				id, plan.Reason, plan.BlockedBy,
			)
	}
	return err
}

// raftPlanDisruptionBlocked supplies the existing quorum wait with its
// authoritative predicate. A fresh Ready count is obtained by the caller
// before each re-evaluation. Unknown or inconsistent state fails closed.
// Without opt-in this invokes the exact legacy quorum predicate.
func (w *worker) raftPlanDisruptionBlocked(
	ctx context.Context, host *api.Host,
	opts *statefulset.ReconcileOptions, snap hostEnsembleSnapshot,
) (bool, error) {
	if !raftPlanEnforceEnabled() {
		return w.hostDisruptionWouldBreakQuorum(ctx, host, opts, snap), nil
	}
	if host == nil {
		return true, fmt.Errorf("Raft disruption host is nil")
	}
	observed := w.raftPlanDisruptionSnapshot(ctx, host, opts, snap)
	decision := raftplan.CheckDisruption(observed, raftPlanHostID(host))
	if decision.Reason == raftplan.DisruptionStateUnknown {
		return true, fmt.Errorf(
			"Raft planner cannot determine disruption safety for %s (ready=%d members=%d rolling=%t)",
			host.GetName(), snap.readyCount, snap.members, snap.rolling,
		)
	}
	return !decision.Allowed, nil
}

// stagedJoinerStartupDeadlineExceeded is an executor-side timeout, not Raft
// policy. A started-but-unready Keeper can legitimately need its existing
// peers to load the new membership, so only an unstarted Pod can time out.
// Use the Pod's creation time (not a per-reconcile timer) to avoid resetting
// the deadline on every soft deferral. For a missing Pod, use the StatefulSet
// creation time if it is available.
func stagedJoinerStartupDeadlineExceeded(
	pod *core.Pod, stsCreated, now time.Time, timeout time.Duration,
) bool {
	if timeout <= 0 {
		return false
	}
	var startedAt time.Time
	if pod != nil {
		if k8s.PodHasAllContainersStarted(pod) {
			return false
		}
		startedAt = pod.CreationTimestamp.Time
	} else {
		startedAt = stsCreated
	}
	return !startedAt.IsZero() && now.Sub(startedAt) >= timeout
}

// stagedJoinerStartupExpired checks only a joiner that the pure planner
// identified as blocking an absent member. The existing StatefulSet create
// wait remains the first timeout path. This is a cross-reconcile backstop
// when the joiner's STS already exists and repeated admission deferrals would
// otherwise leave the CHK indefinitely InProgress.
func (w *worker) stagedJoinerStartupExpired(
	ctx context.Context, cr *apiChk.ClickHouseKeeperInstallation, id raftplan.HostID,
) (bool, error) {
	// Legacy lightweight host-loop tests don't construct Pod clients; the
	// planner and real StatefulSet observation are still exercised there.
	if w == nil || w.c == nil || w.c.kube == nil || w.c.kube.Pod() == nil {
		return false, nil
	}
	var blocker *api.Host
	_ = cr.WalkHosts(func(peer *api.Host) error {
		if peer != nil && raftPlanHostID(peer) == id {
			blocker = peer
		}
		return nil
	})
	if blocker == nil {
		return false, fmt.Errorf("Raft planner blocked by unknown joiner %s", id)
	}
	sts, err := w.stagedJoinerStatefulSet(ctx, blocker)
	if err != nil {
		return false, fmt.Errorf("read blocked joiner StatefulSet %s: %w", id, err)
	}
	if sts == nil {
		return false, nil // lost STS: admission observation must be repeated
	}
	pod, err := w.c.kube.Pod().Get(ctx, blocker)
	if apiErrors.IsNotFound(err) {
		pod = nil
	} else if err != nil {
		return false, fmt.Errorf("read blocked joiner Pod %s: %w", id, err)
	} else if pod == nil {
		return false, fmt.Errorf("nil Pod with successful Get for blocked joiner %s", id)
	}
	timeout := poller.NewOptionsFromConfig().Timeout
	return stagedJoinerStartupDeadlineExceeded(
		pod, sts.CreationTimestamp.Time, time.Now(), timeout,
	), nil
}
