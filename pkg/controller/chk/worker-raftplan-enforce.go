// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package chk

import (
	"context"
	"fmt"
	"os"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/chk/raftplan"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
)

// raftPlanEnforceEnabled opts into planner decisions. The legacy safety
// policy stays the default until parity and e2e have been verified.
// Existing preliminary staging, publication, waits, and rollback remain.
func raftPlanEnforceEnabled() bool {
	return os.Getenv("CHOP_CHK_RAFTPLAN_ENFORCE") == "1"
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
