// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package chk

import (
	"context"
	"errors"
	"os"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/chk/raftplan"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
)

// This env toggle is opt-in. Shadow mode never replaces or modifies a legacy
// decision, status condition, ConfigMap or StatefulSet. It performs extra live
// reads and should only be enabled on a controlled test installation.
func raftPlanShadowEnabled() bool {
	return os.Getenv("CHOP_CHK_RAFTPLAN_SHADOW") == "1"
}

// shadowRaftAdmission compares the *per-host* legacy gate with the new planner.
// The legacy preliminary exclusion is intentionally not compared directly:
// it stages every missing joiner, then publishes exactly one at its host gate,
// while PlanAdmission describes the membership after that publish.
func (w *worker) shadowRaftAdmission(ctx context.Context, host *api.Host, legacyErr error) {
	if !raftPlanShadowEnabled() || host == nil || host.GetCR() == nil || !hostIsStagedScaleUpJoin(host) {
		return
	}
	cr, ok := host.GetCR().(*apiChk.ClickHouseKeeperInstallation)
	if !ok {
		return
	}
	snapshot, err := w.observeRaftPlanAdmission(ctx, cr)
	if err != nil {
		w.a.V(1).M(host).F().Warning("CHK raftplan shadow admission observation failed: %v", err)
		return
	}
	id := raftPlanHostID(host)
	state, ok := snapshot.Hosts[id]
	if !ok || state.StatefulSet != raftplan.PresenceAbsent {
		return // existing STS is not subject to the legacy admission gate
	}
	plan := raftplan.PlanAdmission(snapshot)
	if plan.Reason == raftplan.ObservationUnknown {
		return // not enough information to compare decisions
	}
	legacyAllowed := legacyErr == nil
	if legacyErr != nil && !errors.Is(legacyErr, common.ErrCRUDDeferred) &&
		!errors.Is(legacyErr, errSkipStagedAdmit) {
		return // unrelated hard failure, not a safety-gate decision
	}
	if plan.MayCreate[id] != legacyAllowed {
		w.a.M(host).F().Warning(
			"CHK raftplan shadow ADMISSION MISMATCH host=%s legacyAllowed=%t plannerAllowed=%t plannerReason=%s blockedBy=%s",
			id, legacyAllowed, plan.MayCreate[id], plan.Reason, plan.BlockedBy)
	}
}

// shadowRaftDisruption compares policy decisions just after the live quorum
// snapshot and STS mutation options have been prepared, before the authoritative
// legacy gate starts polling. It must never cause a disruptive mutation.
func (w *worker) shadowRaftDisruption(
	ctx context.Context, host *api.Host, opts *statefulset.ReconcileOptions,
	snap hostEnsembleSnapshot,
) {
	if !raftPlanShadowEnabled() || host == nil {
		return
	}
	policySnapshot := w.raftPlanDisruptionSnapshot(ctx, host, opts, snap)
	result := raftplan.CheckDisruption(policySnapshot, raftPlanHostID(host))
	legacyAllowed := !w.hostDisruptionWouldBreakQuorum(ctx, host, opts, snap)
	if result.Allowed != legacyAllowed {
		w.a.M(host).F().Warning(
			"CHK raftplan shadow DISRUPTION MISMATCH host=%s legacyAllowed=%t plannerAllowed=%t reason=%s ready=%d members=%d rolling=%t",
			host.GetName(), legacyAllowed, result.Allowed, result.Reason,
			snap.readyCount, snap.members, snap.rolling)
	}
}
