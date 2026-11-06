// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package chk

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/controller/chk/raftplan"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
	apps "k8s.io/api/apps/v1"
)

// Uses the real legacy deferUnreadyStagedAdmit on the same fake live StatefulSets
// that feed the new observer. The preliminary exclusion tags are deliberately
// not the oracle: all not-yet-created members are excluded before the host loop,
// while admission occurs immediately before a specific STS create.
func TestRaftPlanAdmissionParityWithLegacyGate(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name          string
		desired       int
		ancestor      int
		readyJoiner   bool
		hasJoiner     bool
		target        int
		published     []int
		stopped       bool
		expectAllowed bool
	}{
		{name: "1 to 3 first joiner", desired: 3, ancestor: 1, target: 1, published: []int{0}, expectAllowed: true},
		{name: "1 to 3 blocked by unready joiner", desired: 3, ancestor: 1, hasJoiner: true, target: 2, published: []int{0, 1}},
		{name: "1 to 3 advances after Ready", desired: 3, ancestor: 1, hasJoiner: true, readyJoiner: true, target: 2, published: []int{0, 1}, expectAllowed: true},
		{name: "2 to 3 final joiner", desired: 3, ancestor: 2, target: 2, published: []int{0, 1}, expectAllowed: true},
		{name: "missing published joiner is repaired", desired: 3, ancestor: 1, target: 1, published: []int{0, 1}, expectAllowed: true},
		{name: "3 to 5 preserves full fan-out", desired: 5, ancestor: 3, target: 3, published: []int{0, 1, 2}, expectAllowed: true},
		{name: "stop prevents admission", desired: 3, ancestor: 1, target: 2, published: []int{0, 1}, stopped: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cr := chkWithHosts(tc.desired)
			cr.SetAncestor(chkWithHosts(tc.ancestor))
			if tc.stopped {
				cr.Spec.Stop = types.NewStringBool(true)
			}
			fake := newRaftFakeSTS()
			for i := 0; i < tc.ancestor; i++ {
				fake.setReady(hostAtOnCR(cr, i), 1)
			}
			if tc.hasJoiner {
				ready := int32(0)
				if tc.readyJoiner {
					ready = 1
				}
				fake.setReady(hostAtOnCR(cr, 1), ready)
			}
			w := raftWorkerWithSTS(fake)
			w.a = a.NewAnnouncer(nil, nil)
			pub := make([]raftplan.HostID, 0, len(tc.published))
			for _, i := range tc.published {
				pub = append(pub, raftPlanHostID(hostAtOnCR(cr, i)))
			}
			snapshot, err := w.observeRaftPlanHosts(ctx, cr, pub, true)
			require.NoError(t, err)
			plan := raftplan.PlanAdmission(snapshot)
			target := hostAtOnCR(cr, tc.target)
			require.Equal(t, raftplan.PresenceAbsent, snapshot.Hosts[raftPlanHostID(target)].StatefulSet)
			legacyErr := w.deferUnreadyStagedAdmit(ctx, target)
			legacyAllowed := legacyErr == nil
			if legacyErr != nil {
				require.True(t, errors.Is(legacyErr, common.ErrCRUDDeferred) || errors.Is(legacyErr, errSkipStagedAdmit), "unexpected legacy error: %v", legacyErr)
			}
			require.Equal(t, tc.expectAllowed, legacyAllowed, "legacy fixture invalid")
			require.Equal(t, legacyAllowed, plan.MayCreate[raftPlanHostID(target)], "planner disagrees with legacy host gate")
		})
	}
}

func TestRaftPlanDisruptionParityWithLegacyGate(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name        string
		memberCount int
		readyCount  int
		rolling     bool
		targetReady bool
		status      types.ObjectStatus
		opts        *statefulset.ReconcileOptions
		wantAllowed bool
	}{
		{name: "degraded 3 members", memberCount: 3, readyCount: 2, rolling: true, targetReady: true, status: types.ObjectStatusModified},
		{name: "healthy 3 members", memberCount: 3, readyCount: 3, rolling: true, targetReady: true, status: types.ObjectStatusModified, wantAllowed: true},
		{name: "unchanged STS", memberCount: 3, readyCount: 2, rolling: true, targetReady: true, status: types.ObjectStatusSame, wantAllowed: true},
		{name: "new STS", memberCount: 3, readyCount: 2, rolling: true, targetReady: true, status: types.ObjectStatusRequested, wantAllowed: true},
		{name: "force recreate", memberCount: 3, readyCount: 2, rolling: true, targetReady: true, status: types.ObjectStatusSame, opts: statefulset.NewReconcileStatefulSetOptions().SetForceRecreate()},
		{name: "bootstrap 3 members", memberCount: 3, readyCount: 1, rolling: false, targetReady: true, status: types.ObjectStatusModified, wantAllowed: true},
		{name: "two members compatibility", memberCount: 2, readyCount: 2, rolling: true, targetReady: true, status: types.ObjectStatusModified, wantAllowed: true},
		{name: "unready target", memberCount: 3, readyCount: 2, rolling: true, targetReady: false, status: types.ObjectStatusModified, wantAllowed: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			host := hostOnCR(chkWithHosts(tc.memberCount))
			host.GetReconcileAttributes().SetStatus(tc.status)
			host.Runtime.CurStatefulSet = &apps.StatefulSet{}
			if tc.targetReady {
				host.Runtime.CurStatefulSet.Status.ReadyReplicas = 1
			}
			w := &worker{}
			snap := hostEnsembleSnapshot{rolling: tc.rolling, members: tc.memberCount, readyCount: tc.readyCount}
			newSnapshot := w.raftPlanDisruptionSnapshot(ctx, host, tc.opts, snap)
			got := raftplan.CheckDisruption(newSnapshot, raftPlanHostID(host))
			legacyAllowed := !w.hostDisruptionWouldBreakQuorum(ctx, host, tc.opts, snap)
			require.Equal(t, tc.wantAllowed, legacyAllowed, "legacy fixture invalid")
			require.Equal(t, legacyAllowed, got.Allowed, "planner diverged from legacy quorum gate")
		})
	}
}
