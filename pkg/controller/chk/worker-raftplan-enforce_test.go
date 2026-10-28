// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package chk

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/controller/chk/raftplan"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
)

func TestRaftPlanEnforcementOptIn(t *testing.T) {
	t.Setenv("CHOP_CHK_RAFTPLAN_ENFORCE", "")
	require.False(t, raftPlanEnforceEnabled())
	t.Setenv("CHOP_CHK_RAFTPLAN_ENFORCE", "1")
	require.True(t, raftPlanEnforceEnabled())
	t.Setenv("CHOP_CHK_RAFTPLAN_ENFORCE", "false")
	require.False(t, raftPlanEnforceEnabled())
}

func TestRaftPlanEnforcedAdmissionVerdicts(t *testing.T) {
	ids := []raftplan.HostID{"h0", "h1", "h2"}
	makeSnap := func() raftplan.Snapshot {
		return raftplan.Snapshot{
			Established: true, PublishedKnown: true,
			DesiredHosts: ids, AncestorHosts: ids[:1], PublishedHosts: ids[:1],
			Hosts: map[raftplan.HostID]raftplan.HostState{
				"h0": {ID: "h0", InAncestor: true, StatefulSet: raftplan.PresencePresent, Ready: true, ReadyKnown: true},
				"h1": {ID: "h1", StatefulSet: raftplan.PresenceAbsent, ReadyKnown: true},
				"h2": {ID: "h2", StatefulSet: raftplan.PresenceAbsent, ReadyKnown: true},
			},
		}
	}
	t.Run("first joiner admitted, second held", func(t *testing.T) {
		s := makeSnap()
		plan, err := raftPlanAdmissionVerdict(s, "h1")
		require.NoError(t, err)
		require.True(t, plan.MayCreate["h1"])
		_, err = raftPlanAdmissionVerdict(s, "h2")
		require.ErrorIs(t, err, common.ErrCRUDDeferred)
	})
	t.Run("unready joiner blocks next", func(t *testing.T) {
		s := makeSnap()
		s.PublishedHosts = ids[:2]
		s.Hosts["h1"] = raftplan.HostState{ID: "h1", StatefulSet: raftplan.PresencePresent, ReadyKnown: true}
		plan, err := raftPlanAdmissionVerdict(s, "h2")
		require.ErrorIs(t, err, common.ErrCRUDDeferred)
		require.Equal(t, raftplan.HostID("h1"), plan.BlockedBy)
	})
	t.Run("ready joiner unblocks next", func(t *testing.T) {
		s := makeSnap()
		s.PublishedHosts = ids[:2]
		s.Hosts["h1"] = raftplan.HostState{ID: "h1", StatefulSet: raftplan.PresencePresent, ReadyKnown: true, Ready: true}
		_, err := raftPlanAdmissionVerdict(s, "h2")
		require.NoError(t, err)
	})
	t.Run("unknown publication fails closed", func(t *testing.T) {
		s := makeSnap()
		s.PublishedKnown = false
		_, err := raftPlanAdmissionVerdict(s, "h1")
		require.Error(t, err)
		require.False(t, errors.Is(err, common.ErrCRUDDeferred))
	})
	t.Run("unknown StatefulSet fails closed", func(t *testing.T) {
		s := makeSnap()
		s.Hosts["h1"] = raftplan.HostState{ID: "h1", StatefulSet: raftplan.PresenceUnknown}
		_, err := raftPlanAdmissionVerdict(s, "h1")
		require.Error(t, err)
	})
	t.Run("stopped defers missing member", func(t *testing.T) {
		s := makeSnap()
		s.Stopped = true
		_, err := raftPlanAdmissionVerdict(s, "h1")
		require.ErrorIs(t, err, common.ErrCRUDDeferred)
	})
}

func TestRaftPlanEnforcedDisruptionGate(t *testing.T) {
	t.Setenv("CHOP_CHK_RAFTPLAN_ENFORCE", "1")
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		ready int
		status types.ObjectStatus
		forceRecreate bool
		blocked bool
	}{
		{"degraded", 2, types.ObjectStatusModified, false, true},
		{"healthy", 3, types.ObjectStatusModified, false, false},
		{"unchanged", 2, types.ObjectStatusSame, false, false},
		{"force recreate", 2, types.ObjectStatusSame, true, true},
		{"new", 2, types.ObjectStatusRequested, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			host := hostOnCR(chkWithHosts(3))
			host.Runtime.CurStatefulSet = &apps.StatefulSet{}
			host.Runtime.CurStatefulSet.Status.ReadyReplicas = 1
			host.GetReconcileAttributes().SetStatus(tc.status)
			w := &worker{a: a.NewAnnouncer(nil, nil)}
			snap := hostEnsembleSnapshot{rolling: true, members: 3, readyCount: tc.ready}
			var opts *statefulset.ReconcileOptions
			if tc.forceRecreate {
				opts = statefulset.NewReconcileStatefulSetOptions().SetForceRecreate()
			}
			blocked, err := w.raftPlanDisruptionBlocked(ctx, host, opts, snap)
			require.NoError(t, err)
			require.Equal(t, tc.blocked, blocked)
		})
	}
	t.Run("inconsistent quorum counts fail closed", func(t *testing.T) {
		host := hostOnCR(chkWithHosts(3))
		host.Runtime.CurStatefulSet = &apps.StatefulSet{}
		host.Runtime.CurStatefulSet.Status.ReadyReplicas = 1
		host.GetReconcileAttributes().SetStatus(types.ObjectStatusModified)
		_, err := (&worker{a: a.NewAnnouncer(nil, nil)}).raftPlanDisruptionBlocked(ctx, host, nil,
			hostEnsembleSnapshot{rolling: true, members: 3, readyCount: 4})
		require.Error(t, err)
	})
	t.Run("existing quorum gate defers", func(t *testing.T) {
		host := hostOnCR(chkWithHosts(3))
		host.Runtime.CurStatefulSet = &apps.StatefulSet{}
		host.Runtime.CurStatefulSet.Status.ReadyReplicas = 1
		host.GetReconcileAttributes().SetStatus(types.ObjectStatusModified)
		w := &worker{
			a: a.NewAnnouncer(nil, nil),
			countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 2 },
			quorumDisruptPollOverride: 2 * time.Millisecond,
			quorumDisruptWaitOverride: 10 * time.Millisecond,
		}
		snap := hostEnsembleSnapshot{rolling: true, members: 3, readyCount: 2}
		err := w.ensureQuorumSafeToDisruptHost(ctx, host, nil, &snap)
		require.ErrorIs(t, err, common.ErrCRUDDeferred)
	})
}
