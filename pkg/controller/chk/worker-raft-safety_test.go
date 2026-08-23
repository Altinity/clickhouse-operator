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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
)

func TestRaftQuorumSize(t *testing.T) {
	require.Equal(t, 0, raftQuorumSize(0))
	require.Equal(t, 1, raftQuorumSize(1))
	require.Equal(t, 2, raftQuorumSize(3))
	require.Equal(t, 3, raftQuorumSize(5))
}

func TestSnapshotHostEnsemble(t *testing.T) {
	ctx := context.Background()

	t.Run("single host is rolling even with 0 ReadyReplicas", func(t *testing.T) {
		w := &worker{
			countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 0 },
		}
		host := hostOnCR(chkWithHosts(1))
		snap := w.snapshotHostEnsemble(ctx, host)
		require.True(t, snap.rolling)
		require.Equal(t, 1, snap.members)
		require.Equal(t, 0, snap.readyCount)
	})

	t.Run("multi-host without live quorum is bootstrap", func(t *testing.T) {
		w := &worker{
			countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 0 },
		}
		host := hostOnCR(chkWithHosts(3))
		snap := w.snapshotHostEnsemble(ctx, host)
		require.False(t, snap.rolling)
		require.Equal(t, 3, snap.members)
		require.Equal(t, 0, snap.readyCount)
	})

	t.Run("multi-host below quorum is bootstrap", func(t *testing.T) {
		w := &worker{
			countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 1 },
		}
		host := hostOnCR(chkWithHosts(3))
		snap := w.snapshotHostEnsemble(ctx, host)
		require.False(t, snap.rolling)
	})

	t.Run("multi-host at quorum is rolling", func(t *testing.T) {
		w := &worker{
			countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 2 },
		}
		host := hostOnCR(chkWithHosts(3))
		snap := w.snapshotHostEnsemble(ctx, host)
		require.True(t, snap.rolling)
		require.Equal(t, 2, snap.readyCount)
	})
}

func TestEnsembleQuorumSafeAfterDisrupt(t *testing.T) {
	host := hostOnCR(chkWithHosts(3))
	host.Runtime.CurStatefulSet = &apps.StatefulSet{}
	host.Runtime.CurStatefulSet.Status.ReadyReplicas = 1

	t.Run("bootstrap mode is always safe", func(t *testing.T) {
		snap := hostEnsembleSnapshot{rolling: false, members: 3, readyCount: 0}
		require.True(t, ensembleQuorumSafeAfterDisrupt(snap, host))
	})

	t.Run("safe when siblings keep quorum", func(t *testing.T) {
		snap := hostEnsembleSnapshot{rolling: true, members: 3, readyCount: 3}
		require.True(t, ensembleQuorumSafeAfterDisrupt(snap, host))
	})

	t.Run("unsafe when remaining would be below quorum", func(t *testing.T) {
		snap := hostEnsembleSnapshot{rolling: true, members: 3, readyCount: 2}
		require.False(t, ensembleQuorumSafeAfterDisrupt(snap, host))
	})

	t.Run("sole host is always safe", func(t *testing.T) {
		solo := hostOnCR(chkWithHosts(1))
		solo.Runtime.CurStatefulSet = &apps.StatefulSet{}
		solo.Runtime.CurStatefulSet.Status.ReadyReplicas = 1
		snap := hostEnsembleSnapshot{rolling: true, members: 1, readyCount: 1}
		require.True(t, ensembleQuorumSafeAfterDisrupt(snap, solo))
	})
}

func TestHostDisruptionWouldBreakQuorum(t *testing.T) {
	ctx := context.Background()
	w := &worker{}
	host := hostOnCR(chkWithHosts(3))
	host.Runtime.CurStatefulSet = &apps.StatefulSet{}
	host.Runtime.CurStatefulSet.Status.ReadyReplicas = 1
	host.GetReconcileAttributes().SetStatus(types.ObjectStatusModified)
	snap := hostEnsembleSnapshot{rolling: true, members: 3, readyCount: 2}

	t.Run("no-op for new host", func(t *testing.T) {
		newHost := hostOnCR(chkWithHosts(3))
		newHost.GetReconcileAttributes().SetStatus(types.ObjectStatusRequested)
		require.False(t, w.hostDisruptionWouldBreakQuorum(ctx, newHost, nil, snap))
	})

	t.Run("no-op when STS is unchanged", func(t *testing.T) {
		same := hostOnCR(chkWithHosts(3))
		same.Runtime.CurStatefulSet = &apps.StatefulSet{}
		same.Runtime.CurStatefulSet.Status.ReadyReplicas = 1
		same.GetReconcileAttributes().SetStatus(types.ObjectStatusSame)
		require.False(t, w.hostDisruptionWouldBreakQuorum(ctx, same, nil, snap))
	})

	t.Run("blocks disruptive roll without quorum headroom", func(t *testing.T) {
		require.True(t, w.hostDisruptionWouldBreakQuorum(ctx, host, nil, snap))
	})

	t.Run("allows disruptive roll when siblings keep quorum", func(t *testing.T) {
		bigSnap := hostEnsembleSnapshot{rolling: true, members: 3, readyCount: 3}
		require.False(t, w.hostDisruptionWouldBreakQuorum(ctx, host, nil, bigSnap))
	})

	t.Run("force recreate counts as disruptive", func(t *testing.T) {
		same := hostOnCR(chkWithHosts(3))
		same.Runtime.CurStatefulSet = &apps.StatefulSet{}
		same.Runtime.CurStatefulSet.Status.ReadyReplicas = 1
		same.GetReconcileAttributes().SetStatus(types.ObjectStatusSame)
		opts := statefulset.NewReconcileStatefulSetOptions().SetForceRecreate()
		require.True(t, w.hostDisruptionWouldBreakQuorum(ctx, same, opts, snap))
	})
}

func TestChkStatefulSetFallbackAborts(t *testing.T) {
	f := newChkStatefulSetFallback()
	require.Equal(t, common.ErrCRUDAbort, f.OnStatefulSetCreateFailed(nil, nil))
	require.Equal(t, common.ErrCRUDAbort, f.OnStatefulSetUpdateFailed(nil, nil, nil, nil))
}

func TestErrCRUDDeferredIsDistinctFromAbort(t *testing.T) {
	require.False(t, errors.Is(common.ErrCRUDDeferred, common.ErrCRUDAbort))
	require.False(t, errors.Is(common.ErrCRUDAbort, common.ErrCRUDDeferred))
}

func TestEnsureQuorumSafeToDisruptHost(t *testing.T) {
	ctx := context.Background()
	host := hostOnCR(chkWithHosts(3))
	host.Runtime.CurStatefulSet = &apps.StatefulSet{}
	host.Runtime.CurStatefulSet.Status.ReadyReplicas = 1
	host.GetReconcileAttributes().SetStatus(types.ObjectStatusModified)
	snap := hostEnsembleSnapshot{rolling: true, members: 3, readyCount: 2}

	t.Run("returns immediately when already safe", func(t *testing.T) {
		w := &worker{}
		safeSnap := hostEnsembleSnapshot{rolling: true, members: 3, readyCount: 3}
		require.NoError(t, w.ensureQuorumSafeToDisruptHost(ctx, host, nil, &safeSnap))
	})

	t.Run("waits until ready count increases", func(t *testing.T) {
		var ready atomic.Int32
		ready.Store(2)
		w := &worker{
			countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int {
				return int(ready.Load())
			},
			quorumDisruptPollOverride: 5 * time.Millisecond,
			quorumDisruptWaitOverride: 200 * time.Millisecond,
		}
		waitSnap := snap
		go func() {
			time.Sleep(20 * time.Millisecond)
			ready.Store(3)
		}()
		require.NoError(t, w.ensureQuorumSafeToDisruptHost(ctx, host, nil, &waitSnap))
	})

	t.Run("defers after wait budget expires", func(t *testing.T) {
		w := &worker{
			countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 2 },
			quorumDisruptPollOverride:   5 * time.Millisecond,
			quorumDisruptWaitOverride:    20 * time.Millisecond,
		}
		waitSnap := snap
		err := w.ensureQuorumSafeToDisruptHost(ctx, host, nil, &waitSnap)
		require.ErrorIs(t, err, common.ErrCRUDDeferred)
	})
}

func TestIsHostHealthyForReconcile(t *testing.T) {
	ctx := context.Background()
	w := &worker{}

	t.Run("nil host", func(t *testing.T) {
		require.False(t, w.isHostHealthyForReconcile(ctx, nil))
	})

	t.Run("stopped counts as healthy for ordering", func(t *testing.T) {
		cr := chkWithHosts(1)
		cr.Spec.Stop = types.NewStringBool(true)
		host := hostOnCR(cr)
		require.True(t, w.isHostHealthyForReconcile(ctx, host))
	})

	t.Run("ready STS counts as healthy", func(t *testing.T) {
		host := hostOnCR(chkWithHosts(1))
		host.Runtime.CurStatefulSet = &apps.StatefulSet{}
		host.Runtime.CurStatefulSet.Status.ReadyReplicas = 1
		require.True(t, w.isHostHealthyForReconcile(ctx, host))
	})

	t.Run("not ready STS is recovery", func(t *testing.T) {
		host := hostOnCR(chkWithHosts(1))
		host.Runtime.CurStatefulSet = &apps.StatefulSet{}
		require.False(t, w.isHostHealthyForReconcile(ctx, host))
	})
}

func TestShardHostsRecoveryFirst(t *testing.T) {
	cr := chkWithHosts(2)
	shard := cr.Spec.Configuration.Clusters[0].Layout.Shards[0]
	h0 := shard.Hosts[0]
	h1 := shard.Hosts[1]
	h0.SetCR(cr)
	h1.SetCR(cr)

	h0.Runtime.CurStatefulSet = &apps.StatefulSet{}
	h0.Runtime.CurStatefulSet.Status.ReadyReplicas = 1
	h1.Runtime.CurStatefulSet = &apps.StatefulSet{}

	healthy := func(host *api.Host) bool {
		return host.Runtime.CurStatefulSet != nil && host.Runtime.CurStatefulSet.Status.ReadyReplicas > 0
	}
	ordered := shardHostsRecoveryFirst(shard, healthy)
	require.Len(t, ordered, 2)
	require.Same(t, h1, ordered[0], "not-ready host should reconcile first")
	require.Same(t, h0, ordered[1], "ready host should reconcile second")
}

func TestMembershipSettleDelay(t *testing.T) {
	w := &worker{}

	t.Run("same size does not wait", func(t *testing.T) {
		cr := chkWithHosts(3)
		cr.SetAncestor(chkWithHosts(3))
		if got := w.membershipSettleDelay(cr); got != 0 {
			t.Fatalf("membershipSettleDelay() = %s, want 0", got)
		}
	})

	t.Run("upscale waits for raft membership", func(t *testing.T) {
		cr := chkWithHosts(3)
		cr.SetAncestor(chkWithHosts(1))
		if got := w.membershipSettleDelay(cr); got != 30*time.Second {
			t.Fatalf("membershipSettleDelay() = %s, want 30s", got)
		}
	})

	t.Run("downscale always waits 120s", func(t *testing.T) {
		cr := chkWithHosts(1)
		cr.SetAncestor(chkWithHosts(3))
		if got := w.membershipSettleDelay(cr); got != 120*time.Second {
			t.Fatalf("membershipSettleDelay() = %s, want 120s", got)
		}
	})
}

func TestPrepareStsReconcileOptsWaitSection(t *testing.T) {
	w := &worker{}

	t.Run("bootstrap skips Ready", func(t *testing.T) {
		host := hostOnCR(chkWithHosts(3))
		opts := w.prepareStsReconcileOptsWaitSection(host, nil, false)
		if !opts.WaitUntilStarted() || opts.WaitUntilReady() {
			t.Fatal("bootstrap should wait Started only")
		}
	})

	t.Run("rolling waits Ready", func(t *testing.T) {
		host := hostOnCR(chkWithHosts(3))
		opts := w.prepareStsReconcileOptsWaitSection(host, nil, true)
		if !opts.WaitUntilReady() {
			t.Fatal("rolling should wait Ready")
		}
	})

	t.Run("rolling can opt out of Ready probe", func(t *testing.T) {
		host := hostOnCR(chkWithHosts(3))
		host.GetCluster().GetReconcile().Host.Wait.Probes.Readiness = types.NewStringBool(false)
		opts := w.prepareStsReconcileOptsWaitSection(host, statefulset.NewReconcileStatefulSetOptions(), true)
		if opts.WaitUntilReady() {
			t.Fatal("readiness=false should skip Ready wait")
		}
	})

	t.Run("single-host post-restart still waits Ready", func(t *testing.T) {
		w.countReadyEnsembleMembersFn = func(context.Context, api.ICustomResource) int { return 0 }
		host := hostOnCR(chkWithHosts(1))
		snap := w.snapshotHostEnsemble(context.Background(), host)
		if !snap.rolling {
			t.Fatal("single host should be rolling")
		}
		opts := w.prepareStsReconcileOptsWaitSection(host, nil, snap.rolling)
		if !opts.WaitUntilReady() {
			t.Fatal("rolling snapshot must drive Ready wait after force-restart")
		}
	})
}

func chkWithHosts(n int) *apiChk.ClickHouseKeeperInstallation {
	cr := &apiChk.ClickHouseKeeperInstallation{}
	cr.EnsureRuntime()
	cluster := &apiChk.Cluster{Name: "c"}
	cluster.Layout = apiChk.NewChkClusterLayout()
	shard := &apiChk.ChkShard{Name: "s"}
	for i := 0; i < n; i++ {
		h := &api.Host{Name: "h"}
		h.Runtime.Address.ClusterName = cluster.Name
		h.Runtime.Address.ShardName = shard.Name
		h.Runtime.Address.HostName = "h"
		shard.Hosts = append(shard.Hosts, h)
	}
	cluster.Layout.Shards = []*apiChk.ChkShard{shard}
	cluster.Runtime.CHK = cr
	cluster.Reconcile = (&api.ClusterReconcile{}).Ensure()
	cluster.Reconcile.Host.Wait.Probes = &api.ReconcileHostWaitProbes{}
	cr.Spec.Configuration = &apiChk.Configuration{
		Clusters: []*apiChk.Cluster{cluster},
	}
	return cr
}

func hostOnCR(cr *apiChk.ClickHouseKeeperInstallation) *api.Host {
	cluster := cr.Spec.Configuration.Clusters[0]
	host := cluster.Layout.Shards[0].Hosts[0]
	host.SetCR(cr)
	host.Runtime.Address.ClusterName = cluster.Name
	host.Runtime.Address.ShardName = cluster.Layout.Shards[0].Name
	return host
}
