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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
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
		snap, err := w.snapshotHostEnsemble(ctx, host)
		require.NoError(t, err)
		require.True(t, snap.rolling)
		require.Equal(t, 1, snap.members)
		require.Equal(t, 0, snap.readyCount)
	})

	t.Run("multi-host without live quorum is bootstrap", func(t *testing.T) {
		w := &worker{
			countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 0 },
		}
		host := hostOnCR(chkWithHosts(3))
		snap, err := w.snapshotHostEnsemble(ctx, host)
		require.NoError(t, err)
		require.False(t, snap.rolling)
		require.Equal(t, 3, snap.members)
		require.Equal(t, 0, snap.readyCount)
	})

	t.Run("multi-host below quorum is bootstrap", func(t *testing.T) {
		w := &worker{
			countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 1 },
		}
		host := hostOnCR(chkWithHosts(3))
		snap, err := w.snapshotHostEnsemble(ctx, host)
		require.NoError(t, err)
		require.False(t, snap.rolling)
	})

	t.Run("multi-host at quorum is rolling", func(t *testing.T) {
		w := &worker{
			countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 2 },
		}
		host := hostOnCR(chkWithHosts(3))
		snap, err := w.snapshotHostEnsemble(ctx, host)
		require.NoError(t, err)
		require.True(t, snap.rolling)
		require.Equal(t, 2, snap.readyCount)
		require.Equal(t, 3, snap.members,
			"pin members: at n<=1 rolling is true unconditionally and this asserts nothing")
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
		// Give it a Ready StatefulSet so the trailing hostContributesReady term cannot be what
		// returns false. Without this the host short-circuits before the exemption is reached
		// and the subtest passes with ObjectStatusModified too - i.e. it never tested anything.
		newHost.Runtime.CurStatefulSet = &apps.StatefulSet{}
		newHost.Runtime.CurStatefulSet.Status.ReadyReplicas = 1
		require.True(t, hostContributesReady(newHost))
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
			quorumDisruptWaitOverride:   20 * time.Millisecond,
		}
		waitSnap := snap
		start := time.Now()
		err := w.ensureQuorumSafeToDisruptHost(ctx, host, nil, &waitSnap)
		require.ErrorIs(t, err, common.ErrCRUDDeferred)
		// Pin that it actually waited. With a zero budget the loop body never runs and this
		// still returns ErrCRUDDeferred in microseconds - indistinguishable from the real thing.
		require.GreaterOrEqual(t, time.Since(start), w.quorumDisruptPollInterval(),
			"must defer only after polling for headroom, not immediately")
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
		// Back the worker with a fake that would answer Ready, so the cached not-Ready object is
		// what drives the verdict. With a nil controller the no-STS fallback returns false too,
		// making "not Ready" and "no StatefulSet at all" indistinguishable.
		ready := raftWorkerWithSTS(newRaftFakeSTS().setReady(host, 1))
		require.False(t, ready.isHostHealthyForReconcile(ctx, host))
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

	// chkWithAncestorHosts, not chkWithHosts: with no ancestor this would pass because
	// joinedEnsemble is false, leaving the `rolling` term itself unpinned.
	t.Run("bootstrap skips Ready", func(t *testing.T) {
		host := hostOnCR(chkWithAncestorHosts(3))
		opts := w.prepareStsReconcileOptsWaitSection(host, nil, false)
		if !opts.WaitUntilStarted() || opts.WaitUntilReady() {
			t.Fatal("bootstrap should wait Started only")
		}
	})

	t.Run("rolling waits Ready", func(t *testing.T) {
		host := hostOnCR(chkWithAncestorHosts(3))
		opts := w.prepareStsReconcileOptsWaitSection(host, nil, true)
		if !opts.WaitUntilReady() {
			t.Fatal("rolling should wait Ready")
		}
	})

	// The scale-up case the guard exists for: a LIVE 3-member ensemble growing to 5. The two
	// added hosts are absent from the ancestor and have no StatefulSet, so they must not carry
	// the Ready wait, while the existing members still must. This is only representable because
	// fixture hosts have distinct names; with every host named "h" the ancestor lookup matched
	// any host and both assertions below collapsed to the same value.
	t.Run("upscale of a live ensemble waits Ready only for existing members", func(t *testing.T) {
		cr := chkWithHosts(5)
		cr.SetAncestor(chkWithHosts(3))

		existing := hostAtOnCR(cr, 0)
		existing.Runtime.CurStatefulSet = &apps.StatefulSet{}
		existing.Runtime.CurStatefulSet.Status.ReadyReplicas = 1
		added := hostAtOnCR(cr, 4)

		require.True(t, existing.HasAncestor(), "host 0 must resolve to an ancestor entry")
		require.False(t, added.HasAncestor(), "host 4 is absent from the 3-host ancestor")

		require.True(t, w.prepareStsReconcileOptsWaitSection(existing, nil, true).WaitUntilReady(),
			"an existing member of the live ensemble must still wait Ready")

		addedOpts := w.prepareStsReconcileOptsWaitSection(added, nil, true)
		require.False(t, addedOpts.WaitUntilReady(),
			"a host this pass is adding cannot reach /ready before the existing members roll")
		require.True(t, addedOpts.WaitUntilStarted(),
			"it must still wait to START, or the loop advances before the Keeper has booted")
	})

	// A live ensemble is not always an ancestor-bearing one. host.HasAncestor() resolves through
	// .status.normalizedCompleted, stamped only by a fully successful pass, so a CHK whose first
	// reconcile deferred, errored, or was killed mid-pass has running Keepers and no ancestor.
	// Dropping the Ready wait there would unserialize a genuine rolling update - and on a
	// 2-member CHK the quorum gate has no headroom to engage, so this wait is the only thing
	// keeping both Keepers from rolling at once.
	t.Run("live ensemble with no completed reconcile still waits Ready", func(t *testing.T) {
		cr := chkWithHosts(3) // no ancestor CR: no pass has ever completed
		host := hostOnCR(cr)
		host.Runtime.CurStatefulSet = &apps.StatefulSet{}
		host.Runtime.CurStatefulSet.Status.ReadyReplicas = 1

		require.False(t, host.HasAncestor(),
			"fixture must actually model the no-completed-reconcile state")
		require.True(t, w.prepareStsReconcileOptsWaitSection(host, nil, true).WaitUntilReady(),
			"a Ready host is a live member whatever .status says")
	})

	// A host added by a scale-up cannot reach /ready until the existing members roll onto the
	// config that admits it, and recovery-first ordering reconciles it BEFORE they do. Waiting
	// there can only expire into ErrCRUDAbort, wedging every upscale from a live ensemble.
	t.Run("rolling does not wait Ready for a host not yet in the ensemble", func(t *testing.T) {
		host := hostOnCR(chkWithHosts(3)) // no ancestor CR: this host is brand new
		opts := w.prepareStsReconcileOptsWaitSection(host, nil, true)
		if opts.WaitUntilReady() {
			t.Fatal("a host with no ancestor must not carry the Ready wait")
		}
	})

	t.Run("rolling can opt out of Ready probe", func(t *testing.T) {
		host := hostOnCR(chkWithAncestorHosts(3))
		require.True(t, host.HasAncestor(),
			"without an ancestor the Ready wait is skipped anyway and readiness=false proves nothing")
		host.GetCluster().GetReconcile().Host.Wait.Probes.Readiness = types.NewStringBool(false)
		opts := w.prepareStsReconcileOptsWaitSection(host, statefulset.NewReconcileStatefulSetOptions(), true)
		if opts.WaitUntilReady() {
			t.Fatal("readiness=false should skip Ready wait")
		}
	})

	t.Run("single-host post-restart still waits Ready", func(t *testing.T) {
		w.countReadyEnsembleMembersFn = func(context.Context, api.ICustomResource) int { return 0 }
		host := hostOnCR(chkWithAncestorHosts(1))
		snap, err := w.snapshotHostEnsemble(context.Background(), host)
		require.NoError(t, err)
		if !snap.rolling {
			t.Fatal("single host should be rolling")
		}
		opts := w.prepareStsReconcileOptsWaitSection(host, nil, snap.rolling)
		if !opts.WaitUntilReady() {
			t.Fatal("rolling snapshot must drive Ready wait after force-restart")
		}
	})
}

// TestRefreshQuorumSnapshotCountsFreezesRolling pins the freeze on a MULTI-host ensemble,
// where re-deriving rolling actually changes the answer (the n<=1 short-circuit hides it).
//
// A force-restart drops the host STS to ReadyReplicas=0, so live Ready falls below quorum
// mid-pass. rolling must keep the value it had before the disruption, otherwise the pass
// silently reclassifies itself as bootstrap and stops waiting for Keeper to become Ready -
// the host is left un-rejoined while the loop moves on to its peers.
func TestRefreshQuorumSnapshotCountsFreezesRolling(t *testing.T) {
	ctx := context.Background()

	var ready atomic.Int32
	ready.Store(3)
	w := &worker{
		countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int {
			return int(ready.Load())
		},
	}
	host := hostOnCR(chkWithAncestorHosts(3)) // a force-restarted host is an existing member

	// Snapshot taken while the ensemble holds quorum -> rolling pass.
	snap, err := w.snapshotHostEnsemble(ctx, host)
	require.NoError(t, err)
	require.True(t, snap.rolling, "healthy 3-member ensemble must classify as rolling")
	require.Equal(t, 3, snap.members)
	require.Equal(t, 3, snap.readyCount)

	// Force-restart: every Ready count drops to 0 while the pass is in flight.
	ready.Store(0)

	// Guard against a vacuous assertion: at this live count a FRESH snapshot is bootstrap,
	// so "still rolling" below can only come from the freeze, not from the input.
	freshSnap, err := w.snapshotHostEnsemble(ctx, host)
	require.NoError(t, err)
	require.False(t, freshSnap.rolling,
		"fresh snapshot at ready=0 must be bootstrap - otherwise the freeze assertion proves nothing")

	w.refreshQuorumSnapshotCounts(ctx, host, &snap)

	require.True(t, snap.rolling, "rolling must stay frozen at its pre-disrupt value")
	require.Equal(t, 0, snap.readyCount, "readyCount must track live peer recovery")
	require.Equal(t, 3, snap.members, "membership must not change mid-pass")

	// Downstream consequence: the frozen rolling flag still drives the Ready wait.
	opts := w.prepareStsReconcileOptsWaitSection(host, nil, snap.rolling)
	require.True(t, opts.WaitUntilReady(),
		"post-force-restart pass must still wait for Keeper to become Ready")
	require.False(t, opts.WaitUntilStarted(),
		"rolling pass must not degrade to the bootstrap Started-only wait")
}

// TestRefreshQuorumSnapshotCountsNoOpOnBootstrap pins the early return: a bootstrap pass
// never upgrades itself to rolling, no matter how many peers come up mid-pass.
func TestRefreshQuorumSnapshotCountsNoOpOnBootstrap(t *testing.T) {
	w := &worker{
		countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 3 },
	}
	host := hostOnCR(chkWithHosts(3))
	snap := hostEnsembleSnapshot{rolling: false, members: 3, readyCount: 0}

	w.refreshQuorumSnapshotCounts(context.Background(), host, &snap)

	require.False(t, snap.rolling, "bootstrap pass must not flip to rolling mid-flight")
	require.Equal(t, 0, snap.readyCount, "bootstrap snapshot counts are not refreshed")
}

func chkWithHosts(n int) *apiChk.ClickHouseKeeperInstallation {
	cr := &apiChk.ClickHouseKeeperInstallation{}
	cr.EnsureRuntime()
	cluster := &apiChk.Cluster{Name: "c"}
	cluster.Layout = apiChk.NewChkClusterLayout()
	shard := &apiChk.ChkShard{Name: "s"}
	for i := 0; i < n; i++ {
		// Distinct names per host. Host.GetAncestor() resolves via FindHost on the ancestor CR by
		// HostName, so naming every host the same made EVERY host resolve to an ancestor entry -
		// which silently made "a host this pass is adding" unrepresentable in any fixture.
		name := fmt.Sprintf("h%d", i)
		h := &api.Host{Name: name}
		h.Runtime.Address.ClusterName = cluster.Name
		h.Runtime.Address.ShardName = shard.Name
		h.Runtime.Address.HostName = name
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

// chkWithAncestorHosts builds a CR whose hosts already exist in the previous generation, so
// host.HasAncestor() is true - the shape of every host that is being ROLLED rather than added.
// Hosts added by a scale-up have no ancestor, which is what suppresses their Ready wait.
func chkWithAncestorHosts(n int) *apiChk.ClickHouseKeeperInstallation {
	cr := chkWithHosts(n)
	cr.SetAncestor(chkWithHosts(n))
	return cr
}

func hostOnCR(cr *apiChk.ClickHouseKeeperInstallation) *api.Host {
	return hostAtOnCR(cr, 0)
}

func hostAtOnCR(cr *apiChk.ClickHouseKeeperInstallation, i int) *api.Host {
	cluster := cr.Spec.Configuration.Clusters[0]
	host := cluster.Layout.Shards[0].Hosts[i]
	host.SetCR(cr)
	host.Runtime.Address.ClusterName = cluster.Name
	host.Runtime.Address.ShardName = cluster.Layout.Shards[0].Name
	return host
}

// raftFakeSTS answers the live StatefulSet lookups countReadyEnsembleMembers makes.
// Ready replicas are keyed by host pointer so a fixture can hold peers at different
// readiness and flip one mid-wait, the way a recovering Keeper pod does.
// The quorum gate only ever reads: Create/Update count the call and return an error so a
// test can assert the gate deferred instead of mutating, and Delete panics outright.
type raftFakeSTS struct {
	mu    sync.Mutex
	ready map[*api.Host]int32
	err   error
	// mirrorDesired makes a Get for a *apps.StatefulSet hand back that very object, i.e. the
	// cluster already holds exactly what the creator just built. That is the only way to make
	// getStatefulSetStatus compare two equal object-version labels and answer ObjectStatusSame;
	// a bare &apps.StatefulSet{} has no version label and is classified Unknown.
	mirrorDesired bool
	getCalls      int
	mutations     int
}

func newRaftFakeSTS() *raftFakeSTS {
	return &raftFakeSTS{ready: map[*api.Host]int32{}}
}

func (f *raftFakeSTS) setReady(host *api.Host, ready int32) *raftFakeSTS {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ready[host] = ready
	return f
}

// mutationCount reports how many times the StatefulSet was created or updated - i.e. how
// many times the host was actually disrupted.
func (f *raftFakeSTS) mutationCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mutations
}

func (f *raftFakeSTS) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getCalls
}

func (f *raftFakeSTS) Get(ctx context.Context, params ...any) (*apps.StatefulSet, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	if f.err != nil {
		// Mirror client-go: a typed Get hands back a non-nil zero object alongside the error.
		return &apps.StatefulSet{}, f.err
	}
	sts := &apps.StatefulSet{}
	if len(params) > 0 {
		if desired, ok := params[0].(*apps.StatefulSet); ok && f.mirrorDesired {
			return desired.DeepCopy(), nil
		}
		if host, ok := params[0].(*api.Host); ok {
			// A host with no recorded StatefulSet has none in the cluster either. Real
			// client-go answers NotFound there; returning a zero-Ready object instead would
			// make a fresh install look like an existing-but-unready one, which is how a
			// whole class of bootstrap bugs stays invisible to this suite.
			ready, known := f.ready[host]
			if !known {
				return &apps.StatefulSet{}, apiErrors.NewNotFound(
					schema.GroupResource{Group: "apps", Resource: "statefulsets"}, host.GetName())
			}
			sts.Status.ReadyReplicas = ready
		}
	}
	return sts, nil
}

func (f *raftFakeSTS) Create(ctx context.Context, sts *apps.StatefulSet) (*apps.StatefulSet, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mutations++
	return nil, errors.New("raftFakeSTS: Create must not be reached")
}

func (f *raftFakeSTS) Update(ctx context.Context, sts *apps.StatefulSet) (*apps.StatefulSet, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mutations++
	return nil, errors.New("raftFakeSTS: Update must not be reached")
}

func (f *raftFakeSTS) Delete(ctx context.Context, namespace, name string) error {
	panic("quorum gate must not delete a StatefulSet")
}

func (f *raftFakeSTS) List(ctx context.Context, namespace string, opts meta.ListOptions) ([]apps.StatefulSet, error) {
	return nil, nil
}

// raftFakeKube implements the handful of accessors the quorum gate reaches: STS() answers from
// the fake, Pod()/Storage() return nil and CR() a stub. IKube is embedded as a nil interface, so
// any accessor NOT defined below panics - a guard that the gate touches nothing else in kube.
type raftFakeKube struct {
	interfaces.IKube
	sts interfaces.IKubeSTS
}

func (k *raftFakeKube) STS() interfaces.IKubeSTS { return k.sts }

// Pod and Storage are wired into the StatefulSet reconciler by newTask but never called on
// the paths under test; nil is enough to let construction succeed.
func (k *raftFakeKube) Pod() interfaces.IKubePod            { return nil }
func (k *raftFakeKube) Storage() interfaces.IKubeStoragePVC { return nil }
func (k *raftFakeKube) CR() interfaces.IKubeCR              { return raftFakeKubeCR{} }

// raftFakeKubeCR stands in for the CR client. NewReconciler dereferences kube.CR() while being
// constructed in newTask, and on ObjectStatusSame ReconcileStatefulSet pushes status through it -
// those are the only two uses. Every other IKubeCR method panics through the nil embed, so an
// unexpected call is loud rather than silent.
type raftFakeKubeCR struct{ interfaces.IKubeCR }

func (raftFakeKubeCR) StatusUpdate(ctx context.Context, cr api.ICustomResource, opts types.UpdateStatusOptions) error {
	return nil
}

func raftWorkerWithSTS(sts interfaces.IKubeSTS) *worker {
	return &worker{c: &Controller{kube: &raftFakeKube{sts: sts}}}
}

// newTask builds a config-files generator, which reads the global operator config.
// Same shape as pkg/model/chk/tags/labeler/list_test.go.
func init() { chop.New(nil, nil, "") }

// TestUpscaleFromSingleMemberClassifiesAsBootstrap pins the growth direction of quorumSizingEnsemble
// against the Ready-wait wedge.
//
// The gate sizes on the live (ancestor) ensemble, but a 1-member ancestor has no quorum to
// protect - and sizing on it would make a 1->3 a 1-member ensemble, which members<=1 classifies
// rolling unconditionally. Rolling turns on SetWaitUntilReady for the two new hosts, which are
// reconciled first, yet a new Keeper only reports /ready once its 3-server config has Raft
// quorum and the second new peer does not exist yet. The wait burns its budget,
// chkStatefulSetFallback converts that to ErrCRUDAbort, and a scale-up that used to work wedges.
// Falling back to the desired set keeps ready(1) < quorum(3)=2, so the pass is bootstrap.
func TestUpscaleFromSingleMemberClassifiesAsBootstrap(t *testing.T) {
	ctx := context.Background()

	cr := chkWithHosts(3)
	cr.SetAncestor(chkWithHosts(1))

	fake := newRaftFakeSTS()
	hosts := cacheAllHostsAt(cr, 1, 0, 0)
	fake.setReady(hosts[0], 1) // only the pre-existing member is up; the two new peers have no STS

	w := raftWorkerWithSTS(fake)
	snap, err := w.snapshotHostEnsemble(ctx, hostOnCR(cr))
	require.NoError(t, err)

	// Behaviour first, sizing second: the classification is the invariant, members is the mechanism.
	require.False(t, snap.rolling,
		"1->3 must classify bootstrap: a rolling pass waits for a Ready that needs a peer it has not created yet")
	require.Equal(t, 3, snap.members, "a 1-member ancestor cannot lose a member, so sizing falls back to the desired set")
}

// TestDegradedUpscaleKeepsGateArmed pins the growth direction against the unsafe half.
//
// Sizing growth on the desired set counts Ready against a membership live Raft has not adopted:
// a 3->5 with one member already down tallies 2 Ready against quorum(5)=3, so rolling goes false
// and the pass is classified bootstrap - which disables this gate AND the Ready wait, leaving the
// two survivors of a 3-member ensemble free to be restarted back to back. That is precisely the
// unguarded fan-out this gate prevents. Sized on the live ensemble it is 2 Ready of 3, quorum 2, and
// disrupting either survivor is refused.
func TestDegradedUpscaleKeepsGateArmed(t *testing.T) {
	ctx := context.Background()

	ancestor := chkWithHosts(3)
	cr := chkWithHosts(5)
	cr.SetAncestor(ancestor)

	fake := newRaftFakeSTS()
	// The ancestor's hosts are separately normalized objects denoting the SAME running pods, and
	// raftFakeSTS keys Ready by host pointer, so they must be registered in their own right.
	// One of the three is already down; a registered zero models "pod exists but is not Ready",
	// which is the degraded case, whereas leaving it unregistered would model a deleted STS.
	ancestorHosts := cacheAllHostsAt(ancestor, 1, 1, 0)
	fake.setReady(ancestorHosts[0], 1)
	fake.setReady(ancestorHosts[1], 1)
	fake.setReady(ancestorHosts[2], 0)

	// The two peers this scale-up adds have no StatefulSet yet.
	hosts := cacheAllHostsAt(cr, 1, 1, 0, 0, 0)
	fake.setReady(hosts[0], 1)
	fake.setReady(hosts[1], 1)
	fake.setReady(hosts[2], 0)

	host := hostOnCR(cr) // hosts[0]: a Ready survivor, so disrupting it costs live quorum
	host.GetReconcileAttributes().SetStatus(types.ObjectStatusModified)

	w := raftWorkerWithSTS(fake)
	snap, err := w.snapshotHostEnsemble(ctx, host)
	require.NoError(t, err)

	// Behaviour first, sizing second: the refusal is the invariant, members is the mechanism.
	require.True(t, snap.rolling,
		"a degraded live ensemble must stay rolling: bootstrap turns off both the gate and the Ready wait")
	require.True(t, w.hostDisruptionWouldBreakQuorum(ctx, host, nil, snap),
		"disrupting the second of two Ready members of a 3-member ensemble must be refused")
	require.Equal(t, 3, snap.members, "live Raft still runs the 3-server config it started with")
	require.Equal(t, 2, snap.readyCount)
}

// TestHealthyUpscaleProceeds pins liveness in the same direction: gating growth on the live
// ensemble must not stall a scale-up whose existing members are all Ready. The new peers are
// exempt (ObjectStatusRequested) and reconciled first; each existing member then rolls with
// quorum to spare.
func TestHealthyUpscaleProceeds(t *testing.T) {
	ctx := context.Background()

	ancestor := chkWithHosts(3)
	cr := chkWithHosts(5)
	cr.SetAncestor(ancestor)

	fake := newRaftFakeSTS()
	for _, h := range cacheAllHostsAt(ancestor, 1, 1, 1) {
		fake.setReady(h, 1)
	}
	hosts := cacheAllHostsAt(cr, 1, 1, 1, 0, 0)
	for _, h := range hosts[:3] {
		fake.setReady(h, 1)
	}

	host := hostOnCR(cr)
	host.GetReconcileAttributes().SetStatus(types.ObjectStatusModified)

	w := raftWorkerWithSTS(fake)
	snap, err := w.snapshotHostEnsemble(ctx, host)
	require.NoError(t, err)

	// Pin that the host actually contributes. Without this the assertion below passes just as
	// well when the host reads NotFound: CurStatefulSet goes nil, hostContributesReady is false,
	// and hostDisruptionWouldBreakQuorum short-circuits to false for the wrong reason.
	require.True(t, hostContributesReady(host),
		"the reconciled host must be a Ready contributor for the headroom assertion to mean anything")
	require.False(t, w.hostDisruptionWouldBreakQuorum(ctx, host, nil, snap),
		"rolling one of three Ready members leaves 2 >= quorum(3)=2 and must proceed")
	require.Equal(t, 3, snap.members)
	require.Equal(t, 3, snap.readyCount)
}

// TestCountReadyEnsembleMembersClearsVanishedStatefulSet pins the "gone means gone" assignment.
// Leaving a deleted peer's cached StatefulSet in place keeps reporting it as Ready to
// hostContributesReady and isHostHealthyForReconcile.
func TestCountReadyEnsembleMembersClearsVanishedStatefulSet(t *testing.T) {
	ctx := context.Background()
	cr := chkWithHosts(3)
	hosts := cacheAllHostsAt(cr, 1, 1, 1) // all cached as Ready

	fake := newRaftFakeSTS()
	fake.setReady(hosts[0], 1)
	fake.setReady(hosts[1], 1)
	// hosts[2] is unregistered: its StatefulSet has been deleted out from under us.

	ready, err := raftWorkerWithSTS(fake).countReadyEnsembleMembers(ctx, cr)
	require.NoError(t, err)
	require.Equal(t, 2, ready)
	require.Nil(t, hosts[2].Runtime.CurStatefulSet,
		"a peer whose StatefulSet is gone must not keep reading as Ready from cache")
	require.False(t, hostContributesReady(hosts[2]))
}

// TestSnapshotRefreshesReconciledHostOnDownscale pins the first gate evaluation.
//
// On a downscale the gated set is the ancestor, whose hosts are separately normalized objects.
// The Ready tally therefore walks different *api.Host pointers than the one the reconciled host
// is, so without an explicit refresh that host keeps whatever fillCurSTS cached at reconcile
// start. A host that recovered since then reads as not-contributing, hostDisruptionWouldBreakQuorum
// returns false, and the gate is skipped entirely - a disrupt that does break quorum proceeds.
// This is the FIRST evaluation and for most passes the only one, so fixing the wait loop alone
// left the hole open.
func TestSnapshotRefreshesReconciledHostOnDownscale(t *testing.T) {
	ctx := context.Background()

	ancestor := chkWithHosts(3)
	cr := chkWithHosts(1)
	cr.SetAncestor(ancestor)

	// Two of the three ancestor members Ready: disrupting one more leaves 1 < quorum(3)=2.
	fake := newRaftFakeSTS()
	ancestorHosts := cacheAllHostsAt(ancestor, 1, 1, 0)
	fake.setReady(ancestorHosts[0], 1)
	fake.setReady(ancestorHosts[1], 1)
	fake.setReady(ancestorHosts[2], 0)
	// The reconciled host is live-Ready, but its cached StatefulSet says otherwise - the state
	// fillCurSTS would have left behind if the host was restarting when the pass began.
	host := hostOnCR(cr)
	host.Runtime.CurStatefulSet = &apps.StatefulSet{}
	host.Runtime.CurStatefulSet.Status.ReadyReplicas = 0
	fake.setReady(host, 1)

	w := raftWorkerWithSTS(fake)
	snap, err := w.snapshotHostEnsemble(ctx, host)
	require.NoError(t, err)

	require.True(t, hostContributesReady(host),
		"the reconciled host must be refreshed by the snapshot, not left at its cached value")
	require.True(t, w.hostDisruptionWouldBreakQuorum(ctx, host, nil, snap),
		"a Ready host whose removal drops the ensemble below quorum must be gated")
}

// TestSnapshotHostEnsembleTreatsMissingStatefulSetAsNotReady guards CHK bootstrap.
//
// Surfacing StatefulSet Get errors is right for genuine API failures, but NotFound is not one:
// it is the normal state of every host on a fresh CHK and of every host added by a scale-up.
// Treating it as an error fails the very reconcile meant to create those StatefulSets, so a new
// CHK never bootstraps and a scale-up never completes.
func TestSnapshotHostEnsembleTreatsMissingStatefulSetAsNotReady(t *testing.T) {
	ctx := context.Background()

	// Fresh install: three hosts, none has a StatefulSet yet.
	cr := chkWithHosts(3)
	fake := newRaftFakeSTS() // records no hosts, so every Get answers NotFound
	w := raftWorkerWithSTS(fake)

	snap, err := w.snapshotHostEnsemble(ctx, hostOnCR(cr))

	require.NoError(t, err, "a missing StatefulSet must not fail the reconcile that creates it")
	require.Equal(t, 3, snap.members)
	require.Zero(t, snap.readyCount, "hosts with no StatefulSet are not Ready")

	t.Run("a genuine API failure is still surfaced", func(t *testing.T) {
		boom := errors.New("apiserver unavailable")
		failing := newRaftFakeSTS()
		failing.err = boom
		_, err := raftWorkerWithSTS(failing).snapshotHostEnsemble(ctx, hostOnCR(chkWithHosts(3)))
		require.ErrorIs(t, err, boom)
	})
}

// TestReconcileHostStatefulSetConsultsQuorumGate pins that the gate is actually WIRED, not
// merely correct in isolation.
//
// Every other test here drives ensureQuorumSafeToDisruptHost directly, so deleting its call
// from reconcileHostStatefulSet leaves the whole suite green while the operator happily rolls
// an ensemble below Raft majority - the exact fan-out this gate prevents. This test goes through
// reconcileHostStatefulSet and asserts two things: the deferral reaches the caller, and the
// StatefulSet was never touched on the way out.
func TestReconcileHostStatefulSetConsultsQuorumGate(t *testing.T) {
	ctx := context.Background()

	// 3-member ensemble with one member already down: disrupting a Ready host would leave
	// 1 < quorum(3)=2.
	cr := chkWithHosts(3)
	hosts := cacheAllHostsAt(cr, 1, 1, 0)
	for _, h := range hosts {
		// cacheAllHostsAt stamps StatefulSets but does not link hosts back to the CR;
		// reconcileHostStatefulSet reads through host.GetCR() from its first line.
		h.SetCR(cr)
		h.Runtime.Address.ClusterName = cr.Spec.Configuration.Clusters[0].Name
		h.Runtime.Address.ShardName = cr.Spec.Configuration.Clusters[0].Layout.Shards[0].Name
	}
	fake := newRaftFakeSTS()
	fake.setReady(hosts[0], 1)
	fake.setReady(hosts[1], 1)
	fake.setReady(hosts[2], 0)

	w := &worker{
		a: a.NewAnnouncer(nil, nil),
		c: &Controller{kube: &raftFakeKube{sts: fake}},
		// Keep the wait from actually sleeping; the ensemble can never regain headroom here.
		quorumDisruptPollOverride: 2 * time.Millisecond,
		quorumDisruptWaitOverride: 10 * time.Millisecond,
	}
	w.newTask(cr, nil)

	host := hosts[0]
	snap := hostEnsembleSnapshot{rolling: true, members: 3, readyCount: 2}

	err := w.reconcileHostStatefulSet(ctx, host, nil, snap)

	require.ErrorIs(t, err, common.ErrCRUDDeferred,
		"reconcileHostStatefulSet must consult the quorum gate and propagate its deferral")
	require.Zero(t, fake.mutationCount(),
		"a deferred host must never have its StatefulSet created or updated")
}

// TestNewTaskWiresChkStatefulSetFallback pins the wiring, not the policy.
//
// chkStatefulSetFallback returning ErrCRUDAbort is worthless unless newTask actually installs
// it: with the default fallback a StatefulSet wait failure returns ErrCRUDIgnore and the walk
// carries on to the next replica, which is the fan-out this gate prevents. Asserting the constants alone
// leaves that revert green, so assert what the reconciler was built with.
func TestNewTaskWiresChkStatefulSetFallback(t *testing.T) {
	w := &worker{
		a: a.NewAnnouncer(nil, nil),
		c: &Controller{kube: &raftFakeKube{sts: newRaftFakeSTS()}},
	}

	w.newTask(chkWithHosts(1), nil)

	require.NotNil(t, w.stsReconciler)
	fb := w.stsReconciler.Fallback()
	require.IsType(t, &chkStatefulSetFallback{}, fb,
		"CHK must install its own fallback, not the default one")
	require.Equal(t, common.ErrCRUDAbort, fb.OnStatefulSetCreateFailed(context.Background(), nil),
		"the installed fallback must abort the reconcile, not ignore the failure")
}

// cacheAllHostsAt stamps every host's CurStatefulSet the way fillCurSTS does at reconcile
// start - one frozen read per host, taken before anything was disrupted.
func cacheAllHostsAt(cr *apiChk.ClickHouseKeeperInstallation, ready ...int32) []*api.Host {
	var hosts []*api.Host
	cr.WalkHosts(func(host *api.Host) error {
		sts := &apps.StatefulSet{}
		if len(hosts) < len(ready) {
			sts.Status.ReadyReplicas = ready[len(hosts)]
		}
		host.Runtime.CurStatefulSet = sts
		hosts = append(hosts, host)
		return nil
	})
	return hosts
}

// TestCountReadyEnsembleMembersRereadsPeers pins that peer readiness is read live.
//
// fillCurSTS populates Runtime.CurStatefulSet for every host at reconcile start. When
// countReadyEnsembleMembers preferred that cache, the quorum wait polled a value that
// could never change: a peer coming back Ready was invisible, so the wait burned its
// whole budget and deferred the roll even though the ensemble had recovered.
func TestCountReadyEnsembleMembersRereadsPeers(t *testing.T) {
	ctx := context.Background()

	t.Run("live Ready beats a stale not-ready cache", func(t *testing.T) {
		cr := chkWithHosts(3)
		hosts := cacheAllHostsAt(cr, 0, 0, 0)
		fake := newRaftFakeSTS()
		for _, host := range hosts {
			fake.setReady(host, 1)
		}
		w := raftWorkerWithSTS(fake)

		gotReady, err := w.countReadyEnsembleMembers(ctx, cr)
		require.NoError(t, err)
		require.Equal(t, 3, gotReady, "must count live Ready, not the frozen cache")
		require.Equal(t, 3, fake.calls(), "every peer must be re-read")
		for _, host := range hosts {
			require.EqualValues(t, 1, host.Runtime.CurStatefulSet.Status.ReadyReplicas,
				"live read must refresh the cached STS")
		}
	})

	t.Run("wait observes a peer recovering instead of burning the budget", func(t *testing.T) {
		cr := chkWithHosts(3)
		hosts := cacheAllHostsAt(cr, 1, 1, 0)
		fake := newRaftFakeSTS().setReady(hosts[0], 1).setReady(hosts[1], 1).setReady(hosts[2], 0)

		w := raftWorkerWithSTS(fake)
		w.quorumDisruptPollOverride = 5 * time.Millisecond
		w.quorumDisruptWaitOverride = 2 * time.Second

		host := hostOnCR(cr)
		host.GetReconcileAttributes().SetStatus(types.ObjectStatusModified)
		snap, err := w.snapshotHostEnsemble(ctx, host)
		require.NoError(t, err)
		require.True(t, snap.rolling)
		require.Equal(t, 2, snap.readyCount, "third peer is down at snapshot time")

		go func() {
			time.Sleep(20 * time.Millisecond)
			fake.setReady(hosts[2], 1)
		}()

		require.NoError(t, w.ensureQuorumSafeToDisruptHost(ctx, host, nil, &snap),
			"recovered peer must unblock the wait")
		require.Equal(t, 3, snap.readyCount)
	})
}

// TestCountReadyEnsembleMembersSurfacesStatefulSetGetError pins the failure direction.
//
// Swallowing the Get error undercounts Ready members, which flips the pass to bootstrap - and a
// bootstrap pass skips both the quorum gate and the Ready wait. An apiserver blip would therefore
// re-enable exactly the unguarded fan-out this gate exists to prevent. The Get already
// runs under GetWithRetry, so an error reaching here is a sustained outage: fail the pass and let
// the reconcile requeue rather than proceed on a count we know is wrong.
func TestCountReadyEnsembleMembersSurfacesStatefulSetGetError(t *testing.T) {
	ctx := context.Background()
	cr := chkWithHosts(3)
	hosts := cacheAllHostsAt(cr, 1, 1, 1)
	fake := newRaftFakeSTS()
	for _, host := range hosts {
		fake.setReady(host, 1)
	}
	wantErr := errors.New("apiserver unavailable")
	fake.err = wantErr
	w := raftWorkerWithSTS(fake)

	_, err := w.countReadyEnsembleMembers(ctx, cr)
	require.ErrorIs(t, err, wantErr, "a failed Get must be reported, not counted as not-Ready")

	host := hostOnCR(cr)
	host.GetReconcileAttributes().SetStatus(types.ObjectStatusModified)
	_, err = w.snapshotHostEnsemble(ctx, host)
	require.ErrorIs(t, err, wantErr,
		"the snapshot must fail the pass rather than classify it as bootstrap on a bad count")
}

// TestSnapshotCountsReadyOverTheSameSetAsMembers pins that members and readyCount are tallied
// over the SAME host set.
//
// Taking members from the ancestor while counting Ready over the (smaller) desired set caps ready
// below quorum(ancestor) on every downscale. That forces the pass to bootstrap, which disables the
// quorum gate AND the Ready wait - strictly worse than not widening members at all. This test uses
// the production counter deliberately: injecting countReadyEnsembleMembersFn bypasses the very
// path the bug lived in.
func TestSnapshotCountsReadyOverTheSameSetAsMembers(t *testing.T) {
	ctx := context.Background()

	// 3 -> 1 downscale with every live member still Ready.
	ancestor := chkWithHosts(3)
	cr := chkWithHosts(1)
	cr.SetAncestor(ancestor)

	fake := newRaftFakeSTS()
	for _, host := range cacheAllHostsAt(ancestor, 1, 1, 1) {
		fake.setReady(host, 1)
	}
	for _, host := range cacheAllHostsAt(cr, 1) {
		fake.setReady(host, 1)
	}
	w := raftWorkerWithSTS(fake)

	snap, err := w.snapshotHostEnsemble(ctx, hostOnCR(cr))
	require.NoError(t, err)

	require.Equal(t, 3, snap.members, "live Raft still runs the ancestor's members")
	require.Equal(t, 3, snap.readyCount,
		"Ready must be tallied over the ancestor set too, or it is structurally capped below quorum")
	require.True(t, snap.rolling,
		"a fully Ready ensemble must stay rolling: classifying it bootstrap disables the gate and the Ready wait")
}

// TestSnapshotUsesAncestorMemberCount pins ensemble size as max(desired, ancestor).
//
// Live Raft still runs the ancestor's membership until clean() purges the removed peers,
// so a 3->1 downscale that sized the ensemble on the desired count alone got members=1,
// fell through the small-ensemble bypass, and disrupted the survivor unguarded - exactly
// when 2 of 3 were still required.
func TestSnapshotUsesAncestorMemberCount(t *testing.T) {
	ctx := context.Background()

	cr := chkWithHosts(1)
	cr.SetAncestor(chkWithHosts(3))
	host := hostOnCR(cr)
	host.Runtime.CurStatefulSet = &apps.StatefulSet{}
	host.Runtime.CurStatefulSet.Status.ReadyReplicas = 1
	host.GetReconcileAttributes().SetStatus(types.ObjectStatusModified)

	w := &worker{
		countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 2 },
		quorumDisruptPollOverride:   5 * time.Millisecond,
		quorumDisruptWaitOverride:   20 * time.Millisecond,
	}

	snap, err := w.snapshotHostEnsemble(ctx, host)

	require.NoError(t, err)
	require.Equal(t, 3, snap.members, "ancestor membership still runs in live Raft")
	require.True(t, snap.rolling)
	require.True(t, ensembleHasQuorumHeadroom(snap.members), "gate must be active, not bypassed")

	require.True(t, w.hostDisruptionWouldBreakQuorum(ctx, host, nil, snap),
		"disrupting the last Ready member of a 3-member ensemble breaks quorum")
	require.ErrorIs(t, w.ensureQuorumSafeToDisruptHost(ctx, host, nil, &snap), common.ErrCRUDDeferred)

	// The subtests below inject their own Ready counts. Reusing the enclosing worker's constant
	// 2 would assert on states neither scenario can reach - a 1->3 has exactly one live member
	// and a fresh CR has none - and 2 of 3 classifies rolling, the opposite of what both pin.
	workerReadyingCountOf := func(ready int) *worker {
		return &worker{countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return ready }}
	}

	t.Run("upscale sizes on the target when the ancestor has no quorum to protect", func(t *testing.T) {
		// The raft config published before the host loop already lists every desired server,
		// so growth must size on the target. See TestUpscaleFromSingleMemberClassifiesAsBootstrap
		// and TestDegradedUpscaleKeepsGateArmed for the two failures the ancestor causes.
		up := chkWithHosts(3)
		up.SetAncestor(chkWithHosts(1))
		upSnap, err := workerReadyingCountOf(1).snapshotHostEnsemble(ctx, hostOnCR(up))
		require.NoError(t, err)
		require.Equal(t, 3, upSnap.members, "a 1-member ancestor has no quorum to protect: size on the desired set")
		require.False(t, upSnap.rolling, "1 Ready of 3 is below quorum(3)=2, so the pass is bootstrap")
	})

	t.Run("no ancestor falls back to desired count", func(t *testing.T) {
		fresh := chkWithHosts(3)
		freshCRSnap, err := workerReadyingCountOf(0).snapshotHostEnsemble(ctx, hostOnCR(fresh))
		require.NoError(t, err)
		require.Equal(t, 3, freshCRSnap.members)
		require.False(t, freshCRSnap.rolling, "a fresh install has no Ready members: bootstrap, not rolling")
	})
}

// TestReconcileHostStatefulSetProceedsOnUnchangedStatefulSet pins the ORDER of the first two
// statements of reconcileHostStatefulSet, which both doc-blocks in worker-raft-safety.go assert
// and nothing else tested: PrepareHostStatefulSetWithStatus must run BEFORE the quorum gate,
// because it is the only place ObjectStatusSame is ever assigned.
//
// Swap the two and the gate reads a still-Unknown status, so willDisrupt is unconditionally true
// and both short-circuits in hostDisruptionWouldBreakQuorum (Same, Requested) become dead code.
// A degraded ensemble then burns the disrupt budget on every host whose StatefulSet did not
// change at all and defers it - a self-inflicted stall on a cluster that needed no disruption.
//
// The fixture is that exact shape: 3 members, one already down (ready=2, quorum=2, so any real
// disruption would be refused), and a host whose live StatefulSet equals the desired one.
func TestReconcileHostStatefulSetProceedsOnUnchangedStatefulSet(t *testing.T) {
	ctx := context.Background()

	cr := chkWithHosts(3)
	hosts := cacheAllHostsAt(cr, 1, 1, 0)
	for _, h := range hosts {
		h.SetCR(cr)
		h.Runtime.Address.ClusterName = cr.Spec.Configuration.Clusters[0].Name
		h.Runtime.Address.ShardName = cr.Spec.Configuration.Clusters[0].Layout.Shards[0].Name
	}

	fake := newRaftFakeSTS()
	fake.mirrorDesired = true // every host's live StatefulSet equals the desired one -> Same
	fake.setReady(hosts[0], 1)
	fake.setReady(hosts[1], 1)
	fake.setReady(hosts[2], 0)

	w := &worker{
		a: a.NewAnnouncer(nil, nil),
		c: &Controller{kube: &raftFakeKube{sts: fake}},
		// Keep a mis-ordered gate from sleeping out its whole budget: headroom can never be
		// regained here, so the wrong answer arrives fast.
		quorumDisruptPollOverride: 2 * time.Millisecond,
		quorumDisruptWaitOverride: 10 * time.Millisecond,
	}
	w.newTask(cr, nil)

	host := hosts[0]
	err := w.reconcileHostStatefulSet(ctx, host, nil,
		hostEnsembleSnapshot{rolling: true, members: 3, readyCount: 2})

	require.NoError(t, err, "an unchanged host disrupts nothing - the gate must not defer it")
	require.True(t, host.GetReconcileAttributes().GetStatus().Is(types.ObjectStatusSame),
		"PrepareHostStatefulSetWithStatus must run before the gate, else the status it reads is Unknown")
	require.Zero(t, fake.mutationCount(),
		"a host already matching the desired StatefulSet must not be created or updated")
}

// TestQuorumWaitBudgetSharedAcrossHostsInOnePass pins that the disrupt wait is rationed per
// PASS, not per host. Per-host, a 5-node CHK with one permanently dead peer burns 5 x 2min of
// pure waiting every reconcile, and with ReconcileCHKsThreadsNumber=1 that worker reconciles
// nothing else meanwhile.
func TestQuorumWaitBudgetSharedAcrossHostsInOnePass(t *testing.T) {
	ctx := context.Background()
	cr := chkWithHosts(3)
	cacheAllHostsAt(cr, 1, 1, 0)
	h0, h1 := hostAtOnCR(cr, 0), hostAtOnCR(cr, 1)
	h0.GetReconcileAttributes().SetStatus(types.ObjectStatusModified)
	h1.GetReconcileAttributes().SetStatus(types.ObjectStatusModified)

	w := &worker{
		a:                           a.NewAnnouncer(nil, nil),
		countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 2 },
		quorumDisruptPollOverride:   5 * time.Millisecond,
		quorumDisruptWaitOverride:   200 * time.Millisecond,
	}
	snap := hostEnsembleSnapshot{rolling: true, members: 3, readyCount: 2}

	first := snap
	start := time.Now()
	require.ErrorIs(t, w.ensureQuorumSafeToDisruptHost(ctx, h0, nil, &first), common.ErrCRUDDeferred)
	require.GreaterOrEqual(t, time.Since(start), w.quorumDisruptWaitTimeout(),
		"the first gated host must still get the whole budget, so a transient blip is absorbed")

	second := snap
	spentAfterFirst := w.quorumWaitSpent
	require.ErrorIs(t, w.ensureQuorumSafeToDisruptHost(ctx, h1, nil, &second), common.ErrCRUDDeferred)
	// State rather than wall clock: a per-host budget would wait the full timeout again and
	// charge for it, so an unchanged counter is a stronger claim than a fast return - and it
	// cannot flake when the scheduler stalls.
	require.Equal(t, spentAfterFirst, w.quorumWaitSpent,
		"budget spent: the next gated host must defer immediately rather than wait again")
}

// TestQuorumWaitBudgetResetsBetweenPasses pins the reset. Without it a worker that outlives one
// pass would inherit a spent budget and stop waiting for peers that are merely slow to recover.
func TestQuorumWaitBudgetResetsBetweenPasses(t *testing.T) {
	ctx := context.Background()
	cr := chkWithHosts(3)
	cacheAllHostsAt(cr, 1, 1, 0)
	host := hostAtOnCR(cr, 0)
	host.GetReconcileAttributes().SetStatus(types.ObjectStatusModified)

	// Register the host Ready: refreshQuorumSnapshotCounts re-reads it mid-wait, and an
	// unregistered host would read NotFound, stop contributing, and let the gate proceed.
	fake := newRaftFakeSTS()
	fake.setReady(host, 1)
	w := &worker{
		a:                           a.NewAnnouncer(nil, nil),
		c:                           &Controller{kube: &raftFakeKube{sts: fake}},
		countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 2 },
		quorumDisruptPollOverride:   5 * time.Millisecond,
		quorumDisruptWaitOverride:   100 * time.Millisecond,
	}
	snap := hostEnsembleSnapshot{rolling: true, members: 3, readyCount: 2}

	w.newTask(cr, nil)
	pass1 := snap
	require.ErrorIs(t, w.ensureQuorumSafeToDisruptHost(ctx, host, nil, &pass1), common.ErrCRUDDeferred)
	require.NotZero(t, w.quorumWaitSpent)

	w.newTask(cr, nil)
	require.Zero(t, w.quorumWaitSpent, "newTask must restore the pass budget")

	pass2 := snap
	start := time.Now()
	require.ErrorIs(t, w.ensureQuorumSafeToDisruptHost(ctx, host, nil, &pass2), common.ErrCRUDDeferred)
	require.GreaterOrEqual(t, time.Since(start), w.quorumDisruptWaitTimeout(),
		"a new pass must wait again, not inherit the previous pass's spend")
}

// TestSpentBudgetStillRefusesQuorumBreakingDisrupt pins that the budget bounds the WAIT and
// never the REFUSAL. Rationing the wait must not become a way to wave a disrupt through.
func TestSpentBudgetStillRefusesQuorumBreakingDisrupt(t *testing.T) {
	ctx := context.Background()
	cr := chkWithHosts(3)
	cacheAllHostsAt(cr, 1, 1, 0)
	host := hostAtOnCR(cr, 0)
	host.GetReconcileAttributes().SetStatus(types.ObjectStatusModified)

	w := &worker{
		a:                           a.NewAnnouncer(nil, nil),
		countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 2 },
		quorumDisruptPollOverride:   5 * time.Millisecond,
		quorumDisruptWaitOverride:   50 * time.Millisecond,
	}
	w.quorumWaitSpent = w.quorumDisruptWaitTimeout() // already burned by an earlier host

	snap := hostEnsembleSnapshot{rolling: true, members: 3, readyCount: 2}
	before := w.quorumWaitSpent
	require.ErrorIs(t, w.ensureQuorumSafeToDisruptHost(ctx, host, nil, &snap), common.ErrCRUDDeferred,
		"a spent budget bounds the wait, never the refusal")
	// Assert state, not wall clock. The exhausted branch returns before waitStart, so it charges
	// nothing - an exact structural fact. A wall-clock ceiling here is just a stall detector and
	// does flake on a loaded runner.
	require.Equal(t, before, w.quorumWaitSpent,
		"the exhausted branch must return without waiting, so it charges nothing")
}

// TestMembershipSettleDelayIsActuallyWaited pins the wiring, not just the duration. The helper
// returning 30s/120s is meaningless if the caller never sleeps on it.
func TestMembershipSettleDelayIsActuallyWaited(t *testing.T) {
	w := &worker{a: a.NewAnnouncer(nil, nil)}

	t.Run("membership change waits", func(t *testing.T) {
		cr := chkWithHosts(3)
		cr.SetAncestor(chkWithHosts(2)) // upscale -> non-zero delay
		require.NotZero(t, w.membershipSettleDelay(cr))

		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()
		start := time.Now()
		require.NoError(t, w.reconcileCRAuxObjectsPreliminaryDomain(ctx, cr))
		require.GreaterOrEqual(t, time.Since(start), 30*time.Millisecond,
			"the settle delay must actually be waited on, not merely computed")
	})

	t.Run("same size does not wait", func(t *testing.T) {
		cr := chkWithAncestorHosts(3)
		require.Zero(t, w.membershipSettleDelay(cr))
		start := time.Now()
		require.NoError(t, w.reconcileCRAuxObjectsPreliminaryDomain(context.Background(), cr))
		require.Less(t, time.Since(start), time.Second,
			"a same-size reconcile must not pause at all")
	})
}

// TestReconcileShardWithHostsLoop pins the two host-loop invariants that no test reached before:
// recovery-first ordering and abort-on-error. A loop that keeps going after a hard error
// is the fan-out this gate prevents - it would recreate the next replica while the previous one never
// rejoined.
func TestReconcileShardWithHostsLoop(t *testing.T) {
	ctx := context.Background()

	newShard := func() (api.IShard, *raftFakeSTS) {
		cr := chkWithHosts(3)
		hosts := cacheAllHostsAt(cr, 1, 0, 1) // h1 is down -> it is the recovery host
		for i := range hosts {
			hostAtOnCR(cr, i) // link each host back to its CR: the health check reads through it
		}
		// No fake registrations: isHostHealthyForReconcile reads Runtime.CurStatefulSet first,
		// which cacheAllHostsAt already seeded. Registering the same readiness twice would be
		// two sources of truth that can silently disagree.
		return cr.Spec.Configuration.Clusters[0].Layout.Shards[0], newRaftFakeSTS()
	}

	t.Run("reconciles the not-ready host first", func(t *testing.T) {
		shard, fake := newShard()
		var visited []string
		w := &worker{a: a.NewAnnouncer(nil, nil), c: &Controller{kube: &raftFakeKube{sts: fake}}}
		w.reconcileHostFn = func(_ context.Context, host *api.Host) error {
			visited = append(visited, host.GetName())
			return nil
		}
		require.NoError(t, w.reconcileShardWithHosts(ctx, shard))
		require.Equal(t, []string{"h1", "h0", "h2"}, visited,
			"the down replica must be recovered before its healthy peers are touched")
	})

	t.Run("aborts the loop on a hard error", func(t *testing.T) {
		shard, fake := newShard()
		boom := errors.New("boom")
		var visited []string
		w := &worker{a: a.NewAnnouncer(nil, nil), c: &Controller{kube: &raftFakeKube{sts: fake}}}
		w.reconcileHostFn = func(_ context.Context, host *api.Host) error {
			visited = append(visited, host.GetName())
			return boom
		}
		require.ErrorIs(t, w.reconcileShardWithHosts(ctx, shard), boom)
		require.Len(t, visited, 1,
			"a hard error must stop the loop - carrying on is the fan-out this gate prevents")
	})

	t.Run("a deferral visits every host and surfaces at the end", func(t *testing.T) {
		shard, fake := newShard()
		var visited []string
		w := &worker{a: a.NewAnnouncer(nil, nil), c: &Controller{kube: &raftFakeKube{sts: fake}}}
		w.reconcileHostFn = func(_ context.Context, host *api.Host) error {
			visited = append(visited, host.GetName())
			if host.GetName() == "h1" {
				return common.ErrCRUDDeferred
			}
			return nil
		}
		require.ErrorIs(t, w.reconcileShardWithHosts(ctx, shard), common.ErrCRUDDeferred)
		require.Len(t, visited, 3, "a deferral is soft: the remaining hosts must still reconcile")
	})
}

// TestSuccessfulQuorumWaitIsCharged closes the gap that a refund would otherwise slip through:
// every budget test still passes if the success path gives its time back. Refunding is the wrong
// call - a peer that flaps in and out of quorum would then buy an unbounded number of full waits
// in a single pass, which is exactly the worker starvation the budget exists to stop.
func TestSuccessfulQuorumWaitIsCharged(t *testing.T) {
	ctx := context.Background()
	cr := chkWithHosts(3)
	cacheAllHostsAt(cr, 1, 1, 0)
	host := hostAtOnCR(cr, 0)
	host.GetReconcileAttributes().SetStatus(types.ObjectStatusModified)

	fake := newRaftFakeSTS()
	fake.setReady(host, 1)

	// Deterministic by construction, with no timer race: the snapshot passed in is unsafe
	// (2 of 3 Ready), so the gate must enter its poll loop; the injected count then reports the
	// recovered peer on the first in-loop refresh, so the wait SUCCEEDS after exactly one poll.
	// A wall-clock fixture here is flaky in the sneaky direction - under load the recovery can
	// land before the call, the gate returns safe without ever waiting, and the charge is
	// legitimately ~0 while observed elapsed time is not.
	w := &worker{
		a:                           a.NewAnnouncer(nil, nil),
		c:                           &Controller{kube: &raftFakeKube{sts: fake}},
		countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 3 },
		quorumDisruptPollOverride:   5 * time.Millisecond,
		quorumDisruptWaitOverride:   time.Second,
	}

	snap := hostEnsembleSnapshot{rolling: true, members: 3, readyCount: 2}
	require.NoError(t, w.ensureQuorumSafeToDisruptHost(ctx, host, nil, &snap),
		"the peer recovered, so the gate must let the disrupt proceed")

	// One poll interval is an exact structural floor: the loop cannot reach the success branch
	// without sleeping at least once. A refund on the success path drops the charge to ~0.
	require.GreaterOrEqual(t, w.quorumWaitSpent, w.quorumDisruptPollInterval(),
		"a successful wait still costs the pass its time and must be charged, not refunded")
	// ...and an upper bound, because the floor alone does not pin the CADENCE. The peer here
	// recovers on the very first refresh, so a correct gate notices within roughly one interval.
	// Sleeping the whole remaining budget in one go instead (clamping the wrong way, or not
	// clamping at all) still satisfies the floor while leaving a recovered peer unnoticed for
	// the full timeout - 2 minutes in production.
	require.Less(t, w.quorumWaitSpent, 10*w.quorumDisruptPollInterval(),
		"the gate must poll at its interval, not sleep out the whole budget in one wait")
}

// TestForceRecreateIsDisruptiveEvenWhenUnchanged pins the opts axis of the gate. Every other
// gate test passes nil opts, so this was the one input with no coverage - and dropping it is
// silent: a data-loss recreate (hostPVCsDataLossDetectedOptions) targets a host whose
// fingerprint is ObjectStatusSame, so without ForceRecreate the gate reads "not disrupting" and
// waves through a delete-and-recreate of a Ready Keeper with no quorum headroom.
func TestForceRecreateIsDisruptiveEvenWhenUnchanged(t *testing.T) {
	ctx := context.Background()
	w := &worker{}
	host := hostOnCR(chkWithHosts(3))
	host.Runtime.CurStatefulSet = &apps.StatefulSet{}
	host.Runtime.CurStatefulSet.Status.ReadyReplicas = 1
	host.GetReconcileAttributes().SetStatus(types.ObjectStatusSame)
	snap := hostEnsembleSnapshot{rolling: true, members: 3, readyCount: 2}

	require.False(t, w.hostDisruptionWouldBreakQuorum(ctx, host, nil, snap),
		"an unchanged host with no opts disrupts nothing")
	require.True(t, w.hostDisruptionWouldBreakQuorum(ctx, host,
		statefulset.NewReconcileStatefulSetOptions().SetForceRecreate(), snap),
		"ForceRecreate deletes and recreates the StatefulSet - the gate must see that as a disrupt")
}

// TestReconcileShardsWorkersNumIsOne is a tripwire, not a behaviour test. The constant is
// load-bearing for Raft safety - see the comment on getReconcileShardsWorkersNum - and nothing
// else fails if someone raises it, so the invariant is asserted here explicitly.
func TestReconcileShardsWorkersNumIsOne(t *testing.T) {
	require.Equal(t, 1, (&worker{}).getReconcileShardsWorkersNum(nil, nil),
		"shard concurrency must stay 1 until the quorum gate has a cluster-wide disrupt budget")
}

// TestPassBudgetBoundsTheWholeShardLoop pins the budget as a property of the PASS: the gate is
// invoked once per host by the REAL shard loop, so the spend accumulates the way it does in
// production instead of through two hand-written calls.
//
// Be clear about what this does and does not cover. The seam's closure supplies its own opts
// (SetForceRecreate, which production does not set here) and skips PrepareHostStatefulSetWithStatus,
// so willDisrupt is synthetic; ordering and soft-defer continuation are also covered by
// TestReconcileShardWithHostsLoop. The genuinely new coverage is the budget SHARING across hosts
// and the pass-level ceiling, neither of which any other test sees.
func TestPassBudgetBoundsTheWholeShardLoop(t *testing.T) {
	ctx := context.Background()

	cr := chkWithHosts(3)
	hosts := cacheAllHostsAt(cr, 1, 1, 0) // h2 is down -> recovery-first must visit it first
	for i := range hosts {
		hostAtOnCR(cr, i)
	}
	// Real countReadyEnsembleMembers against the fake: ready=2 of members=3, so quorum(3)=2 is
	// exactly met and disrupting either Ready host is refused.
	fake := newRaftFakeSTS()
	fake.setReady(hosts[0], 1)
	fake.setReady(hosts[1], 1)
	fake.setReady(hosts[2], 0)

	w := &worker{
		a:                         a.NewAnnouncer(nil, nil),
		c:                         &Controller{kube: &raftFakeKube{sts: fake}},
		quorumDisruptPollOverride: 5 * time.Millisecond,
		quorumDisruptWaitOverride: 150 * time.Millisecond,
	}
	w.newTask(cr, nil)

	var visited []string
	spentAt := map[string]time.Duration{}
	// Drive the gate for real, but stop short of the StatefulSet reconcile: this test is about
	// the loop's budget arithmetic, not about STS mechanics.
	w.reconcileHostFn = func(ctx context.Context, host *api.Host) error {
		snap, err := w.snapshotHostEnsemble(ctx, host)
		if err != nil {
			return err
		}
		visited = append(visited, host.GetName())
		err = w.ensureQuorumSafeToDisruptHost(ctx, host,
			statefulset.NewReconcileStatefulSetOptions().SetForceRecreate(), &snap)
		spentAt[host.GetName()] = w.quorumWaitSpent
		return err
	}

	shard := cr.Spec.Configuration.Clusters[0].Layout.Shards[0]
	require.ErrorIs(t, w.reconcileShardWithHosts(ctx, shard), common.ErrCRUDDeferred,
		"a deferral is soft: the loop must finish and surface the deferral at the end")

	require.Equal(t, []string{"h2", "h0", "h1"}, visited,
		"recovery-first must hold and a soft defer must not cut the loop short")
	require.NotZero(t, spentAt["h0"], "the first gated host must actually have waited")
	require.Equal(t, spentAt["h0"], spentAt["h1"],
		"the second gated host must inherit the spent budget, not buy another full wait")
	// Generous slack on purpose. The charged window closes after the final refresh's Gets, which
	// land past the deadline, and on a loaded single-P runner that overshoot reached 30ms against
	// a 150ms budget. The regimes are far apart - a per-host budget spends ~2x the timeout - so a
	// 1.5x ceiling separates them with room to spare, where a one-poll ceiling flaked at 10%.
	require.LessOrEqual(t, w.quorumWaitSpent, 3*w.quorumDisruptWaitTimeout()/2,
		"total gate wait across the pass must not exceed one timeout, however many hosts are gated")
}

// TestReconcileHostStatefulSetWithEnsembleSnapshot pins the WIRING between the snapshot and the
// StatefulSet reconcile, which no other test can see.
//
// Both quorum protections hang off snap.rolling: ensureQuorumSafeToDisruptHost returns nil on its
// first line for a non-rolling snapshot, and prepareStsReconcileOptsWaitSection drops
// SetWaitUntilReady. An empty hostEnsembleSnapshot is non-rolling, so handing one to
// reconcileHostStatefulSet - by passing a literal, by snapshotting AFTER the disruption, or by
// demoting the snapshot error to a warning and continuing on the zero value - switches both off
// on every host while every gate test here keeps passing, because they all supply their own.
func TestReconcileHostStatefulSetWithEnsembleSnapshot(t *testing.T) {
	ctx := context.Background()

	// 3 members with one already down: ready(2) meets quorum(3)=2 exactly, so the pass is rolling
	// with zero headroom and disrupting a Ready host must be refused.
	newFixture := func() (*worker, *api.Host, *raftFakeSTS) {
		cr := chkWithHosts(3)
		hosts := cacheAllHostsAt(cr, 1, 1, 0)
		for i := range hosts {
			hostAtOnCR(cr, i)
		}
		fake := newRaftFakeSTS()
		fake.setReady(hosts[0], 1)
		fake.setReady(hosts[1], 1)
		fake.setReady(hosts[2], 0)

		w := &worker{
			a:                         a.NewAnnouncer(nil, nil),
			c:                         &Controller{kube: &raftFakeKube{sts: fake}},
			quorumDisruptPollOverride: 2 * time.Millisecond,
			quorumDisruptWaitOverride: 10 * time.Millisecond,
		}
		w.newTask(cr, nil)
		return w, hosts[0], fake
	}

	t.Run("the live snapshot is what the StatefulSet reconcile is gated on", func(t *testing.T) {
		w, host, fake := newFixture()

		snap, err := reconcileHostStatefulSetGuarded(ctx, w, host)

		require.ErrorIs(t, err, common.ErrCRUDDeferred,
			"the freshly taken snapshot must reach the quorum gate - an empty one disables it")
		require.Zero(t, fake.mutationCount(),
			"a host the gate deferred must never have its StatefulSet created or updated")
		require.Equal(t, hostEnsembleSnapshot{rolling: true, members: 3, readyCount: 2}, snap,
			"the caller reuses this snapshot downstream, so it must be the real one")
	})

	t.Run("a snapshot read failure aborts before any disruption", func(t *testing.T) {
		w, host, fake := newFixture()
		boom := errors.New("apiserver unavailable")
		fake.err = boom

		snap, err := reconcileHostStatefulSetGuarded(ctx, w, host)

		require.ErrorIs(t, err, boom,
			"an unreadable ensemble must fail the pass: continuing degrades to bootstrap, the unsafe direction")
		require.Equal(t, hostEnsembleSnapshot{}, snap)
		require.Zero(t, fake.mutationCount(),
			"nothing may be disrupted while ensemble readiness is unknown")
	})
}

// reconcileHostStatefulSetGuarded turns a disruption that should never have started into a plain
// error. The fixture builds a Controller with no namer, so a reconcile that slips past the quorum
// gate runs on into recreateStatefulSet -> doDeleteStatefulSet and nil-derefs r.namer - the right
// verdict, but a bare panic tears down the test binary and hides which assertion caught it. The
// converted error satisfies neither subtest's ErrorIs, so a gate removal still fails loudly.
func reconcileHostStatefulSetGuarded(
	ctx context.Context,
	w *worker,
	host *api.Host,
) (snap hostEnsembleSnapshot, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("host was disrupted past the quorum gate: %v", r)
		}
	}()
	return w.reconcileHostStatefulSetWithEnsembleSnapshot(ctx, host, nil)
}

// TestQuorumWaitReturnsContextErrorOnCancellation pins that a cancelled reconcile reports
// cancellation rather than a downstream API error - i.e. the `return ctx.Err()` after the wait.
//
// It exercises the ordinary path, where the clamped sleep is still positive and ctx.Done() wins
// the select deterministically. It does NOT reach the narrow window the follow-up re-check exists
// for: once the clamp yields a non-positive duration both select cases are ready at once and Go
// picks at random. Constructing that window deterministically would mean landing a cancellation
// inside a sub-millisecond gap, so the re-check is defensive and deliberately unpinned.
func TestQuorumWaitReturnsContextErrorOnCancellation(t *testing.T) {
	cr := chkWithHosts(3)
	cacheAllHostsAt(cr, 1, 1, 0)
	host := hostAtOnCR(cr, 0)
	host.GetReconcileAttributes().SetStatus(types.ObjectStatusModified)

	fake := newRaftFakeSTS()
	fake.setReady(host, 1)
	w := &worker{
		a:                           a.NewAnnouncer(nil, nil),
		c:                           &Controller{kube: &raftFakeKube{sts: fake}},
		countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 2 },
		quorumDisruptPollOverride:   time.Millisecond,
		quorumDisruptWaitOverride:   time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled: the very first wait sees a dead context

	snap := hostEnsembleSnapshot{rolling: true, members: 3, readyCount: 2}
	for i := 0; i < 50; i++ {
		require.ErrorIs(t, w.ensureQuorumSafeToDisruptHost(ctx, host, nil, &snap), context.Canceled,
			"a cancelled reconcile must report cancellation, not a downstream API error")
	}
}
