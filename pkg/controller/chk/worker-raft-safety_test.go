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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
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
// the host is left un-rejoined while the loop moves on to its peers (#2069).
func TestRefreshQuorumSnapshotCountsFreezesRolling(t *testing.T) {
	ctx := context.Background()

	var ready atomic.Int32
	ready.Store(3)
	w := &worker{
		countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int {
			return int(ready.Load())
		},
	}
	host := hostOnCR(chkWithHosts(3))

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

// raftFakeSTS answers the live StatefulSet lookups countReadyEnsembleMembers makes.
// Ready replicas are keyed by host pointer so a fixture can hold peers at different
// readiness and flip one mid-wait, the way a recovering Keeper pod does.
// Every mutating method panics: the quorum gate only ever reads.
type raftFakeSTS struct {
	mu       sync.Mutex
	ready    map[*api.Host]int32
	err      error
	getCalls int
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
		if host, ok := params[0].(*api.Host); ok {
			sts.Status.ReadyReplicas = f.ready[host]
		}
	}
	return sts, nil
}

func (f *raftFakeSTS) Create(ctx context.Context, sts *apps.StatefulSet) (*apps.StatefulSet, error) {
	panic("quorum gate must not create a StatefulSet")
}

func (f *raftFakeSTS) Update(ctx context.Context, sts *apps.StatefulSet) (*apps.StatefulSet, error) {
	panic("quorum gate must not update a StatefulSet")
}

func (f *raftFakeSTS) Delete(ctx context.Context, namespace, name string) error {
	panic("quorum gate must not delete a StatefulSet")
}

func (f *raftFakeSTS) List(ctx context.Context, namespace string, opts meta.ListOptions) ([]apps.StatefulSet, error) {
	return nil, nil
}

// raftFakeKube exposes STS() only. IKube is embedded as a nil interface, so any other
// accessor panics - a guard that the quorum gate reaches nothing else in kube.
type raftFakeKube struct {
	interfaces.IKube
	sts interfaces.IKubeSTS
}

func (k *raftFakeKube) STS() interfaces.IKubeSTS { return k.sts }

func raftWorkerWithSTS(sts interfaces.IKubeSTS) *worker {
	return &worker{c: &Controller{kube: &raftFakeKube{sts: sts}}}
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

// TestCountReadyEnsembleMembersSwallowsStatefulSetGetError pins CURRENT behavior, which is
// not the behavior the fix commit claims.
//
// countReadyEnsembleMembers records the first Get error in a local firstErr and then drops
// it on the floor - the signature returns only int, so an API blip is indistinguishable
// from "peer not Ready". At snapshot time that undercount flips rolling to false, which
// disables both the quorum gate and the Ready wait, i.e. it fails OPEN on the one path
// where failing closed matters. Update this test when the error is surfaced.
// TestCountReadyEnsembleMembersSurfacesStatefulSetGetError pins the failure direction.
//
// Swallowing the Get error undercounts Ready members, which flips the pass to bootstrap - and a
// bootstrap pass skips both the quorum gate and the Ready wait. An apiserver blip would therefore
// re-enable exactly the unguarded fan-out this gate exists to prevent (#2069). The Get already
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

// TestSnapshotUsesAncestorMemberCount pins ensemble size as max(desired, ancestor).
//
// Live Raft still runs the ancestor's membership until clean() purges the removed peers,
// so a 3->1 downscale that sized the ensemble on the desired count alone got members=1,
// fell through the small-ensemble bypass, and disrupted the survivor unguarded - exactly
// when 2 of 3 were still required.
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

	t.Run("desired count wins when it is the larger set", func(t *testing.T) {
		up := chkWithHosts(3)
		up.SetAncestor(chkWithHosts(1))
		upSnap, err := w.snapshotHostEnsemble(ctx, hostOnCR(up))
		require.NoError(t, err)
		require.Equal(t, 3, upSnap.members)
	})

	t.Run("no ancestor falls back to desired count", func(t *testing.T) {
		fresh := chkWithHosts(3)
		freshCRSnap, err := w.snapshotHostEnsemble(ctx, hostOnCR(fresh))
		require.NoError(t, err)
		require.Equal(t, 3, freshCRSnap.members)
	})
}
