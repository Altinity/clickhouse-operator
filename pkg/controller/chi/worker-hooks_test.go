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

package chi

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	chiLabeler "github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	commonLabeler "github.com/altinity/clickhouse-operator/pkg/model/common/tags/labeler"
)

// makeCompletedCR builds a minimal CHI with one cluster, one shard, one host —
// enough for FindHost(cluster, shard, replica) to resolve. ChiShard.FindHost matches
// against host.Runtime.Address.HostName (not host.Name), so the fixture must populate
// the Runtime.Address fields rather than just the top-level Name.
func makeCompletedCR(clusterName, shardName, replicaName string) *api.ClickHouseInstallation {
	host := &api.Host{Name: replicaName}
	host.Runtime.Address.HostName = replicaName
	shard := &api.ChiShard{
		Name:  shardName,
		Hosts: []*api.Host{host},
	}
	cluster := &api.Cluster{
		Name: clusterName,
		Layout: &api.ChiClusterLayout{
			Shards: []*api.ChiShard{shard},
		},
	}
	cr := &api.ClickHouseInstallation{
		Spec: api.ChiSpec{
			Configuration: &api.Configuration{
				Clusters: []*api.Cluster{cluster},
			},
		},
	}
	return cr
}

// objWithLabels wraps a label map in a meta.ObjectMeta so it satisfies meta.Object
// for findHostInCompletedFromLabels.
func objWithLabels(labels map[string]string) meta.Object {
	return &meta.ObjectMeta{Labels: labels}
}

// TestFindHostInCompletedFromLabels exercises the orphan-host resolver: given a
// StatefulSet's labels, can we recover the corresponding *api.Host from the
// previously-completed CR? Covers the skip cases (missing labels, no match) and
// the happy path (labels match a real host in completed).
func TestFindHostInCompletedFromLabels(t *testing.T) {
	completed := makeCompletedCR("c1", "s1", "r1")
	labeler := chiLabeler.New(completed)

	clusterKey := labeler.Get(commonLabeler.LabelClusterName)
	shardKey := labeler.Get(commonLabeler.LabelShardName)
	replicaKey := labeler.Get(commonLabeler.LabelReplicaName)

	t.Run("nil labels → nil", func(t *testing.T) {
		got := findHostInCompletedFromLabels(completed, labeler, &meta.ObjectMeta{Labels: nil})
		require.Nil(t, got)
	})

	t.Run("missing cluster label → nil", func(t *testing.T) {
		obj := objWithLabels(map[string]string{
			shardKey:   "s1",
			replicaKey: "r1",
		})
		require.Nil(t, findHostInCompletedFromLabels(completed, labeler, obj))
	})

	t.Run("missing shard label → nil", func(t *testing.T) {
		obj := objWithLabels(map[string]string{
			clusterKey: "c1",
			replicaKey: "r1",
		})
		require.Nil(t, findHostInCompletedFromLabels(completed, labeler, obj))
	})

	t.Run("missing replica label → nil", func(t *testing.T) {
		obj := objWithLabels(map[string]string{
			clusterKey: "c1",
			shardKey:   "s1",
		})
		require.Nil(t, findHostInCompletedFromLabels(completed, labeler, obj))
	})

	t.Run("labels point to non-existent cluster → nil (true orphan)", func(t *testing.T) {
		obj := objWithLabels(map[string]string{
			clusterKey: "ghost",
			shardKey:   "s1",
			replicaKey: "r1",
		})
		require.Nil(t, findHostInCompletedFromLabels(completed, labeler, obj))
	})

	t.Run("labels point to non-existent shard → nil", func(t *testing.T) {
		obj := objWithLabels(map[string]string{
			clusterKey: "c1",
			shardKey:   "ghost",
			replicaKey: "r1",
		})
		require.Nil(t, findHostInCompletedFromLabels(completed, labeler, obj))
	})

	t.Run("labels point to non-existent replica → nil", func(t *testing.T) {
		obj := objWithLabels(map[string]string{
			clusterKey: "c1",
			shardKey:   "s1",
			replicaKey: "ghost",
		})
		require.Nil(t, findHostInCompletedFromLabels(completed, labeler, obj))
	})

	t.Run("labels match a real host → returns that host", func(t *testing.T) {
		obj := objWithLabels(map[string]string{
			clusterKey: "c1",
			shardKey:   "s1",
			replicaKey: "r1",
		})
		got := findHostInCompletedFromLabels(completed, labeler, obj)
		require.NotNil(t, got)
		require.Equal(t, "r1", got.GetName())
	})
}

// TestComputeFiredHostEventsFromState covers the pure event-firing rules for a
// host. State (ancestor presence, stopped flags, restart/rollout decisions) is
// passed in as a tuple so we can exercise every combination without standing up
// the cluster/CR/pod plumbing that the live predicates transitively need.
func TestComputeFiredHostEventsFromState(t *testing.T) {
	t.Run("new host (no ancestor) → HostCreate only", func(t *testing.T) {
		got := computeFiredHostEventsFromState(hostState{})
		require.Equal(t, []api.HookEvent{api.HookEventHostCreate}, got)
	})

	t.Run("existing host (has ancestor) → HostUpdate, no HostCreate", func(t *testing.T) {
		got := computeFiredHostEventsFromState(hostState{hasAncestor: true})
		require.Contains(t, got, api.HookEventHostUpdate)
		require.NotContains(t, got, api.HookEventHostCreate)
	})

	t.Run("currently-stopped host fires HostStop and HostShutdown", func(t *testing.T) {
		got := computeFiredHostEventsFromState(hostState{hasAncestor: true, isStopped: true})
		require.Contains(t, got, api.HookEventHostStop)
		require.Contains(t, got, api.HookEventHostShutdown)
	})

	t.Run("ancestor stopped, current not stopped → HostStart fires", func(t *testing.T) {
		got := computeFiredHostEventsFromState(hostState{
			hasAncestor: true, ancestorIsStopped: true, isStopped: false,
		})
		require.Contains(t, got, api.HookEventHostStart)
		require.NotContains(t, got, api.HookEventHostStop)
		require.NotContains(t, got, api.HookEventHostShutdown)
	})

	t.Run("ancestor stopped, current also stopped → no HostStart (it's not transitioning to running)", func(t *testing.T) {
		got := computeFiredHostEventsFromState(hostState{
			hasAncestor: true, ancestorIsStopped: true, isStopped: true,
		})
		require.NotContains(t, got, api.HookEventHostStart)
		require.Contains(t, got, api.HookEventHostStop)
	})

	t.Run("no ancestor + ancestorIsStopped flag is meaningless → HostStart does NOT fire", func(t *testing.T) {
		// On first creation there's nothing to "start FROM stopped"; HostCreate
		// covers the new-host case. Sanity: even if caller mistakenly sets
		// ancestorIsStopped without hasAncestor, HostStart must not fire.
		got := computeFiredHostEventsFromState(hostState{
			hasAncestor: false, ancestorIsStopped: true,
		})
		require.NotContains(t, got, api.HookEventHostStart)
	})

	t.Run("forceRestart=true fires HostConfigRestart and HostShutdown aggregate", func(t *testing.T) {
		got := computeFiredHostEventsFromState(hostState{hasAncestor: true, forceRestart: true})
		require.Contains(t, got, api.HookEventHostConfigRestart)
		require.Contains(t, got, api.HookEventHostShutdown)
	})

	t.Run("requiresRollout=true fires HostRollout and HostShutdown aggregate", func(t *testing.T) {
		got := computeFiredHostEventsFromState(hostState{hasAncestor: true, requiresRollout: true})
		require.Contains(t, got, api.HookEventHostRollout)
		require.Contains(t, got, api.HookEventHostShutdown)
	})

	t.Run("both forceRestart and requiresRollout → both events plus single HostShutdown", func(t *testing.T) {
		got := computeFiredHostEventsFromState(hostState{
			hasAncestor: true, forceRestart: true, requiresRollout: true,
		})
		require.Contains(t, got, api.HookEventHostConfigRestart)
		require.Contains(t, got, api.HookEventHostRollout)
		count := 0
		for _, e := range got {
			if e == api.HookEventHostShutdown {
				count++
			}
		}
		require.Equal(t, 1, count, "HostShutdown should aggregate once, not per source event")
	})

	t.Run("benign reconcile (existing host, no restart) → no HostShutdown", func(t *testing.T) {
		got := computeFiredHostEventsFromState(hostState{hasAncestor: true})
		require.NotContains(t, got, api.HookEventHostShutdown)
	})

	t.Run("HostDelete is never emitted from the regular reconcile path", func(t *testing.T) {
		// Sanity: even with all knobs flipped, HostDelete must not appear here.
		// It comes from firedHostDeleteEvents on the deletion sweep instead.
		for _, restart := range []bool{false, true} {
			for _, rollout := range []bool{false, true} {
				got := computeFiredHostEventsFromState(hostState{
					hasAncestor:     true,
					isStopped:       true,
					forceRestart:    restart,
					requiresRollout: rollout,
				})
				require.NotContains(t, got, api.HookEventHostDelete,
					"HostDelete must NOT fire from reconcile; restart=%v rollout=%v", restart, rollout)
			}
		}
	})
}

// clusterFixture builds a *api.Cluster with N hosts, configurable per-host ancestor
// presence (via host.Name placeholder — HasAncestor is what firedClusterEvents reads).
//
// The function constructs a real CR/Cluster/Shard graph and a paired ancestor CR
// so that host.HasAncestor() returns the desired value. HasAncestor walks
// host.GetCR().GetAncestor().FindHost(...) — so we wire up the parent CR with a
// status that yields the right ancestor.
type clusterFixture struct {
	hostsHaveAncestor []bool // one entry per host; true = has ancestor
}

func (f clusterFixture) build() *api.Cluster {
	const clusterName = "test-cluster"
	const shardName = "0"

	// Current hosts.
	hosts := make([]*api.Host, 0, len(f.hostsHaveAncestor))
	for i := range f.hostsHaveAncestor {
		h := &api.Host{Name: replicaName(i)}
		h.Runtime.Address.HostName = replicaName(i)
		h.Runtime.Address.ClusterName = clusterName
		h.Runtime.Address.ShardName = shardName
		h.Runtime.Address.ReplicaName = replicaName(i)
		hosts = append(hosts, h)
	}
	cluster := &api.Cluster{
		Name: clusterName,
		Layout: &api.ChiClusterLayout{
			Shards: []*api.ChiShard{{Name: shardName, Hosts: hosts}},
		},
	}

	// Build the ancestor CR containing only the hosts whose hasAncestor=true.
	// HasAncestor matches by FindHost which uses host.Runtime.Address.HostName.
	//
	// NOTE: ancestor hosts are NOT linked back to the ancestorCR via SetCR.
	// host.IsStopped() walks host.GetCR().IsStopped(), which is nil-safe (returns
	// false for nil CR). Tests of the firedHostEvents wrapper that need to exercise
	// the "ancestor was stopped" branch should use the pure
	// computeFiredHostEventsFromState directly with ancestorIsStopped=true.
	ancestorHosts := []*api.Host{}
	for i, hasAnc := range f.hostsHaveAncestor {
		if !hasAnc {
			continue
		}
		ah := &api.Host{Name: replicaName(i)}
		ah.Runtime.Address.HostName = replicaName(i)
		ancestorHosts = append(ancestorHosts, ah)
	}
	ancestorCR := &api.ClickHouseInstallation{
		Spec: api.ChiSpec{
			Configuration: &api.Configuration{
				Clusters: []*api.Cluster{{
					Name: clusterName,
					Layout: &api.ChiClusterLayout{
						Shards: []*api.ChiShard{{Name: shardName, Hosts: ancestorHosts}},
					},
				}},
			},
		},
	}

	// Wire up a current CR whose Status holds the ancestor — that's the chain
	// host.GetCR().GetAncestor() walks.
	currentCR := &api.ClickHouseInstallation{
		Spec: api.ChiSpec{
			Configuration: &api.Configuration{
				Clusters: []*api.Cluster{cluster},
			},
		},
	}
	currentCR.EnsureStatus().NormalizedCRCompleted = ancestorCR

	// Cross-link hosts to their CR so HasAncestor traversal works.
	for _, h := range hosts {
		h.Runtime.SetCR(currentCR)
	}

	return cluster
}

func replicaName(i int) string { return "r" + strconv.Itoa(i) }

// TestFiredClusterEvents covers the cluster-scope event emitter.
func TestFiredClusterEvents(t *testing.T) {
	w := &worker{}

	t.Run("nil cluster returns nil", func(t *testing.T) {
		require.Nil(t, w.firedClusterEvents(nil))
	})

	t.Run("empty cluster (no hosts) emits no events", func(t *testing.T) {
		c := clusterFixture{hostsHaveAncestor: nil}.build()
		require.Nil(t, w.firedClusterEvents(c))
	})

	t.Run("all hosts new (none has ancestor) → ClusterCreate", func(t *testing.T) {
		c := clusterFixture{hostsHaveAncestor: []bool{false, false}}.build()
		got := w.firedClusterEvents(c)
		require.Equal(t, []api.HookEvent{api.HookEventClusterCreate}, got)
	})

	t.Run("at least one host has ancestor → ClusterReconcile", func(t *testing.T) {
		c := clusterFixture{hostsHaveAncestor: []bool{true, false}}.build()
		got := w.firedClusterEvents(c)
		require.Equal(t, []api.HookEvent{api.HookEventClusterReconcile}, got)
	})

	t.Run("all hosts have ancestor → ClusterReconcile", func(t *testing.T) {
		c := clusterFixture{hostsHaveAncestor: []bool{true, true}}.build()
		got := w.firedClusterEvents(c)
		require.Equal(t, []api.HookEvent{api.HookEventClusterReconcile}, got)
	})

	t.Run("ClusterDelete never emitted from reconcile path", func(t *testing.T) {
		for _, hosts := range [][]bool{nil, {false}, {true}, {true, false}} {
			c := clusterFixture{hostsHaveAncestor: hosts}.build()
			got := w.firedClusterEvents(c)
			require.NotContains(t, got, api.HookEventClusterDelete,
				"ClusterDelete must NOT fire from reconcile; hosts=%v", hosts)
		}
	})
}

// TestFiredHostDeleteEvents covers the deletion-sweep emitter.
func TestFiredHostDeleteEvents(t *testing.T) {
	w := &worker{}

	t.Run("nil host → nil", func(t *testing.T) {
		require.Nil(t, w.firedHostDeleteEvents(nil))
	})

	t.Run("any host → HostDelete + HostShutdown", func(t *testing.T) {
		got := w.firedHostDeleteEvents(&api.Host{})
		require.ElementsMatch(t, []api.HookEvent{
			api.HookEventHostDelete,
			api.HookEventHostShutdown,
		}, got)
	})
}

// TestRunClusterSQLHookActionTargetDispatch exercises the no-host error path of
// runClusterSQLHookAction for each target value, including case-insensitive
// normalization. Full SQL execution isn't exercised (no schemer mock); the goal
// is to lock in dispatch correctness and the empty-cluster error paths.
func TestRunClusterSQLHookActionTargetDispatch(t *testing.T) {
	w := &worker{}
	emptyCluster := &api.Cluster{
		Name:   "empty",
		Layout: &api.ChiClusterLayout{Shards: nil},
	}

	cases := []struct {
		name   string
		target *api.HookTarget
	}{
		{"FirstHost (default, target unset)", nil},
		{"FirstHost (explicit)", types.NewString(string(api.HookTargetFirstHost))},
		{"FirstHost (lowercase normalized)", types.NewString("firsthost")},
		{"AllHosts (case-insensitive)", types.NewString("allhosts")},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name+": no hosts → error", func(t *testing.T) {
			action := &api.HookAction{
				SQL:    &api.SQLHookAction{Queries: []string{"SELECT 1"}},
				Target: c.target,
				Events: []api.HookEvent{api.HookEventClusterReconcile},
			}
			err := w.runClusterSQLHookAction(t.Context(), action, emptyCluster)
			require.Error(t, err, "empty cluster must surface a no-hosts error")
			require.Contains(t, err.Error(), "no hosts for hook execution")
		})
	}

	t.Run("AllShards: no shards → no error (nothing to execute is OK)", func(t *testing.T) {
		// AllShards walks shards individually; an empty shard list means the loop
		// runs zero times and returns nil. Distinct semantics from FirstHost/AllHosts
		// which require at least one host to dispatch to.
		action := &api.HookAction{
			SQL:    &api.SQLHookAction{Queries: []string{"SELECT 1"}},
			Target: types.NewString(string(api.HookTargetAllShards)),
			Events: []api.HookEvent{api.HookEventClusterReconcile},
		}
		err := w.runClusterSQLHookAction(t.Context(), action, emptyCluster)
		require.NoError(t, err)
	})

	t.Run("Empty queries list → silent no-op regardless of target", func(t *testing.T) {
		action := &api.HookAction{
			SQL:    &api.SQLHookAction{Queries: nil},
			Target: types.NewString(string(api.HookTargetAllHosts)),
			Events: []api.HookEvent{api.HookEventClusterReconcile},
		}
		err := w.runClusterSQLHookAction(t.Context(), action, emptyCluster)
		require.NoError(t, err)
	})
}
