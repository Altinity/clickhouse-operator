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

package v1

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type apShardFixture struct {
	name  string
	hosts []string
}

// makeHostForAP builds a host with the Runtime.Address fields WalkRemoved relies on.
// Address fields are normally populated by the normalizer.
func makeHostForAP(clusterName, shardName, hostName string) *Host {
	host := &Host{Name: hostName}
	host.Runtime.Address.ClusterName = clusterName
	host.Runtime.Address.ShardName = shardName
	host.Runtime.Address.HostName = hostName
	return host
}

func makeCRForAP(clusterName string, shards ...apShardFixture) *ClickHouseInstallation {
	var chiShards []*ChiShard
	for _, s := range shards {
		var hosts []*Host
		for _, h := range s.hosts {
			hosts = append(hosts, makeHostForAP(clusterName, s.name, h))
		}
		chiShards = append(chiShards, &ChiShard{Name: s.name, Hosts: hosts})
	}
	return &ClickHouseInstallation{
		Spec: ChiSpec{
			Configuration: &Configuration{
				Clusters: []*Cluster{{
					Name:   clusterName,
					Layout: &ChiClusterLayout{Shards: chiShards},
				}},
			},
		},
	}
}

// walkRemovedNames collects the callback invocations of WalkRemoved.
func walkRemovedNames(ap IActionPlan) (clusters, shards, hosts []string) {
	ap.WalkRemoved(
		func(cluster ICluster) {
			clusters = append(clusters, cluster.GetName())
		},
		func(shard IShard) {
			shards = append(shards, shard.GetName())
		},
		func(host *Host) {
			hosts = append(hosts, host.GetName())
		},
	)
	return clusters, shards, hosts
}

// TestActionPlanWalkRemovedHostsByName verifies that removed hosts are computed by NAME,
// not by position in the hosts list. messagediff compares slices index-by-index, so removing
// hosts from the head of the list used to report the tail hosts as removed - the operator then
// issued SYSTEM DROP REPLICA for replicas that survive and left the actually removed replicas
// in Keeper.
func TestActionPlanWalkRemovedHostsByName(t *testing.T) {

	t.Run("hosts removed from the head of the list", func(t *testing.T) {
		old := makeCRForAP("production", apShardFixture{"0", []string{"0-0", "0-1", "0-2", "0-3", "0-4"}})
		new := makeCRForAP("production", apShardFixture{"0", []string{"0-2", "0-3", "0-4"}})
		ap := MakeActionPlan(old, new)

		clusters, shards, hosts := walkRemovedNames(ap)
		require.Empty(t, clusters)
		require.Empty(t, shards)
		require.Equal(t, []string{"0-0", "0-1"}, hosts)
		require.Equal(t, 2, ap.GetRemovedHostsNum())
	})

	t.Run("hosts removed from the tail of the list", func(t *testing.T) {
		old := makeCRForAP("production", apShardFixture{"0", []string{"0-0", "0-1", "0-2", "0-3", "0-4"}})
		new := makeCRForAP("production", apShardFixture{"0", []string{"0-0", "0-1", "0-2"}})
		ap := MakeActionPlan(old, new)

		clusters, shards, hosts := walkRemovedNames(ap)
		require.Empty(t, clusters)
		require.Empty(t, shards)
		require.Equal(t, []string{"0-3", "0-4"}, hosts)
		require.Equal(t, 2, ap.GetRemovedHostsNum())
	})

	t.Run("host removed from the middle of the list", func(t *testing.T) {
		old := makeCRForAP("production", apShardFixture{"0", []string{"0-0", "0-1", "0-2"}})
		new := makeCRForAP("production", apShardFixture{"0", []string{"0-0", "0-2"}})
		ap := MakeActionPlan(old, new)

		_, _, hosts := walkRemovedNames(ap)
		require.Equal(t, []string{"0-1"}, hosts)
	})

	t.Run("host renamed in place", func(t *testing.T) {
		old := makeCRForAP("production", apShardFixture{"0", []string{"0-0", "0-1"}})
		new := makeCRForAP("production", apShardFixture{"0", []string{"0-0", "leader"}})
		ap := MakeActionPlan(old, new)

		// The old name leaves the cluster - its replica has to be dropped
		_, _, hosts := walkRemovedNames(ap)
		require.Equal(t, []string{"0-1"}, hosts)
	})

	t.Run("no changes", func(t *testing.T) {
		old := makeCRForAP("production", apShardFixture{"0", []string{"0-0", "0-1"}})
		new := makeCRForAP("production", apShardFixture{"0", []string{"0-0", "0-1"}})
		ap := MakeActionPlan(old, new)

		clusters, shards, hosts := walkRemovedNames(ap)
		require.Empty(t, clusters)
		require.Empty(t, shards)
		require.Empty(t, hosts)
	})

	t.Run("whole shard removed - hosts are covered by the shard callback only", func(t *testing.T) {
		old := makeCRForAP("production",
			apShardFixture{"0", []string{"0-0", "0-1"}},
			apShardFixture{"1", []string{"1-0", "1-1"}},
		)
		new := makeCRForAP("production", apShardFixture{"0", []string{"0-0", "0-1"}})
		ap := MakeActionPlan(old, new)

		clusters, shards, hosts := walkRemovedNames(ap)
		require.Empty(t, clusters)
		require.Equal(t, []string{"1"}, shards)
		require.Empty(t, hosts)
		require.Equal(t, 2, ap.GetRemovedHostsNum())
	})

	t.Run("hosts added only", func(t *testing.T) {
		old := makeCRForAP("production", apShardFixture{"0", []string{"0-0"}})
		new := makeCRForAP("production", apShardFixture{"0", []string{"0-0", "0-1"}})
		ap := MakeActionPlan(old, new)

		clusters, shards, hosts := walkRemovedNames(ap)
		require.Empty(t, clusters)
		require.Empty(t, shards)
		require.Empty(t, hosts)
	})
}
