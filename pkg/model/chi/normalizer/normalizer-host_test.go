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

package normalizer

import (
	"testing"

	"github.com/stretchr/testify/require"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// TestNormalizeHostNamePositionIndependence verifies that an explicitly specified
// bare-number host name keeps the same canonical form regardless of the host's
// position in the hosts list. Before this behavior, a host named "3" was normalized
// to "0-3" only while it sat at replica index 3; once a preceding list entry was
// removed and the host shifted to index 2, the name no longer matched the
// index-derived auto-generated patterns and was kept verbatim - producing a brand
// new StatefulSet ("chi-...-3") with an empty PVC while the old one was purged.
func TestNormalizeHostNamePositionIndependence(t *testing.T) {
	n := New(nil)

	shardsLayout := &chi.Cluster{Layout: &chi.ChiClusterLayout{ShardsExplicitlySpecified: true}}
	replicasLayout := &chi.Cluster{Layout: &chi.ChiClusterLayout{ReplicasExplicitlySpecified: true}}
	bothLayout := &chi.Cluster{Layout: &chi.ChiClusterLayout{ShardsExplicitlySpecified: true, ReplicasExplicitlySpecified: true}}

	type tc struct {
		name         string
		cluster      *chi.Cluster
		shardName    string
		shardIndex   int
		replicaName  string
		replicaIndex int
		hostName     string
		expect       string
	}

	cases := []tc{
		{
			// The dangerous case: hosts "0","1","2","3","4" under shard "0", entry "2" removed.
			// Host "3" now sits at replica index 2 and must still normalize to "0-3".
			name:    "bare number at shifted position keeps its identity",
			cluster: shardsLayout, shardName: "0", shardIndex: 0, replicaName: "2", replicaIndex: 2,
			hostName: "3", expect: "0-3",
		},
		{
			// Same host at its natural position - the historical auto-generated-name path.
			name:    "bare number at natural position",
			cluster: shardsLayout, shardName: "0", shardIndex: 0, replicaName: "2", replicaIndex: 2,
			hostName: "2", expect: "0-2",
		},
		{
			name:    "canonical name at shifted position is kept",
			cluster: shardsLayout, shardName: "0", shardIndex: 0, replicaName: "2", replicaIndex: 2,
			hostName: "0-3", expect: "0-3",
		},
		{
			name:    "custom name is kept verbatim",
			cluster: shardsLayout, shardName: "0", shardIndex: 0, replicaName: "2", replicaIndex: 2,
			hostName: "leader", expect: "leader",
		},
		{
			name:    "empty name is auto-generated",
			cluster: shardsLayout, shardName: "0", shardIndex: 0, replicaName: "2", replicaIndex: 2,
			hostName: "", expect: "0-2",
		},
		{
			// Hosts declared under a replica vary along the shard axis: the number is the shard part.
			name:    "bare number under replicas-defined layout",
			cluster: replicasLayout, shardName: "1", shardIndex: 1, replicaName: "0", replicaIndex: 0,
			hostName: "3", expect: "3-0",
		},
		{
			// With hosts declared under both shards and replicas, provenance is unknown -
			// keep the historical verbatim behavior.
			name:    "bare number with both layouts declared is kept verbatim",
			cluster: bothLayout, shardName: "0", shardIndex: 0, replicaName: "2", replicaIndex: 2,
			hostName: "3", expect: "3",
		},
		{
			// Not a canonical integer - not treated as a positional identity.
			name:    "leading-zero name is kept verbatim",
			cluster: shardsLayout, shardName: "0", shardIndex: 0, replicaName: "2", replicaIndex: 2,
			hostName: "03", expect: "03",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			shard := &chi.ChiShard{Name: c.shardName}
			replica := &chi.ChiReplica{Name: c.replicaName}
			host := &chi.Host{Name: c.hostName}
			n.normalizeHostName(host, c.cluster, shard, c.shardIndex, replica, c.replicaIndex)
			require.Equal(t, c.expect, host.Name)
		})
	}
}
