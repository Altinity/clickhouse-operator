package v1

import (
	"testing"

	"github.com/stretchr/testify/require"

	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// Test_KeeperClusterSettingsSource_PreferReplicaOverShard tests that when both
// shard and replica settings are explicitly specified, replica settings
// take precedence as they are more fine-grained control points.
func Test_KeeperClusterSettingsSource_PreferReplicaOverShard(t *testing.T) {
	testCases := []struct {
		name                 string
		shardsExplicit       bool
		replicasExplicit     bool
		expectShardAsSource  bool
		description          string
	}{
		{
			name:                 "neither_shards_nor_replicas_explicit",
			shardsExplicit:       false,
			replicasExplicit:     false,
			expectShardAsSource:  true,
			description:          "When neither shards nor replicas are explicitly specified, use shard as settings source",
		},
		{
			name:                 "only_shards_explicit",
			shardsExplicit:       true,
			replicasExplicit:     false,
			expectShardAsSource:  true,
			description:          "When only shards are explicitly specified, use shard as settings source",
		},
		{
			name:                 "only_replicas_explicit",
			shardsExplicit:       false,
			replicasExplicit:     true,
			expectShardAsSource:  false,
			description:          "When only replicas are explicitly specified, use replica as settings source",
		},
		{
			name:                 "both_shards_and_replicas_explicit",
			shardsExplicit:       true,
			replicasExplicit:     true,
			expectShardAsSource:  false,
			description:          "When both shards and replicas are explicitly specified, prefer replica settings as they are more fine-grained",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			cluster := &Cluster{
				Layout: &ChkClusterLayout{
					ShardsExplicitlySpecified:   tc.shardsExplicit,
					ReplicasExplicitlySpecified: tc.replicasExplicit,
				},
			}

			// Test isShardToBeUsedToInheritSettingsFrom
			actualShardSource := cluster.isShardToBeUsedToInheritSettingsFrom()
			require.Equal(tt, tc.expectShardAsSource, actualShardSource,
				"%s: isShardToBeUsedToInheritSettingsFrom() returned %v, expected %v",
				tc.description, actualShardSource, tc.expectShardAsSource)

			// Test SelectSettingsSourceFrom
			shard := &apiChi.ChiShard{Name: "test-shard"}
			replica := &apiChi.ChiReplica{Name: "test-replica"}

			src := cluster.SelectSettingsSourceFrom(shard, replica)
			if tc.expectShardAsSource {
				require.Equal(tt, shard, src,
					"%s: SelectSettingsSourceFrom() should return shard", tc.description)
			} else {
				require.Equal(tt, replica, src,
					"%s: SelectSettingsSourceFrom() should return replica", tc.description)
			}
		})
	}
}

// Test_KeeperClusterReplicaExplicitlySpecified tests that isReplicaExplicitlySpecified
// returns true whenever replicas are explicitly specified, regardless of shard specification
func Test_KeeperClusterReplicaExplicitlySpecified(t *testing.T) {
	testCases := []struct {
		name             string
		shardsExplicit   bool
		replicasExplicit bool
		expected         bool
	}{
		{
			name:             "replicas_not_explicit",
			shardsExplicit:   false,
			replicasExplicit: false,
			expected:         false,
		},
		{
			name:             "replicas_explicit_shards_not",
			shardsExplicit:   false,
			replicasExplicit: true,
			expected:         true,
		},
		{
			name:             "replicas_explicit_shards_explicit",
			shardsExplicit:   true,
			replicasExplicit: true,
			expected:         true,
		},
		{
			name:             "replicas_not_explicit_shards_explicit",
			shardsExplicit:   true,
			replicasExplicit: false,
			expected:         false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			cluster := &Cluster{
				Layout: &ChkClusterLayout{
					ShardsExplicitlySpecified:   tc.shardsExplicit,
					ReplicasExplicitlySpecified: tc.replicasExplicit,
				},
			}

			actual := cluster.isReplicaExplicitlySpecified()
			require.Equal(tt, tc.expected, actual,
				"isReplicaExplicitlySpecified() returned %v, expected %v",
				actual, tc.expected)
		})
	}
}
