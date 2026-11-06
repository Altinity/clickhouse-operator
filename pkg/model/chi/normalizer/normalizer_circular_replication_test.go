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
	core "k8s.io/api/core/v1"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
)

const circularReplicationPodTemplate = "pt-circular"

func normalizeCircularReplicationCR(t *testing.T, distribution chi.PodDistribution) *chi.ClickHouseInstallation {
	t.Helper()

	cr := &chi.ClickHouseInstallation{}
	cr.Name = "circular"
	cr.Namespace = precedenceNamespace
	cr.Spec.Defaults = &chi.Defaults{Templates: &chi.TemplatesList{PodTemplate: circularReplicationPodTemplate}}
	cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{{
		Name:   "c",
		Layout: &chi.ChiClusterLayout{ShardsCount: 3, ReplicasCount: 3},
	}}}
	cr.Spec.Templates = &chi.Templates{PodTemplates: []chi.PodTemplate{{
		Name:            circularReplicationPodTemplate,
		PodDistribution: []chi.PodDistribution{distribution},
	}}}

	got, err := New(nil).CreateTemplated(cr, commonNormalizer.NewOptions[chi.ClickHouseInstallation]())
	require.NoError(t, err)
	return got
}

// TestCircularReplication checks that CircularReplication shortcut is expanded into fully normalized distributions
// and takes part in host scope cycles calculation
func TestCircularReplication(t *testing.T) {
	const (
		shards     = 3
		replicas   = 3
		hostsCount = shards * replicas
	)

	tests := []struct {
		name            string
		distribution    chi.PodDistribution
		wantTopologyKey string
	}{
		{
			name:            "default topology key",
			distribution:    chi.PodDistribution{Type: deployment.PodDistributionCircularReplication},
			wantTopologyKey: core.LabelHostname,
		},
		{
			name: "explicit topology key",
			distribution: chi.PodDistribution{
				Type:        deployment.PodDistributionCircularReplication,
				TopologyKey: core.LabelTopologyZone,
			},
			wantTopologyKey: core.LabelTopologyZone,
		},
		{
			name:            "type casing",
			distribution:    chi.PodDistribution{Type: "circularreplication"},
			wantTopologyKey: core.LabelHostname,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeCircularReplicationCR(t, tt.distribution)

			template, ok := got.GetPodTemplate(circularReplicationPodTemplate)
			require.True(t, ok)

			// Shortcut itself and all its expansions are normalized
			distributionTypes := map[string]int{}
			for _, podDistribution := range template.PodDistribution {
				distributionTypes[podDistribution.Type]++
				require.Equal(t, tt.wantTopologyKey, podDistribution.TopologyKey, "distribution %q", podDistribution.Type)
				if podDistribution.Type == deployment.PodDistributionMaxNumberPerNode {
					require.Equal(t, replicas, podDistribution.Number)
				}
			}
			require.Len(t, template.PodDistribution, 8)
			require.Equal(t, 1, distributionTypes[deployment.PodDistributionCircularReplication])
			require.Equal(t, 1, distributionTypes[deployment.PodDistributionShardAntiAffinity])
			require.Equal(t, 1, distributionTypes[deployment.PodDistributionReplicaAntiAffinity])
			require.Equal(t, 1, distributionTypes[deployment.PodDistributionMaxNumberPerNode])
			require.Equal(t, 1, distributionTypes[deployment.PodDistributionPreviousTailAffinity])

			// Generated pod affinity is valid: every term has topology key
			require.NotNil(t, template.Spec.Affinity)
			podAffinity := template.Spec.Affinity.PodAffinity
			require.NotNil(t, podAffinity)
			require.NotEmpty(t, podAffinity.RequiredDuringSchedulingIgnoredDuringExecution)
			for _, term := range podAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
				require.Equal(t, tt.wantTopologyKey, term.TopologyKey)
			}
			require.NotEmpty(t, podAffinity.PreferredDuringSchedulingIgnoredDuringExecution)
			for _, term := range podAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
				require.Equal(t, tt.wantTopologyKey, term.PodAffinityTerm.TopologyKey)
			}
			podAntiAffinity := template.Spec.Affinity.PodAntiAffinity
			require.NotNil(t, podAntiAffinity)
			require.NotEmpty(t, podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution)
			for _, term := range podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
				require.Equal(t, tt.wantTopologyKey, term.TopologyKey)
			}

			// Host cycles: max number per node equals to replicas count, thus 'replicas' cycles of 'shards' hosts
			const wantCycleSize = hostsCount / replicas
			i := 0
			got.WalkHosts(func(host *chi.Host) error {
				address := host.Runtime.Address
				require.Equal(t, wantCycleSize, address.ClusterScopeCycleSize, "host %d cycle size", i)
				require.Equal(t, i/wantCycleSize, address.ClusterScopeCycleIndex, "host %d cycle index", i)
				require.Equal(t, i%wantCycleSize, address.ClusterScopeCycleOffset, "host %d cycle offset", i)
				i++
				return nil
			})
			require.Equal(t, hostsCount, i)
		})
	}
}
