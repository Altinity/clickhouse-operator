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
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
)

const maxNumberPerNodePodTemplate = "pt-max-number"

func maxNumberPerNodePodTemplates(number int) []chi.PodTemplate {
	return []chi.PodTemplate{{
		Name: maxNumberPerNodePodTemplate,
		PodDistribution: []chi.PodDistribution{{
			Type:   deployment.PodDistributionMaxNumberPerNode,
			Number: number,
		}},
	}}
}

// requireMaxNumberPerNodeCycles checks the cluster scope cycles of 3 shards x 2 replicas hosts
// normalized with MaxNumberPerNode = 2: 6 hosts in 2 cycles of 3 hosts each
func requireMaxNumberPerNodeCycles(t *testing.T, cr *chi.ClickHouseInstallation) {
	t.Helper()

	const (
		number        = 2
		hostsCount    = 6
		wantCycleSize = 3 // ceil(hostsCount / number)
	)

	perOffset := map[int]int{}
	i := 0
	cr.WalkHosts(func(host *chi.Host) error {
		address := host.Runtime.Address
		require.Equal(t, wantCycleSize, address.ClusterScopeCycleSize, "host %d cycle size", i)
		require.Equal(t, i/wantCycleSize, address.ClusterScopeCycleIndex, "host %d cycle index", i)
		require.Equal(t, i%wantCycleSize, address.ClusterScopeCycleOffset, "host %d cycle offset", i)
		perOffset[address.ClusterScopeCycleOffset]++
		i++
		return nil
	})
	require.Equal(t, hostsCount, i)

	// Hosts of the same offset are placed on the same node, no more than 'number' of them are allowed
	for offset, hosts := range perOffset {
		require.LessOrEqual(t, hosts, number, "hosts sharing offset %d", offset)
	}
}

func newMaxNumberPerNodeCR() *chi.ClickHouseInstallation {
	cr := &chi.ClickHouseInstallation{}
	cr.Name = "mpn"
	cr.Namespace = precedenceNamespace
	cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{{
		Name:   "c",
		Layout: &chi.ChiClusterLayout{ShardsCount: 3, ReplicasCount: 2},
	}}}
	return cr
}

// TestMaxNumberPerNodeScopeCycles checks that podDistribution MaxNumberPerNode reaches host scope cycles:
// cycles are calculated before the pod templates are normalized and indexed.
func TestMaxNumberPerNodeScopeCycles(t *testing.T) {
	t.Run("pod template of the CHI", func(t *testing.T) {
		cr := newMaxNumberPerNodeCR()
		cr.Spec.Defaults = &chi.Defaults{Templates: &chi.TemplatesList{PodTemplate: maxNumberPerNodePodTemplate}}
		cr.Spec.Templates = &chi.Templates{PodTemplates: maxNumberPerNodePodTemplates(2)}

		got, err := New(nil).CreateTemplated(cr, commonNormalizer.NewOptions[chi.ClickHouseInstallation]())
		require.NoError(t, err)
		requireMaxNumberPerNodeCycles(t, got)
	})

	t.Run("pod template of the CHIT", func(t *testing.T) {
		chit := newCHIT("chit-max-number", chi.TemplatingPolicyManual)
		chit.Spec.Defaults = &chi.Defaults{Templates: &chi.TemplatesList{PodTemplate: maxNumberPerNodePodTemplate}}
		chit.Spec.Templates = &chi.Templates{PodTemplates: maxNumberPerNodePodTemplates(2)}
		registerCHIT(t, chit)

		cr := newMaxNumberPerNodeCR()
		cr.Spec.UseTemplates = []*chi.TemplateRef{{Name: chit.Name, Namespace: chit.Namespace}}

		got, err := New(nil).CreateTemplated(cr, commonNormalizer.NewOptions[chi.ClickHouseInstallation]())
		require.NoError(t, err)
		requireMaxNumberPerNodeCycles(t, got)
	})
}
