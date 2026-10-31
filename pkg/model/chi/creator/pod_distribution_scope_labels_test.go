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

package creator_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sLabels "k8s.io/apimachinery/pkg/labels"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	"github.com/altinity/clickhouse-operator/pkg/chop"
)

const (
	// Scope labels as they are stamped on pods, see labeler.GetHostScope
	labelClusterScopeIndex      = "clickhouse.altinity.com/clusterScopeIndex"
	labelClusterScopeCycleIndex = "clickhouse.altinity.com/clusterScopeCycleIndex"

	scopeShards   = 3
	scopeReplicas = 3
	scopeHosts    = scopeShards * scopeReplicas
	scopeTemplate = "pt-scope"
)

// withAppendScope switches operator config label.appendScope for the duration of the test.
// Labeler reads the config on construction, so the creator has to be built after that.
func withAppendScope(t *testing.T, appendScope bool) {
	t.Helper()

	previous := chop.Config().Label.Runtime.AppendScope
	chop.Config().Label.Runtime.AppendScope = appendScope
	t.Cleanup(func() { chop.Config().Label.Runtime.AppendScope = previous })
}

// scopePod is what the operator creates for the host: labels of the pod and its affinity
type scopePod struct {
	host     *chi.Host
	labels   map[string]string
	affinity *core.Affinity
}

// createScopePods normalizes 3 shards x 3 replicas cluster with the distribution and creates
// StatefulSet for every host the way the controller does
func createScopePods(t *testing.T, distribution chi.PodDistribution) []scopePod {
	t.Helper()

	cr := &chi.ClickHouseInstallation{}
	cr.Name = "scope"
	cr.Namespace = "ns"
	cr.Spec.Defaults = &chi.Defaults{Templates: &chi.TemplatesList{PodTemplate: scopeTemplate}}
	cr.Spec.Templates = &chi.Templates{PodTemplates: []chi.PodTemplate{{
		Name:            scopeTemplate,
		PodDistribution: []chi.PodDistribution{distribution},
	}}}
	cr.Spec.Configuration = &chi.Configuration{Clusters: []*chi.Cluster{{
		Name:   "c",
		Layout: &chi.ChiClusterLayout{ShardsCount: scopeShards, ReplicasCount: scopeReplicas},
	}}}
	cr = normalize(t, cr)

	creator := buildCreator(cr)
	var pods []scopePod
	cr.WalkHosts(func(host *chi.Host) error {
		sts := creator.CreateStatefulSet(host, false)
		require.NotNil(t, sts)
		pods = append(pods, scopePod{
			host:     host,
			labels:   sts.Spec.Template.Labels,
			affinity: sts.Spec.Template.Spec.Affinity,
		})
		return nil
	})
	require.Len(t, pods, scopeHosts)
	return pods
}

// selects evaluates label selector of the affinity term against labels of the pod
func selects(t *testing.T, term core.PodAffinityTerm, pod scopePod) bool {
	t.Helper()

	require.NotNil(t, term.LabelSelector)
	selector, err := meta.LabelSelectorAsSelector(term.LabelSelector)
	require.NoError(t, err)
	return selector.Matches(k8sLabels.Set(pod.labels))
}

// termWithLabel finds the only term selecting by the label
func termWithLabel(t *testing.T, terms []core.PodAffinityTerm, label string) core.PodAffinityTerm {
	t.Helper()

	var found []core.PodAffinityTerm
	for _, term := range terms {
		if term.LabelSelector != nil {
			if _, ok := term.LabelSelector.MatchLabels[label]; ok {
				found = append(found, term)
			}
		}
	}
	require.Len(t, found, 1, "terms selecting by %q", label)
	return found[0]
}

// TestPodDistributionScopeLabels checks that pod distributions that select pods by scope labels
// are satisfiable by the labels the operator stamps on pods. Scope labels are stamped
// only with operator config label.appendScope = yes.
func TestPodDistributionScopeLabels(t *testing.T) {
	maxNumberPerNode := chi.PodDistribution{
		Type:        deployment.PodDistributionMaxNumberPerNode,
		Number:      scopeReplicas,
		TopologyKey: core.LabelHostname,
	}
	circularReplication := chi.PodDistribution{Type: deployment.PodDistributionCircularReplication}

	t.Run("without appendScope scope labels are absent", func(t *testing.T) {
		withAppendScope(t, false)
		pods := createScopePods(t, maxNumberPerNode)
		for _, pod := range pods {
			require.NotContains(t, pod.labels, labelClusterScopeIndex)
			require.NotContains(t, pod.labels, labelClusterScopeCycleIndex)

			// Thus MaxNumberPerNode selector, which requires the label, selects no pod at all
			terms := pod.affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
			term := termWithLabel(t, terms, labelClusterScopeCycleIndex)
			for _, other := range pods {
				require.False(t, selects(t, term, other))
			}
		}
	})

	t.Run("with appendScope pods carry scope labels", func(t *testing.T) {
		withAppendScope(t, true)
		for i, pod := range createScopePods(t, maxNumberPerNode) {
			address := pod.host.Runtime.Address
			require.Equal(t, i, address.ClusterScopeIndex)
			require.Equal(t, strconv.Itoa(address.ClusterScopeIndex), pod.labels[labelClusterScopeIndex], "host %d", i)
			require.Equal(t, strconv.Itoa(address.ClusterScopeCycleIndex), pod.labels[labelClusterScopeCycleIndex], "host %d", i)
		}
	})

	t.Run("MaxNumberPerNode selects hosts of the same cycle only", func(t *testing.T) {
		withAppendScope(t, true)
		pods := createScopePods(t, maxNumberPerNode)

		const wantCycleSize = scopeHosts / scopeReplicas // ceil(hosts / number)
		for i, pod := range pods {
			require.NotNil(t, pod.affinity, "host %d", i)
			require.NotNil(t, pod.affinity.PodAntiAffinity, "host %d", i)
			terms := pod.affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
			term := termWithLabel(t, terms, labelClusterScopeCycleIndex)
			require.Equal(t, core.LabelHostname, term.TopologyKey)

			// Selector is the host's own cycle, expanded from macros
			require.Equal(t, map[string]string{labelClusterScopeCycleIndex: pod.labels[labelClusterScopeCycleIndex]}, term.LabelSelector.MatchLabels, "host %d", i)

			// It matches all the hosts of the cycle (itself included) and none of other cycles, so hosts of
			// one cycle never share a node and a node holds no more than one host per cycle: 'number' at most
			for j, other := range pods {
				sameCycle := j/wantCycleSize == i/wantCycleSize
				require.Equal(t, sameCycle, selects(t, term, other), "host %d vs host %d", i, j)
			}
		}
	})

	t.Run("CircularReplication terms are valid and satisfiable", func(t *testing.T) {
		withAppendScope(t, true)
		pods := createScopePods(t, circularReplication)

		for i, pod := range pods {
			require.NotNil(t, pod.affinity, "host %d", i)

			var required []core.PodAffinityTerm
			if pod.affinity.PodAffinity != nil {
				required = append(required, pod.affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution...)
				for _, weighted := range pod.affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
					require.NotEmpty(t, weighted.PodAffinityTerm.TopologyKey, "host %d", i)
				}
			}
			require.NotNil(t, pod.affinity.PodAntiAffinity, "host %d", i)
			required = append(required, pod.affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution...)
			for _, term := range required {
				require.NotEmpty(t, term.TopologyKey, "host %d", i)
			}

			// PreviousTailAffinity selects the previous cycle tail for a cycle head and the host itself otherwise
			require.NotNil(t, pod.affinity.PodAffinity, "host %d", i)
			term := termWithLabel(t, pod.affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution, labelClusterScopeIndex)
			wantIndex := i
			if (pod.host.Runtime.Address.ClusterScopeCycleOffset == 0) && (i > 0) {
				wantIndex = i - 1
			}
			for j, other := range pods {
				require.Equal(t, j == wantIndex, selects(t, term, other), "host %d vs host %d", i, j)
			}
		}
	})
}
