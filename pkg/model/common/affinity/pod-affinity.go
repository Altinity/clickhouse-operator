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

package affinity

import (
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"gopkg.in/d4l3k/messagediff.v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	"github.com/altinity/clickhouse-operator/pkg/model/common/macro"
	commonLabeler "github.com/altinity/clickhouse-operator/pkg/model/common/tags/labeler"
)

// newPodAffinity
func (a *Affinity) newPodAffinity(template *api.PodTemplate) *core.PodAffinity {
	// Return podAffinity only in case something was added into it
	added := false
	podAffinity := &core.PodAffinity{}

	for i := range template.PodDistribution {
		podDistribution := &template.PodDistribution[i]
		switch podDistribution.Type {
		case deployment.PodDistributionNamespaceAffinity:
			added = true
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				newWeightedPodAffinityTermWithMatchLabels(
					1,
					podDistribution,
					map[string]string{
						commonLabeler.LabelNamespace: a.macro.Get(macro.MacrosNamespace),
					},
				),
			)
		case deployment.PodDistributionClickHouseInstallationAffinity:
			added = true
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				newWeightedPodAffinityTermWithMatchLabels(
					1,
					podDistribution,
					map[string]string{
						commonLabeler.LabelCRName: a.macro.Get(macro.MacrosCRName),
					},
				),
			)
		case deployment.PodDistributionClusterAffinity:
			added = true
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				newWeightedPodAffinityTermWithMatchLabels(
					1,
					podDistribution,
					map[string]string{
						commonLabeler.LabelClusterName: a.macro.Get(macro.MacrosClusterName),
					},
				),
			)
		case deployment.PodDistributionShardAffinity:
			added = true
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				newWeightedPodAffinityTermWithMatchLabels(
					1,
					podDistribution,
					map[string]string{
						commonLabeler.LabelShardName: a.macro.Get(macro.MacrosShardName),
					},
				),
			)
		case deployment.PodDistributionReplicaAffinity:
			added = true
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				newWeightedPodAffinityTermWithMatchLabels(
					1,
					podDistribution,
					map[string]string{
						commonLabeler.LabelReplicaName: a.macro.Get(macro.MacrosReplicaName),
					},
				),
			)
		case deployment.PodDistributionPreviousTailAffinity:
			// Newer k8s insists on Required for this Affinity
			added = true
			podAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
				podAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				newPodAffinityTermWithMatchLabels(
					podDistribution,
					map[string]string{
						commonLabeler.LabelClusterScopeIndex: a.macro.Get(macro.MacrosClusterScopeCycleHeadPointsToPreviousCycleTail),
					},
				),
			)
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				newWeightedPodAffinityTermWithMatchLabels(
					1,
					podDistribution,
					map[string]string{
						commonLabeler.LabelClusterScopeIndex: a.macro.Get(macro.MacrosClusterScopeCycleHeadPointsToPreviousCycleTail),
					},
				),
			)
		}
	}

	if added {
		// Has something to return
		return podAffinity
	}

	return nil
}

func getPodAffinityTerms(affinity *core.PodAffinity) []core.PodAffinityTerm {
	if affinity == nil {
		return nil
	}

	return affinity.RequiredDuringSchedulingIgnoredDuringExecution
}

func getPodAffinityTerm(affinity *core.PodAffinity, i int) *core.PodAffinityTerm {
	terms := getPodAffinityTerms(affinity)
	if terms == nil {
		return nil
	}
	if i >= len(terms) {
		return nil
	}
	return &terms[i]
}

func appendPodAffinityTerm(affinity *core.PodAffinity, term *core.PodAffinityTerm) *core.PodAffinity {
	if term == nil {
		return affinity
	}

	// Ensure path to terms exists
	if affinity == nil {
		affinity = &core.PodAffinity{}
	}

	affinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
		affinity.RequiredDuringSchedulingIgnoredDuringExecution,
		*term,
	)

	return affinity
}

func getWeightedPodAffinityTerms(affinity *core.PodAffinity) []core.WeightedPodAffinityTerm {
	if affinity == nil {
		return nil
	}

	return affinity.PreferredDuringSchedulingIgnoredDuringExecution
}

func getWeightedPodAffinityTerm(affinity *core.PodAffinity, i int) *core.WeightedPodAffinityTerm {
	terms := getWeightedPodAffinityTerms(affinity)
	if terms == nil {
		return nil
	}
	if i >= len(terms) {
		return nil
	}
	return &terms[i]
}

func appendWeightedPodAffinityTerm(affinity *core.PodAffinity, term *core.WeightedPodAffinityTerm) *core.PodAffinity {
	if term == nil {
		return affinity
	}

	// Ensure path to terms exists
	if affinity == nil {
		affinity = &core.PodAffinity{}
	}

	affinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
		affinity.PreferredDuringSchedulingIgnoredDuringExecution,
		*term,
	)

	return affinity
}

// mergePodAffinity
func mergePodAffinity(dst *core.PodAffinity, src *core.PodAffinity) *core.PodAffinity {
	if src == nil {
		// Nothing to merge from
		return dst
	}

	if dst == nil {
		// In case no receiver, it will be allocated by appendPodAffinityTerm() or appendWeightedPodAffinityTerm() if need be
	}

	// Merge PodAffinityTerm
	for i := range getPodAffinityTerms(src) {
		s := getPodAffinityTerm(src, i)
		equal := false
		for j := range getPodAffinityTerms(dst) {
			d := getPodAffinityTerm(dst, j)
			if _, equal = messagediff.DeepDiff(*s, *d); equal {
				break
			}
		}
		if !equal {
			dst = appendPodAffinityTerm(dst, s)
		}
	}

	// Merge WeightedPodAffinityTerm
	for i := range getWeightedPodAffinityTerms(src) {
		s := getWeightedPodAffinityTerm(src, i)
		equal := false
		for j := range getWeightedPodAffinityTerms(dst) {
			d := getWeightedPodAffinityTerm(dst, j)
			if _, equal = messagediff.DeepDiff(*s, *d); equal {
				break
			}
		}
		if !equal {
			dst = appendWeightedPodAffinityTerm(dst, s)
		}
	}

	return dst
}

// newPodAffinityTermWithMatchLabels
func newPodAffinityTermWithMatchLabels(
	podDistribution *api.PodDistribution,
	matchLabels map[string]string,
) core.PodAffinityTerm {
	return core.PodAffinityTerm{
		LabelSelector: &meta.LabelSelector{
			// A list of node selector requirements by node's labels.
			//MatchLabels: map[string]string{
			//	LabelClusterScopeCycleIndex: macrosClusterScopeCycleIndex,
			//},
			MatchLabels: matchLabels,
			// Switch to MatchLabels
			//MatchExpressions: []meta.LabelSelectorRequirement{
			//	{
			//		Key:      LabelAppName,
			//		Operator: meta.LabelSelectorOpIn,
			//		Values: []string{
			//			LabelAppValue,
			//		},
			//	},
			//},
		},
		TopologyKey: podDistribution.TopologyKey,
	}
}

// newPodAffinityTermWithMatchExpressions
func newPodAffinityTermWithMatchExpressions(
	podDistribution *api.PodDistribution,
	matchExpressions []meta.LabelSelectorRequirement,
) core.PodAffinityTerm {
	return core.PodAffinityTerm{
		LabelSelector: &meta.LabelSelector{
			// A list of node selector requirements by node's labels.
			//MatchLabels: map[string]string{
			//	LabelClusterScopeCycleIndex: macrosClusterScopeCycleIndex,
			//},
			//MatchExpressions: []meta.LabelSelectorRequirement{
			//	{
			//		Key:      LabelAppName,
			//		Operator: meta.LabelSelectorOpIn,
			//		Values: []string{
			//			LabelAppValue,
			//		},
			//	},
			//},
			MatchExpressions: matchExpressions,
		},
		TopologyKey: podDistribution.TopologyKey,
	}
}

// newWeightedPodAffinityTermWithMatchLabels is an enhanced append()
func newWeightedPodAffinityTermWithMatchLabels(
	weight int32,
	podDistribution *api.PodDistribution,
	matchLabels map[string]string,
) core.WeightedPodAffinityTerm {
	return core.WeightedPodAffinityTerm{
		Weight: weight,
		PodAffinityTerm: core.PodAffinityTerm{
			LabelSelector: &meta.LabelSelector{
				// A list of node selector requirements by node's labels.
				//MatchLabels: map[string]string{
				//	LabelClusterScopeCycleIndex: macrosClusterScopeCycleIndex,
				//},
				MatchLabels: matchLabels,
				// Switch to MatchLabels
				//MatchExpressions: []meta.LabelSelectorRequirement{
				//	{
				//		Key:      LabelAppName,
				//		Operator: meta.LabelSelectorOpIn,
				//		Values: []string{
				//			LabelAppValue,
				//		},
				//	},
				//},
			},
			TopologyKey: podDistribution.TopologyKey,
		},
	}
}
