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
	"gopkg.in/d4l3k/messagediff.v1"
	"k8s.io/api/core/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	chiV1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// newAffinity creates new Affinity struct
func newAffinity(template *chiV1.ChiPodTemplate) *v1.Affinity {
	// Pod node affinity scheduling rules.
	nodeAffinity := newNodeAffinity(template)
	// Pod affinity scheduling rules. Ex.: co-locate this pod in the same node, zone, etc
	podAffinity := newPodAffinity(template)
	// Pod anti-affinity scheduling rules. Ex.: avoid putting this pod in the same node, zone, etc
	podAntiAffinity := newPodAntiAffinity(template)

	// At least one affinity has to be reasonable
	if (nodeAffinity == nil) && (podAffinity == nil) && (podAntiAffinity == nil) {
		// Neither Affinity nor AntiAffinity specified
		return nil
	}

	return &v1.Affinity{
		NodeAffinity:    nodeAffinity,
		PodAffinity:     podAffinity,
		PodAntiAffinity: podAntiAffinity,
	}
}

// mergeAffinity merges from src into dst and returns dst
func mergeAffinity(dst *v1.Affinity, src *v1.Affinity) *v1.Affinity {
	if src == nil {
		// Nothing to merge from
		return dst
	}

	created := false
	if dst == nil {
		// No receiver specified, allocate a new one
		dst = &v1.Affinity{}
		created = true
	}

	dst.NodeAffinity = mergeNodeAffinity(dst.NodeAffinity, src.NodeAffinity)
	dst.PodAffinity = mergePodAffinity(dst.PodAffinity, src.PodAffinity)
	dst.PodAntiAffinity = mergePodAntiAffinity(dst.PodAntiAffinity, src.PodAntiAffinity)

	empty := (dst.NodeAffinity == nil) && (dst.PodAffinity == nil) && (dst.PodAntiAffinity == nil)
	if created && empty {
		// Do not return empty and internally created dst
		return nil
	}

	return dst
}

// newNodeAffinity
func newNodeAffinity(template *chiV1.ChiPodTemplate) *v1.NodeAffinity {
	if template.Zone.Key == "" {
		return nil
	}

	return &v1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
			NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					// A list of node selector requirements by node's labels.
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      template.Zone.Key,
							Operator: v1.NodeSelectorOpIn,
							Values:   template.Zone.Values,
						},
					},
					// A list of node selector requirements by node's fields.
					//MatchFields: []v1.NodeSelectorRequirement{
					//	v1.NodeSelectorRequirement{},
					//},
				},
			},
		},

		// PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{},
	}
}

func getNodeSelectorTerms(affinity *v1.NodeAffinity) []v1.NodeSelectorTerm {
	if affinity == nil {
		return nil
	}

	if affinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return nil
	}
	return affinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
}

func getNodeSelectorTerm(affinity *v1.NodeAffinity, i int) *v1.NodeSelectorTerm {
	terms := getNodeSelectorTerms(affinity)
	if terms == nil {
		return nil
	}
	if i >= len(terms) {
		return nil
	}
	return &terms[i]
}

func appendNodeSelectorTerm(affinity *v1.NodeAffinity, term *v1.NodeSelectorTerm) *v1.NodeAffinity {
	if term == nil {
		return affinity
	}

	// Ensure path to terms exists
	if affinity == nil {
		affinity = &v1.NodeAffinity{}
	}
	if affinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		affinity.RequiredDuringSchedulingIgnoredDuringExecution = &v1.NodeSelector{}
	}

	affinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = append(
		affinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms,
		*term,
	)

	return affinity
}

func getPreferredSchedulingTerms(affinity *v1.NodeAffinity) []v1.PreferredSchedulingTerm {
	if affinity == nil {
		return nil
	}

	return affinity.PreferredDuringSchedulingIgnoredDuringExecution
}

func getPreferredSchedulingTerm(affinity *v1.NodeAffinity, i int) *v1.PreferredSchedulingTerm {
	terms := getPreferredSchedulingTerms(affinity)
	if terms == nil {
		return nil
	}
	if i >= len(terms) {
		return nil
	}
	return &terms[i]
}

func appendPreferredSchedulingTerm(affinity *v1.NodeAffinity, term *v1.PreferredSchedulingTerm) *v1.NodeAffinity {
	if term == nil {
		return affinity
	}

	// Ensure path to terms exists
	if affinity == nil {
		affinity = &v1.NodeAffinity{}
	}

	affinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
		affinity.PreferredDuringSchedulingIgnoredDuringExecution,
		*term,
	)

	return affinity
}

// mergeNodeAffinity
func mergeNodeAffinity(dst *v1.NodeAffinity, src *v1.NodeAffinity) *v1.NodeAffinity {
	if src == nil {
		// Nothing to merge from
		return dst
	}

	if dst == nil {
		// In case no receiver, it will be allocated by appendNodeSelectorTerm() or appendPreferredSchedulingTerm() if need be
	}

	// Merge NodeSelectors
	for i := range getNodeSelectorTerms(src) {
		s := getNodeSelectorTerm(src, i)
		equal := false
		for j := range getNodeSelectorTerms(dst) {
			d := getNodeSelectorTerm(dst, j)
			if _, equal = messagediff.DeepDiff(*s, *d); equal {
				break
			}
		}
		if !equal {
			dst = appendNodeSelectorTerm(dst, s)
		}
	}

	// Merge PreferredSchedulingTerm
	for i := range getPreferredSchedulingTerms(src) {
		s := getPreferredSchedulingTerm(src, i)
		equal := false
		for j := range getPreferredSchedulingTerms(dst) {
			d := getPreferredSchedulingTerm(dst, j)
			if _, equal = messagediff.DeepDiff(*s, *d); equal {
				break
			}
		}
		if !equal {
			dst = appendPreferredSchedulingTerm(dst, s)
		}
	}

	return dst
}

// newPodAffinity
func newPodAffinity(template *chiV1.ChiPodTemplate) *v1.PodAffinity {
	// Return podAffinity only in case something was added into it
	added := false
	podAffinity := &v1.PodAffinity{}

	for i := range template.PodDistribution {
		podDistribution := &template.PodDistribution[i]
		switch podDistribution.Type {
		case chiV1.PodDistributionNamespaceAffinity:
			added = true
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				newWeightedPodAffinityTermWithMatchLabels(
					1,
					podDistribution,
					map[string]string{
						LabelNamespace: macrosNamespace,
					},
				),
			)
		case chiV1.PodDistributionClickHouseInstallationAffinity:
			added = true
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				newWeightedPodAffinityTermWithMatchLabels(
					1,
					podDistribution,
					map[string]string{
						LabelCHIName: macrosChiName,
					},
				),
			)
		case chiV1.PodDistributionClusterAffinity:
			added = true
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				newWeightedPodAffinityTermWithMatchLabels(
					1,
					podDistribution,
					map[string]string{
						LabelClusterName: macrosClusterName,
					},
				),
			)
		case chiV1.PodDistributionShardAffinity:
			added = true
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				newWeightedPodAffinityTermWithMatchLabels(
					1,
					podDistribution,
					map[string]string{
						LabelShardName: macrosShardName,
					},
				),
			)
		case chiV1.PodDistributionReplicaAffinity:
			added = true
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				newWeightedPodAffinityTermWithMatchLabels(
					1,
					podDistribution,
					map[string]string{
						LabelReplicaName: macrosReplicaName,
					},
				),
			)
		case chiV1.PodDistributionPreviousTailAffinity:
			// Newer k8s insists on Required for this Affinity
			added = true
			podAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
				podAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				newPodAffinityTermWithMatchLabels(
					podDistribution,
					map[string]string{
						LabelClusterScopeIndex: macrosClusterScopeCycleHeadPointsToPreviousCycleTail,
					},
				),
			)
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				newWeightedPodAffinityTermWithMatchLabels(
					1,
					podDistribution,
					map[string]string{
						LabelClusterScopeIndex: macrosClusterScopeCycleHeadPointsToPreviousCycleTail,
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

func getPodAffinityTerms(affinity *v1.PodAffinity) []v1.PodAffinityTerm {
	if affinity == nil {
		return nil
	}

	return affinity.RequiredDuringSchedulingIgnoredDuringExecution
}

func getPodAffinityTerm(affinity *v1.PodAffinity, i int) *v1.PodAffinityTerm {
	terms := getPodAffinityTerms(affinity)
	if terms == nil {
		return nil
	}
	if i >= len(terms) {
		return nil
	}
	return &terms[i]
}

func appendPodAffinityTerm(affinity *v1.PodAffinity, term *v1.PodAffinityTerm) *v1.PodAffinity {
	if term == nil {
		return affinity
	}

	// Ensure path to terms exists
	if affinity == nil {
		affinity = &v1.PodAffinity{}
	}

	affinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
		affinity.RequiredDuringSchedulingIgnoredDuringExecution,
		*term,
	)

	return affinity
}

func getWeightedPodAffinityTerms(affinity *v1.PodAffinity) []v1.WeightedPodAffinityTerm {
	if affinity == nil {
		return nil
	}

	return affinity.PreferredDuringSchedulingIgnoredDuringExecution
}

func getWeightedPodAffinityTerm(affinity *v1.PodAffinity, i int) *v1.WeightedPodAffinityTerm {
	terms := getWeightedPodAffinityTerms(affinity)
	if terms == nil {
		return nil
	}
	if i >= len(terms) {
		return nil
	}
	return &terms[i]
}

func appendWeightedPodAffinityTerm(affinity *v1.PodAffinity, term *v1.WeightedPodAffinityTerm) *v1.PodAffinity {
	if term == nil {
		return affinity
	}

	// Ensure path to terms exists
	if affinity == nil {
		affinity = &v1.PodAffinity{}
	}

	affinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
		affinity.PreferredDuringSchedulingIgnoredDuringExecution,
		*term,
	)

	return affinity
}

// mergePodAffinity
func mergePodAffinity(dst *v1.PodAffinity, src *v1.PodAffinity) *v1.PodAffinity {
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

// newMatchLabels
func newMatchLabels(
	podDistribution *chiV1.ChiPodDistribution,
	matchLabels map[string]string,
) map[string]string {
	var scopeLabels map[string]string

	switch podDistribution.Scope {
	case chiV1.PodDistributionScopeShard:
		scopeLabels = map[string]string{
			LabelNamespace:   macrosNamespace,
			LabelCHIName:     macrosChiName,
			LabelClusterName: macrosClusterName,
			LabelShardName:   macrosShardName,
		}
	case chiV1.PodDistributionScopeReplica:
		scopeLabels = map[string]string{
			LabelNamespace:   macrosNamespace,
			LabelCHIName:     macrosChiName,
			LabelClusterName: macrosClusterName,
			LabelReplicaName: macrosReplicaName,
		}
	case chiV1.PodDistributionScopeCluster:
		scopeLabels = map[string]string{
			LabelNamespace:   macrosNamespace,
			LabelCHIName:     macrosChiName,
			LabelClusterName: macrosClusterName,
		}
	case chiV1.PodDistributionScopeClickHouseInstallation:
		scopeLabels = map[string]string{
			LabelNamespace: macrosNamespace,
			LabelCHIName:   macrosChiName,
		}
	case chiV1.PodDistributionScopeNamespace:
		scopeLabels = map[string]string{
			LabelNamespace: macrosNamespace,
		}
	case chiV1.PodDistributionScopeGlobal:
		scopeLabels = map[string]string{}
	}

	return util.MergeStringMapsOverwrite(matchLabels, scopeLabels)
}

// newPodAntiAffinity
func newPodAntiAffinity(template *chiV1.ChiPodTemplate) *v1.PodAntiAffinity {
	// Return podAntiAffinity only in case something was added into it
	added := false
	podAntiAffinity := &v1.PodAntiAffinity{}

	// PodDistribution
	for i := range template.PodDistribution {
		podDistribution := &template.PodDistribution[i]
		switch podDistribution.Type {
		case chiV1.PodDistributionClickHouseAntiAffinity:
			added = true
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				newPodAffinityTermWithMatchLabels(
					podDistribution,
					newMatchLabels(
						podDistribution,
						map[string]string{
							LabelAppName: LabelAppValue,
						},
					),
				),
			)
		case chiV1.PodDistributionMaxNumberPerNode:
			added = true
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				newPodAffinityTermWithMatchLabels(
					podDistribution,
					newMatchLabels(
						podDistribution,
						map[string]string{
							LabelClusterScopeCycleIndex: macrosClusterScopeCycleIndex,
						},
					),
				),
			)
		case chiV1.PodDistributionShardAntiAffinity:
			added = true
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				newPodAffinityTermWithMatchLabels(
					podDistribution,
					newMatchLabels(
						podDistribution,
						map[string]string{
							LabelShardName: macrosShardName,
						},
					),
				),
			)
		case chiV1.PodDistributionReplicaAntiAffinity:
			added = true
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				newPodAffinityTermWithMatchLabels(
					podDistribution,
					newMatchLabels(
						podDistribution,
						map[string]string{
							LabelReplicaName: macrosReplicaName,
						},
					),
				),
			)
		case chiV1.PodDistributionAnotherNamespaceAntiAffinity:
			added = true
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				newPodAffinityTermWithMatchExpressions(
					podDistribution,
					[]metaV1.LabelSelectorRequirement{
						{
							Key:      LabelNamespace,
							Operator: metaV1.LabelSelectorOpNotIn,
							Values: []string{
								macrosNamespace,
							},
						},
					},
				),
			)
		case chiV1.PodDistributionAnotherClickHouseInstallationAntiAffinity:
			added = true
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				newPodAffinityTermWithMatchExpressions(
					podDistribution,
					[]metaV1.LabelSelectorRequirement{
						{
							Key:      LabelCHIName,
							Operator: metaV1.LabelSelectorOpNotIn,
							Values: []string{
								macrosChiName,
							},
						},
					},
				),
			)
		case chiV1.PodDistributionAnotherClusterAntiAffinity:
			added = true
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				newPodAffinityTermWithMatchExpressions(
					podDistribution,
					[]metaV1.LabelSelectorRequirement{
						{
							Key:      LabelClusterName,
							Operator: metaV1.LabelSelectorOpNotIn,
							Values: []string{
								macrosClusterName,
							},
						},
					},
				),
			)
		}
	}

	if added {
		// Has something to return
		return podAntiAffinity
	}

	return nil
}

func getPodAntiAffinityTerms(affinity *v1.PodAntiAffinity) []v1.PodAffinityTerm {
	if affinity == nil {
		return nil
	}

	return affinity.RequiredDuringSchedulingIgnoredDuringExecution
}

func getPodAntiAffinityTerm(affinity *v1.PodAntiAffinity, i int) *v1.PodAffinityTerm {
	terms := getPodAntiAffinityTerms(affinity)
	if terms == nil {
		return nil
	}
	if i >= len(terms) {
		return nil
	}
	return &terms[i]
}

func appendPodAntiAffinityTerm(affinity *v1.PodAntiAffinity, term *v1.PodAffinityTerm) *v1.PodAntiAffinity {
	if term == nil {
		return affinity
	}

	// Ensure path to terms exists
	if affinity == nil {
		affinity = &v1.PodAntiAffinity{}
	}

	affinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
		affinity.RequiredDuringSchedulingIgnoredDuringExecution,
		*term,
	)

	return affinity
}

func getWeightedPodAntiAffinityTerms(affinity *v1.PodAntiAffinity) []v1.WeightedPodAffinityTerm {
	if affinity == nil {
		return nil
	}

	return affinity.PreferredDuringSchedulingIgnoredDuringExecution
}

func getWeightedPodAntiAffinityTerm(affinity *v1.PodAntiAffinity, i int) *v1.WeightedPodAffinityTerm {
	terms := getWeightedPodAntiAffinityTerms(affinity)
	if terms == nil {
		return nil
	}
	if i >= len(terms) {
		return nil
	}
	return &terms[i]
}

func appendWeightedPodAntiAffinityTerm(affinity *v1.PodAntiAffinity, term *v1.WeightedPodAffinityTerm) *v1.PodAntiAffinity {
	if term == nil {
		return affinity
	}

	// Ensure path to terms exists
	if affinity == nil {
		affinity = &v1.PodAntiAffinity{}
	}

	affinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
		affinity.PreferredDuringSchedulingIgnoredDuringExecution,
		*term,
	)

	return affinity
}

// mergePodAntiAffinity
func mergePodAntiAffinity(dst *v1.PodAntiAffinity, src *v1.PodAntiAffinity) *v1.PodAntiAffinity {
	if src == nil {
		// Nothing to merge from
		return dst
	}

	if dst == nil {
		// In case no receiver, it will be allocated by appendPodAntiAffinityTerm() or appendWeightedPodAntiAffinityTerm() if need be
	}

	// Merge PodAffinityTerm
	for i := range getPodAntiAffinityTerms(src) {
		s := getPodAntiAffinityTerm(src, i)
		equal := false
		for j := range getPodAntiAffinityTerms(dst) {
			d := getPodAntiAffinityTerm(dst, j)
			if _, equal = messagediff.DeepDiff(*s, *d); equal {
				break
			}
		}
		if !equal {
			dst = appendPodAntiAffinityTerm(dst, s)
		}
	}

	// Merge WeightedPodAffinityTerm
	for i := range getWeightedPodAntiAffinityTerms(src) {
		s := getWeightedPodAntiAffinityTerm(src, i)
		equal := false
		for j := range getWeightedPodAntiAffinityTerms(dst) {
			d := getWeightedPodAntiAffinityTerm(dst, j)
			if _, equal = messagediff.DeepDiff(*s, *d); equal {
				break
			}
		}
		if !equal {
			dst = appendWeightedPodAntiAffinityTerm(dst, s)
		}
	}

	return dst
}

// newPodAffinityTermWithMatchLabels
func newPodAffinityTermWithMatchLabels(
	podDistribution *chiV1.ChiPodDistribution,
	matchLabels map[string]string,
) v1.PodAffinityTerm {
	return v1.PodAffinityTerm{
		LabelSelector: &metaV1.LabelSelector{
			// A list of node selector requirements by node's labels.
			//MatchLabels: map[string]string{
			//	LabelClusterScopeCycleIndex: macrosClusterScopeCycleIndex,
			//},
			MatchLabels: matchLabels,
			// Switch to MatchLabels
			//MatchExpressions: []metaV1.LabelSelectorRequirement{
			//	{
			//		Key:      LabelAppName,
			//		Operator: metaV1.LabelSelectorOpIn,
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
	podDistribution *chiV1.ChiPodDistribution,
	matchExpressions []metaV1.LabelSelectorRequirement,
) v1.PodAffinityTerm {
	return v1.PodAffinityTerm{
		LabelSelector: &metaV1.LabelSelector{
			// A list of node selector requirements by node's labels.
			//MatchLabels: map[string]string{
			//	LabelClusterScopeCycleIndex: macrosClusterScopeCycleIndex,
			//},
			//MatchExpressions: []metaV1.LabelSelectorRequirement{
			//	{
			//		Key:      LabelAppName,
			//		Operator: metaV1.LabelSelectorOpIn,
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
	podDistribution *chiV1.ChiPodDistribution,
	matchLabels map[string]string,
) v1.WeightedPodAffinityTerm {
	return v1.WeightedPodAffinityTerm{
		Weight: weight,
		PodAffinityTerm: v1.PodAffinityTerm{
			LabelSelector: &metaV1.LabelSelector{
				// A list of node selector requirements by node's labels.
				//MatchLabels: map[string]string{
				//	LabelClusterScopeCycleIndex: macrosClusterScopeCycleIndex,
				//},
				MatchLabels: matchLabels,
				// Switch to MatchLabels
				//MatchExpressions: []metaV1.LabelSelectorRequirement{
				//	{
				//		Key:      LabelAppName,
				//		Operator: metaV1.LabelSelectorOpIn,
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

// prepareAffinity
func prepareAffinity(podTemplate *chiV1.ChiPodTemplate, host *chiV1.ChiHost) {
	switch {
	case podTemplate == nil:
		return
	case podTemplate.Spec.Affinity == nil:
		return
	}

	// Walk over all affinity fields

	if podTemplate.Spec.Affinity.NodeAffinity != nil {
		processNodeSelector(podTemplate.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution, host)
		processPreferredSchedulingTerms(podTemplate.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution, host)
	}

	if podTemplate.Spec.Affinity.PodAffinity != nil {
		processPodAffinityTerms(podTemplate.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution, host)
		processWeightedPodAffinityTerms(podTemplate.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution, host)
	}

	if podTemplate.Spec.Affinity.PodAntiAffinity != nil {
		processPodAffinityTerms(podTemplate.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, host)
		processWeightedPodAffinityTerms(podTemplate.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, host)
	}
}

// processNodeSelector
func processNodeSelector(nodeSelector *v1.NodeSelector, host *chiV1.ChiHost) {
	if nodeSelector == nil {
		return
	}
	for i := range nodeSelector.NodeSelectorTerms {
		nodeSelectorTerm := &nodeSelector.NodeSelectorTerms[i]
		processNodeSelectorTerm(nodeSelectorTerm, host)
	}
}

// processPreferredSchedulingTerms
func processPreferredSchedulingTerms(preferredSchedulingTerms []v1.PreferredSchedulingTerm, host *chiV1.ChiHost) {
	for i := range preferredSchedulingTerms {
		nodeSelectorTerm := &preferredSchedulingTerms[i].Preference
		processNodeSelectorTerm(nodeSelectorTerm, host)
	}
}

// processNodeSelectorTerm
func processNodeSelectorTerm(nodeSelectorTerm *v1.NodeSelectorTerm, host *chiV1.ChiHost) {
	for i := range nodeSelectorTerm.MatchExpressions {
		nodeSelectorRequirement := &nodeSelectorTerm.MatchExpressions[i]
		processNodeSelectorRequirement(nodeSelectorRequirement, host)
	}

	for i := range nodeSelectorTerm.MatchFields {
		nodeSelectorRequirement := &nodeSelectorTerm.MatchFields[i]
		processNodeSelectorRequirement(nodeSelectorRequirement, host)
	}
}

// processNodeSelectorRequirement
func processNodeSelectorRequirement(nodeSelectorRequirement *v1.NodeSelectorRequirement, host *chiV1.ChiHost) {
	if nodeSelectorRequirement == nil {
		return
	}
	nodeSelectorRequirement.Key = macro(host).Line(nodeSelectorRequirement.Key)
	// Update values only, keys are not macros-ed
	for i := range nodeSelectorRequirement.Values {
		nodeSelectorRequirement.Values[i] = macro(host).Line(nodeSelectorRequirement.Values[i])
	}
}

// processPodAffinityTerms
func processPodAffinityTerms(podAffinityTerms []v1.PodAffinityTerm, host *chiV1.ChiHost) {
	for i := range podAffinityTerms {
		podAffinityTerm := &podAffinityTerms[i]
		processPodAffinityTerm(podAffinityTerm, host)
	}
}

// processWeightedPodAffinityTerms
func processWeightedPodAffinityTerms(weightedPodAffinityTerms []v1.WeightedPodAffinityTerm, host *chiV1.ChiHost) {
	for i := range weightedPodAffinityTerms {
		podAffinityTerm := &weightedPodAffinityTerms[i].PodAffinityTerm
		processPodAffinityTerm(podAffinityTerm, host)
	}
}

// processPodAffinityTerm
func processPodAffinityTerm(podAffinityTerm *v1.PodAffinityTerm, host *chiV1.ChiHost) {
	if podAffinityTerm == nil {
		return
	}
	processLabelSelector(podAffinityTerm.LabelSelector, host)
	podAffinityTerm.TopologyKey = macro(host).Line(podAffinityTerm.TopologyKey)
}

// processLabelSelector
func processLabelSelector(labelSelector *metaV1.LabelSelector, host *chiV1.ChiHost) {
	if labelSelector == nil {
		return
	}

	for k := range labelSelector.MatchLabels {
		labelSelector.MatchLabels[k] = macro(host).Line(labelSelector.MatchLabels[k])
	}
	for j := range labelSelector.MatchExpressions {
		labelSelectorRequirement := &labelSelector.MatchExpressions[j]
		processLabelSelectorRequirement(labelSelectorRequirement, host)
	}
}

// processLabelSelectorRequirement
func processLabelSelectorRequirement(labelSelectorRequirement *metaV1.LabelSelectorRequirement, host *chiV1.ChiHost) {
	if labelSelectorRequirement == nil {
		return
	}
	labelSelectorRequirement.Key = macro(host).Line(labelSelectorRequirement.Key)
	// Update values only, keys are not macros-ed
	for i := range labelSelectorRequirement.Values {
		labelSelectorRequirement.Values[i] = macro(host).Line(labelSelectorRequirement.Values[i])
	}
}
