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

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	"github.com/altinity/clickhouse-operator/pkg/model/common/namer/macro"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// NewAffinity creates new Affinity struct
func NewAffinity(template *api.PodTemplate) *core.Affinity {
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

	return &core.Affinity{
		NodeAffinity:    nodeAffinity,
		PodAffinity:     podAffinity,
		PodAntiAffinity: podAntiAffinity,
	}
}

// MergeAffinity merges from src into dst and returns dst
func MergeAffinity(dst *core.Affinity, src *core.Affinity) *core.Affinity {
	if src == nil {
		// Nothing to merge from
		return dst
	}

	created := false
	if dst == nil {
		// No receiver specified, allocate a new one
		dst = &core.Affinity{}
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
func newNodeAffinity(template *api.PodTemplate) *core.NodeAffinity {
	if template.Zone.Key == "" {
		return nil
	}

	return &core.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &core.NodeSelector{
			NodeSelectorTerms: []core.NodeSelectorTerm{
				{
					// A list of node selector requirements by node's labels.
					MatchExpressions: []core.NodeSelectorRequirement{
						{
							Key:      template.Zone.Key,
							Operator: core.NodeSelectorOpIn,
							Values:   template.Zone.Values,
						},
					},
					// A list of node selector requirements by node's fields.
					//MatchFields: []core.NodeSelectorRequirement{
					//	core.NodeSelectorRequirement{},
					//},
				},
			},
		},

		// PreferredDuringSchedulingIgnoredDuringExecution: []core.PreferredSchedulingTerm{},
	}
}

func getNodeSelectorTerms(affinity *core.NodeAffinity) []core.NodeSelectorTerm {
	if affinity == nil {
		return nil
	}

	if affinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return nil
	}
	return affinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
}

func getNodeSelectorTerm(affinity *core.NodeAffinity, i int) *core.NodeSelectorTerm {
	terms := getNodeSelectorTerms(affinity)
	if terms == nil {
		return nil
	}
	if i >= len(terms) {
		return nil
	}
	return &terms[i]
}

func appendNodeSelectorTerm(affinity *core.NodeAffinity, term *core.NodeSelectorTerm) *core.NodeAffinity {
	if term == nil {
		return affinity
	}

	// Ensure path to terms exists
	if affinity == nil {
		affinity = &core.NodeAffinity{}
	}
	if affinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		affinity.RequiredDuringSchedulingIgnoredDuringExecution = &core.NodeSelector{}
	}

	affinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = append(
		affinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms,
		*term,
	)

	return affinity
}

func getPreferredSchedulingTerms(affinity *core.NodeAffinity) []core.PreferredSchedulingTerm {
	if affinity == nil {
		return nil
	}

	return affinity.PreferredDuringSchedulingIgnoredDuringExecution
}

func getPreferredSchedulingTerm(affinity *core.NodeAffinity, i int) *core.PreferredSchedulingTerm {
	terms := getPreferredSchedulingTerms(affinity)
	if terms == nil {
		return nil
	}
	if i >= len(terms) {
		return nil
	}
	return &terms[i]
}

func appendPreferredSchedulingTerm(affinity *core.NodeAffinity, term *core.PreferredSchedulingTerm) *core.NodeAffinity {
	if term == nil {
		return affinity
	}

	// Ensure path to terms exists
	if affinity == nil {
		affinity = &core.NodeAffinity{}
	}

	affinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
		affinity.PreferredDuringSchedulingIgnoredDuringExecution,
		*term,
	)

	return affinity
}

// mergeNodeAffinity
func mergeNodeAffinity(dst *core.NodeAffinity, src *core.NodeAffinity) *core.NodeAffinity {
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
func newPodAffinity(template *api.PodTemplate) *core.PodAffinity {
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
						labeler.LabelNamespace: macro.MacrosNamespace,
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
						labeler.LabelCHIName: macro.MacrosChiName,
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
						labeler.LabelClusterName: macro.MacrosClusterName,
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
						labeler.LabelShardName: macro.MacrosShardName,
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
						labeler.LabelReplicaName: macro.MacrosReplicaName,
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
						labeler.LabelClusterScopeIndex: macro.MacrosClusterScopeCycleHeadPointsToPreviousCycleTail,
					},
				),
			)
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				newWeightedPodAffinityTermWithMatchLabels(
					1,
					podDistribution,
					map[string]string{
						labeler.LabelClusterScopeIndex: macro.MacrosClusterScopeCycleHeadPointsToPreviousCycleTail,
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

// newMatchLabels
func newMatchLabels(
	podDistribution *api.PodDistribution,
	matchLabels map[string]string,
) map[string]string {
	var scopeLabels map[string]string

	switch podDistribution.Scope {
	case deployment.PodDistributionScopeShard:
		scopeLabels = map[string]string{
			labeler.LabelNamespace:   macro.MacrosNamespace,
			labeler.LabelCHIName:     macro.MacrosChiName,
			labeler.LabelClusterName: macro.MacrosClusterName,
			labeler.LabelShardName:   macro.MacrosShardName,
		}
	case deployment.PodDistributionScopeReplica:
		scopeLabels = map[string]string{
			labeler.LabelNamespace:   macro.MacrosNamespace,
			labeler.LabelCHIName:     macro.MacrosChiName,
			labeler.LabelClusterName: macro.MacrosClusterName,
			labeler.LabelReplicaName: macro.MacrosReplicaName,
		}
	case deployment.PodDistributionScopeCluster:
		scopeLabels = map[string]string{
			labeler.LabelNamespace:   macro.MacrosNamespace,
			labeler.LabelCHIName:     macro.MacrosChiName,
			labeler.LabelClusterName: macro.MacrosClusterName,
		}
	case deployment.PodDistributionScopeClickHouseInstallation:
		scopeLabels = map[string]string{
			labeler.LabelNamespace: macro.MacrosNamespace,
			labeler.LabelCHIName:   macro.MacrosChiName,
		}
	case deployment.PodDistributionScopeNamespace:
		scopeLabels = map[string]string{
			labeler.LabelNamespace: macro.MacrosNamespace,
		}
	case deployment.PodDistributionScopeGlobal:
		scopeLabels = map[string]string{}
	}

	return util.MergeStringMapsOverwrite(matchLabels, scopeLabels)
}

// newPodAntiAffinity
func newPodAntiAffinity(template *api.PodTemplate) *core.PodAntiAffinity {
	// Return podAntiAffinity only in case something was added into it
	added := false
	podAntiAffinity := &core.PodAntiAffinity{}

	// PodDistribution
	for i := range template.PodDistribution {
		podDistribution := &template.PodDistribution[i]
		switch podDistribution.Type {
		case deployment.PodDistributionClickHouseAntiAffinity:
			added = true
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				newPodAffinityTermWithMatchLabels(
					podDistribution,
					newMatchLabels(
						podDistribution,
						map[string]string{
							labeler.LabelAppName: labeler.LabelAppValue,
						},
					),
				),
			)
		case deployment.PodDistributionMaxNumberPerNode:
			added = true
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				newPodAffinityTermWithMatchLabels(
					podDistribution,
					newMatchLabels(
						podDistribution,
						map[string]string{
							labeler.LabelClusterScopeCycleIndex: macro.MacrosClusterScopeCycleIndex,
						},
					),
				),
			)
		case deployment.PodDistributionShardAntiAffinity:
			added = true
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				newPodAffinityTermWithMatchLabels(
					podDistribution,
					newMatchLabels(
						podDistribution,
						map[string]string{
							labeler.LabelShardName: macro.MacrosShardName,
						},
					),
				),
			)
		case deployment.PodDistributionReplicaAntiAffinity:
			added = true
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				newPodAffinityTermWithMatchLabels(
					podDistribution,
					newMatchLabels(
						podDistribution,
						map[string]string{
							labeler.LabelReplicaName: macro.MacrosReplicaName,
						},
					),
				),
			)
		case deployment.PodDistributionAnotherNamespaceAntiAffinity:
			added = true
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				newPodAffinityTermWithMatchExpressions(
					podDistribution,
					[]meta.LabelSelectorRequirement{
						{
							Key:      labeler.LabelNamespace,
							Operator: meta.LabelSelectorOpNotIn,
							Values: []string{
								macro.MacrosNamespace,
							},
						},
					},
				),
			)
		case deployment.PodDistributionAnotherClickHouseInstallationAntiAffinity:
			added = true
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				newPodAffinityTermWithMatchExpressions(
					podDistribution,
					[]meta.LabelSelectorRequirement{
						{
							Key:      labeler.LabelCHIName,
							Operator: meta.LabelSelectorOpNotIn,
							Values: []string{
								macro.MacrosChiName,
							},
						},
					},
				),
			)
		case deployment.PodDistributionAnotherClusterAntiAffinity:
			added = true
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				newPodAffinityTermWithMatchExpressions(
					podDistribution,
					[]meta.LabelSelectorRequirement{
						{
							Key:      labeler.LabelClusterName,
							Operator: meta.LabelSelectorOpNotIn,
							Values: []string{
								macro.MacrosClusterName,
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

func getPodAntiAffinityTerms(affinity *core.PodAntiAffinity) []core.PodAffinityTerm {
	if affinity == nil {
		return nil
	}

	return affinity.RequiredDuringSchedulingIgnoredDuringExecution
}

func getPodAntiAffinityTerm(affinity *core.PodAntiAffinity, i int) *core.PodAffinityTerm {
	terms := getPodAntiAffinityTerms(affinity)
	if terms == nil {
		return nil
	}
	if i >= len(terms) {
		return nil
	}
	return &terms[i]
}

func appendPodAntiAffinityTerm(affinity *core.PodAntiAffinity, term *core.PodAffinityTerm) *core.PodAntiAffinity {
	if term == nil {
		return affinity
	}

	// Ensure path to terms exists
	if affinity == nil {
		affinity = &core.PodAntiAffinity{}
	}

	affinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
		affinity.RequiredDuringSchedulingIgnoredDuringExecution,
		*term,
	)

	return affinity
}

func getWeightedPodAntiAffinityTerms(affinity *core.PodAntiAffinity) []core.WeightedPodAffinityTerm {
	if affinity == nil {
		return nil
	}

	return affinity.PreferredDuringSchedulingIgnoredDuringExecution
}

func getWeightedPodAntiAffinityTerm(affinity *core.PodAntiAffinity, i int) *core.WeightedPodAffinityTerm {
	terms := getWeightedPodAntiAffinityTerms(affinity)
	if terms == nil {
		return nil
	}
	if i >= len(terms) {
		return nil
	}
	return &terms[i]
}

func appendWeightedPodAntiAffinityTerm(affinity *core.PodAntiAffinity, term *core.WeightedPodAffinityTerm) *core.PodAntiAffinity {
	if term == nil {
		return affinity
	}

	// Ensure path to terms exists
	if affinity == nil {
		affinity = &core.PodAntiAffinity{}
	}

	affinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
		affinity.PreferredDuringSchedulingIgnoredDuringExecution,
		*term,
	)

	return affinity
}

// mergePodAntiAffinity
func mergePodAntiAffinity(dst *core.PodAntiAffinity, src *core.PodAntiAffinity) *core.PodAntiAffinity {
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

// PrepareAffinity
func PrepareAffinity(podTemplate *api.PodTemplate, host *api.Host) {
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
func processNodeSelector(nodeSelector *core.NodeSelector, host *api.Host) {
	if nodeSelector == nil {
		return
	}
	for i := range nodeSelector.NodeSelectorTerms {
		nodeSelectorTerm := &nodeSelector.NodeSelectorTerms[i]
		processNodeSelectorTerm(nodeSelectorTerm, host)
	}
}

// processPreferredSchedulingTerms
func processPreferredSchedulingTerms(preferredSchedulingTerms []core.PreferredSchedulingTerm, host *api.Host) {
	for i := range preferredSchedulingTerms {
		nodeSelectorTerm := &preferredSchedulingTerms[i].Preference
		processNodeSelectorTerm(nodeSelectorTerm, host)
	}
}

// processNodeSelectorTerm
func processNodeSelectorTerm(nodeSelectorTerm *core.NodeSelectorTerm, host *api.Host) {
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
func processNodeSelectorRequirement(nodeSelectorRequirement *core.NodeSelectorRequirement, host *api.Host) {
	if nodeSelectorRequirement == nil {
		return
	}
	nodeSelectorRequirement.Key = macro.Macro(host).Line(nodeSelectorRequirement.Key)
	// Update values only, keys are not macros-ed
	for i := range nodeSelectorRequirement.Values {
		nodeSelectorRequirement.Values[i] = macro.Macro(host).Line(nodeSelectorRequirement.Values[i])
	}
}

// processPodAffinityTerms
func processPodAffinityTerms(podAffinityTerms []core.PodAffinityTerm, host *api.Host) {
	for i := range podAffinityTerms {
		podAffinityTerm := &podAffinityTerms[i]
		processPodAffinityTerm(podAffinityTerm, host)
	}
}

// processWeightedPodAffinityTerms
func processWeightedPodAffinityTerms(weightedPodAffinityTerms []core.WeightedPodAffinityTerm, host *api.Host) {
	for i := range weightedPodAffinityTerms {
		podAffinityTerm := &weightedPodAffinityTerms[i].PodAffinityTerm
		processPodAffinityTerm(podAffinityTerm, host)
	}
}

// processPodAffinityTerm
func processPodAffinityTerm(podAffinityTerm *core.PodAffinityTerm, host *api.Host) {
	if podAffinityTerm == nil {
		return
	}
	processLabelSelector(podAffinityTerm.LabelSelector, host)
	podAffinityTerm.TopologyKey = macro.Macro(host).Line(podAffinityTerm.TopologyKey)
}

// processLabelSelector
func processLabelSelector(labelSelector *meta.LabelSelector, host *api.Host) {
	if labelSelector == nil {
		return
	}

	for k := range labelSelector.MatchLabels {
		labelSelector.MatchLabels[k] = macro.Macro(host).Line(labelSelector.MatchLabels[k])
	}
	for j := range labelSelector.MatchExpressions {
		labelSelectorRequirement := &labelSelector.MatchExpressions[j]
		processLabelSelectorRequirement(labelSelectorRequirement, host)
	}
}

// processLabelSelectorRequirement
func processLabelSelectorRequirement(labelSelectorRequirement *meta.LabelSelectorRequirement, host *api.Host) {
	if labelSelectorRequirement == nil {
		return
	}
	labelSelectorRequirement.Key = macro.Macro(host).Line(labelSelectorRequirement.Key)
	// Update values only, keys are not macros-ed
	for i := range labelSelectorRequirement.Values {
		labelSelectorRequirement.Values[i] = macro.Macro(host).Line(labelSelectorRequirement.Values[i])
	}
}
