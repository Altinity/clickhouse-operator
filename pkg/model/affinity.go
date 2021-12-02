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

package model

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

	if dst == nil {
		// No receiver, allocate new one
		dst = &v1.Affinity{}
	}

	dst.NodeAffinity = mergeNodeAffinity(dst.NodeAffinity, src.NodeAffinity)
	dst.PodAffinity = mergePodAffinity(dst.PodAffinity, src.PodAffinity)
	dst.PodAntiAffinity = mergePodAntiAffinity(dst.PodAntiAffinity, src.PodAntiAffinity)

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

		PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{},
	}
}

// mergeNodeAffinity
func mergeNodeAffinity(dst *v1.NodeAffinity, src *v1.NodeAffinity) *v1.NodeAffinity {
	if src == nil {
		// Nothing to merge from
		return dst
	}

	if dst == nil {
		// No receiver, allocate new one
		dst = &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{},
			},
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{},
		}
	}

	// Merge NodeSelectors
	for i := range src.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		s := &src.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[i]
		equal := false
		for j := range dst.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
			d := &dst.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[j]
			if _, equal = messagediff.DeepDiff(*s, *d); equal {
				break
			}
		}
		if !equal {
			dst.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = append(
				dst.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms,
				src.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[i],
			)
		}
	}

	// Merge PreferredSchedulingTerm
	for i := range src.PreferredDuringSchedulingIgnoredDuringExecution {
		s := &src.PreferredDuringSchedulingIgnoredDuringExecution[i]
		equal := false
		for j := range dst.PreferredDuringSchedulingIgnoredDuringExecution {
			d := &dst.PreferredDuringSchedulingIgnoredDuringExecution[j]
			if _, equal = messagediff.DeepDiff(*s, *d); equal {
				break
			}
		}
		if !equal {
			dst.PreferredDuringSchedulingIgnoredDuringExecution = append(
				dst.PreferredDuringSchedulingIgnoredDuringExecution,
				src.PreferredDuringSchedulingIgnoredDuringExecution[i],
			)
		}
	}

	return dst
}

// newPodAffinity
func newPodAffinity(template *chiV1.ChiPodTemplate) *v1.PodAffinity {
	podAffinity := &v1.PodAffinity{}

	for i := range template.PodDistribution {
		podDistribution := &template.PodDistribution[i]
		switch podDistribution.Type {
		case chiV1.PodDistributionNamespaceAffinity:
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

	if len(podAffinity.PreferredDuringSchedulingIgnoredDuringExecution) > 0 {
		// Has something to return
		return podAffinity
	}

	return nil
}

// mergePodAffinity
func mergePodAffinity(dst *v1.PodAffinity, src *v1.PodAffinity) *v1.PodAffinity {
	if src == nil {
		// Nothing to merge from
		return dst
	}

	if dst == nil {
		// No receiver, allocate new one
		dst = &v1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution:  []v1.PodAffinityTerm{},
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{},
		}
	}

	// Merge PodAffinityTerm
	for i := range src.RequiredDuringSchedulingIgnoredDuringExecution {
		s := &src.RequiredDuringSchedulingIgnoredDuringExecution[i]
		equal := false
		for j := range dst.RequiredDuringSchedulingIgnoredDuringExecution {
			d := &dst.RequiredDuringSchedulingIgnoredDuringExecution[j]
			if _, equal = messagediff.DeepDiff(*s, *d); equal {
				break
			}
		}
		if !equal {
			dst.RequiredDuringSchedulingIgnoredDuringExecution = append(
				dst.RequiredDuringSchedulingIgnoredDuringExecution,
				src.RequiredDuringSchedulingIgnoredDuringExecution[i],
			)
		}
	}

	// Merge WeightedPodAffinityTerm
	for i := range src.PreferredDuringSchedulingIgnoredDuringExecution {
		s := &src.PreferredDuringSchedulingIgnoredDuringExecution[i]
		equal := false
		for j := range dst.PreferredDuringSchedulingIgnoredDuringExecution {
			d := &dst.PreferredDuringSchedulingIgnoredDuringExecution[j]
			if _, equal = messagediff.DeepDiff(*s, *d); equal {
				break
			}
		}
		if !equal {
			dst.PreferredDuringSchedulingIgnoredDuringExecution = append(
				dst.PreferredDuringSchedulingIgnoredDuringExecution,
				src.PreferredDuringSchedulingIgnoredDuringExecution[i],
			)
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
	podAntiAffinity := &v1.PodAntiAffinity{}

	// PodDistribution
	for i := range template.PodDistribution {
		podDistribution := &template.PodDistribution[i]
		switch podDistribution.Type {
		case chiV1.PodDistributionClickHouseAntiAffinity:
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

	if len(podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) > 0 {
		// Has something to return
		return podAntiAffinity
	}

	return nil
}

// mergePodAntiAffinity
func mergePodAntiAffinity(dst *v1.PodAntiAffinity, src *v1.PodAntiAffinity) *v1.PodAntiAffinity {
	if src == nil {
		// Nothing to merge from
		return dst
	}

	if dst == nil {
		// No receiver, allocate new one
		dst = &v1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution:  []v1.PodAffinityTerm{},
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{},
		}
	}

	// Merge PodAffinityTerm
	for i := range src.RequiredDuringSchedulingIgnoredDuringExecution {
		s := &src.RequiredDuringSchedulingIgnoredDuringExecution[i]
		equal := false
		for j := range dst.RequiredDuringSchedulingIgnoredDuringExecution {
			d := &dst.RequiredDuringSchedulingIgnoredDuringExecution[j]
			if _, equal = messagediff.DeepDiff(*s, *d); equal {
				break
			}
		}
		if !equal {
			dst.RequiredDuringSchedulingIgnoredDuringExecution = append(
				dst.RequiredDuringSchedulingIgnoredDuringExecution,
				src.RequiredDuringSchedulingIgnoredDuringExecution[i],
			)
		}
	}

	// Merge WeightedPodAffinityTerm
	for i := range src.PreferredDuringSchedulingIgnoredDuringExecution {
		s := &src.PreferredDuringSchedulingIgnoredDuringExecution[i]
		equal := false
		for j := range dst.PreferredDuringSchedulingIgnoredDuringExecution {
			d := &dst.PreferredDuringSchedulingIgnoredDuringExecution[j]
			if _, equal = messagediff.DeepDiff(*s, *d); equal {
				break
			}
		}
		if !equal {
			dst.PreferredDuringSchedulingIgnoredDuringExecution = append(
				dst.PreferredDuringSchedulingIgnoredDuringExecution,
				src.PreferredDuringSchedulingIgnoredDuringExecution[i],
			)
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
	if podTemplate.Spec.Affinity == nil {
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
	labelSelectorRequirement.Key = macro(host).Line(labelSelectorRequirement.Key)
	// Update values only, keys are not macros-ed
	for i := range labelSelectorRequirement.Values {
		labelSelectorRequirement.Values[i] = macro(host).Line(labelSelectorRequirement.Values[i])
	}
}
