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

// newAffinity
func newAffinity(template *chiV1.ChiPodTemplate) *v1.Affinity {
	nodeAffinity := newNodeAffinity(template)
	podAffinity := newPodAffinity(template)
	podAntiAffinity := newPodAntiAffinity(template)

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

// mergeAffinity
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
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = addWeightedPodAffinityTermWithMatchLabels(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				1,
				map[string]string{
					LabelNamespace: macrosNamespace,
				},
			)
		case chiV1.PodDistributionClickHouseInstallationAffinity:
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = addWeightedPodAffinityTermWithMatchLabels(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				1,
				map[string]string{
					LabelCHIName: macrosChiName,
				},
			)
		case chiV1.PodDistributionClusterAffinity:
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = addWeightedPodAffinityTermWithMatchLabels(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				1,
				map[string]string{
					LabelClusterName: macrosClusterName,
				},
			)
		case chiV1.PodDistributionShardAffinity:
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = addWeightedPodAffinityTermWithMatchLabels(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				1,
				map[string]string{
					LabelShardName: macrosShardName,
				},
			)
		case chiV1.PodDistributionReplicaAffinity:
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = addWeightedPodAffinityTermWithMatchLabels(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				1,
				map[string]string{
					LabelReplicaName: macrosReplicaName,
				},
			)
		case chiV1.PodDistributionPreviousTailAffinity:
			// Newer k8s insists on Required for this Affinity
			podAffinity.RequiredDuringSchedulingIgnoredDuringExecution = addPodAffinityTermWithMatchLabels(
				podAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				map[string]string{
					LabelClusterScopeIndex: macrosClusterScopeCycleHeadPointsToPreviousCycleTail,
				},
			)
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = addWeightedPodAffinityTermWithMatchLabels(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				1,
				map[string]string{
					LabelClusterScopeIndex: macrosClusterScopeCycleHeadPointsToPreviousCycleTail,
				},
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

	// Distribution
	// DEPRECATED
	if template.Distribution == chiV1.PodDistributionOnePerHost {
		podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = addPodAffinityTermWithMatchLabels(
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
			map[string]string{
				LabelAppName: LabelAppValue,
			},
		)
	}

	// PodDistribution
	for i := range template.PodDistribution {
		podDistribution := &template.PodDistribution[i]
		switch podDistribution.Type {
		case chiV1.PodDistributionClickHouseAntiAffinity:
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = addPodAffinityTermWithMatchLabels(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				newMatchLabels(
					podDistribution,
					map[string]string{
						LabelAppName: LabelAppValue,
					},
				),
			)
		case chiV1.PodDistributionMaxNumberPerNode:
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = addPodAffinityTermWithMatchLabels(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				newMatchLabels(
					podDistribution,
					map[string]string{
						LabelClusterScopeCycleIndex: macrosClusterScopeCycleIndex,
					},
				),
			)
		case chiV1.PodDistributionShardAntiAffinity:
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = addPodAffinityTermWithMatchLabels(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				newMatchLabels(
					podDistribution,
					map[string]string{
						LabelShardName: macrosShardName,
					},
				),
			)
		case chiV1.PodDistributionReplicaAntiAffinity:
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = addPodAffinityTermWithMatchLabels(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				newMatchLabels(
					podDistribution,
					map[string]string{
						LabelReplicaName: macrosReplicaName,
					},
				),
			)
		case chiV1.PodDistributionAnotherNamespaceAntiAffinity:
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = addPodAffinityTermWithMatchExpressions(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				[]metaV1.LabelSelectorRequirement{
					{
						Key:      LabelNamespace,
						Operator: metaV1.LabelSelectorOpNotIn,
						Values: []string{
							macrosNamespace,
						},
					},
				},
			)
		case chiV1.PodDistributionAnotherClickHouseInstallationAntiAffinity:
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = addPodAffinityTermWithMatchExpressions(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				[]metaV1.LabelSelectorRequirement{
					{
						Key:      LabelCHIName,
						Operator: metaV1.LabelSelectorOpNotIn,
						Values: []string{
							macrosChiName,
						},
					},
				},
			)
		case chiV1.PodDistributionAnotherClusterAntiAffinity:
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = addPodAffinityTermWithMatchExpressions(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				[]metaV1.LabelSelectorRequirement{
					{
						Key:      LabelClusterName,
						Operator: metaV1.LabelSelectorOpNotIn,
						Values: []string{
							macrosClusterName,
						},
					},
				},
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

// addPodAffinityTermWithMatchLabels
func addPodAffinityTermWithMatchLabels(terms []v1.PodAffinityTerm, matchLabels map[string]string) []v1.PodAffinityTerm {
	return append(terms,
		v1.PodAffinityTerm{
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
			TopologyKey: "kubernetes.io/hostname",
		},
	)
}

// addPodAffinityTermWithMatchExpressions
func addPodAffinityTermWithMatchExpressions(terms []v1.PodAffinityTerm, matchExpressions []metaV1.LabelSelectorRequirement) []v1.PodAffinityTerm {
	return append(terms,
		v1.PodAffinityTerm{
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
			TopologyKey: "kubernetes.io/hostname",
		},
	)
}

// addWeightedPodAffinityTermWithMatchLabels
func addWeightedPodAffinityTermWithMatchLabels(
	terms []v1.WeightedPodAffinityTerm,
	weight int32,
	matchLabels map[string]string,
) []v1.WeightedPodAffinityTerm {
	return append(terms,
		v1.WeightedPodAffinityTerm{
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
				TopologyKey: "kubernetes.io/hostname",
			},
		},
	)
}
