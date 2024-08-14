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

// newPodAntiAffinity
func (a *Affinity) newPodAntiAffinity(template *api.PodTemplate) *core.PodAntiAffinity {
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
					a.newMatchLabels(
						podDistribution,
						map[string]string{
							commonLabeler.LabelAppName: commonLabeler.LabelAppValue,
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
					a.newMatchLabels(
						podDistribution,
						map[string]string{
							commonLabeler.LabelClusterScopeCycleIndex: a.macro.Get(macro.MacrosClusterScopeCycleIndex),
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
					a.newMatchLabels(
						podDistribution,
						map[string]string{
							commonLabeler.LabelShardName: a.macro.Get(macro.MacrosShardName),
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
					a.newMatchLabels(
						podDistribution,
						map[string]string{
							commonLabeler.LabelReplicaName: a.macro.Get(macro.MacrosReplicaName),
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
							Key:      commonLabeler.LabelNamespace,
							Operator: meta.LabelSelectorOpNotIn,
							Values: []string{
								a.macro.Get(macro.MacrosNamespace),
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
							Key:      commonLabeler.LabelCRName,
							Operator: meta.LabelSelectorOpNotIn,
							Values: []string{
								a.macro.Get(macro.MacrosCRName),
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
							Key:      commonLabeler.LabelClusterName,
							Operator: meta.LabelSelectorOpNotIn,
							Values: []string{
								a.macro.Get(macro.MacrosClusterName),
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
