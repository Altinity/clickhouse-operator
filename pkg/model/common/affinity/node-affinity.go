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
	"gopkg.in/d4l3k/messagediff.v1"
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

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
