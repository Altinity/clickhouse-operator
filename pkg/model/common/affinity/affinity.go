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

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

type Affinity struct {
	macro   interfaces.IMacro
	labeler interfaces.ILabeler
}

func New(macro interfaces.IMacro, labeler interfaces.ILabeler) *Affinity {
	return &Affinity{
		macro:   macro,
		labeler: labeler,
	}
}

// Make creates new Affinity struct
func (a *Affinity) Make(template *api.PodTemplate) *core.Affinity {
	// Pod node affinity scheduling rules.
	nodeAffinity := newNodeAffinity(template)
	// Pod affinity scheduling rules. Ex.: co-locate this pod in the same node, zone, etc
	podAffinity := a.newPodAffinity(template)
	// Pod anti-affinity scheduling rules. Ex.: avoid putting this pod in the same node, zone, etc
	podAntiAffinity := a.newPodAntiAffinity(template)

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

// PreparePreparePodTemplate
func (a *Affinity) PreparePodTemplate(podTemplate *api.PodTemplate, host *api.Host) {
	switch {
	case podTemplate == nil:
		return
	case podTemplate.Spec.Affinity == nil:
		return
	}

	// Walk over all affinity fields

	if podTemplate.Spec.Affinity.NodeAffinity != nil {
		a.processNodeSelector(podTemplate.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution, host)
		a.processPreferredSchedulingTerms(podTemplate.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution, host)
	}

	if podTemplate.Spec.Affinity.PodAffinity != nil {
		a.processPodAffinityTerms(podTemplate.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution, host)
		a.processWeightedPodAffinityTerms(podTemplate.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution, host)
	}

	if podTemplate.Spec.Affinity.PodAntiAffinity != nil {
		a.processPodAffinityTerms(podTemplate.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, host)
		a.processWeightedPodAffinityTerms(podTemplate.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, host)
	}
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

// processNodeSelector
func (a *Affinity) processNodeSelector(nodeSelector *core.NodeSelector, host *api.Host) {
	if nodeSelector == nil {
		return
	}
	for i := range nodeSelector.NodeSelectorTerms {
		nodeSelectorTerm := &nodeSelector.NodeSelectorTerms[i]
		a.processNodeSelectorTerm(nodeSelectorTerm, host)
	}
}

// processPreferredSchedulingTerms
func (a *Affinity) processPreferredSchedulingTerms(preferredSchedulingTerms []core.PreferredSchedulingTerm, host *api.Host) {
	for i := range preferredSchedulingTerms {
		nodeSelectorTerm := &preferredSchedulingTerms[i].Preference
		a.processNodeSelectorTerm(nodeSelectorTerm, host)
	}
}

// processNodeSelectorTerm
func (a *Affinity) processNodeSelectorTerm(nodeSelectorTerm *core.NodeSelectorTerm, host *api.Host) {
	for i := range nodeSelectorTerm.MatchExpressions {
		nodeSelectorRequirement := &nodeSelectorTerm.MatchExpressions[i]
		a.processNodeSelectorRequirement(nodeSelectorRequirement, host)
	}

	for i := range nodeSelectorTerm.MatchFields {
		nodeSelectorRequirement := &nodeSelectorTerm.MatchFields[i]
		a.processNodeSelectorRequirement(nodeSelectorRequirement, host)
	}
}

// processNodeSelectorRequirement
func (a *Affinity) processNodeSelectorRequirement(nodeSelectorRequirement *core.NodeSelectorRequirement, host *api.Host) {
	if nodeSelectorRequirement == nil {
		return
	}
	nodeSelectorRequirement.Key = a.macro.Scope(host).Line(nodeSelectorRequirement.Key)
	// Update values only, keys are not macros-ed
	for i := range nodeSelectorRequirement.Values {
		nodeSelectorRequirement.Values[i] = a.macro.Scope(host).Line(nodeSelectorRequirement.Values[i])
	}
}

// processPodAffinityTerms
func (a *Affinity) processPodAffinityTerms(podAffinityTerms []core.PodAffinityTerm, host *api.Host) {
	for i := range podAffinityTerms {
		podAffinityTerm := &podAffinityTerms[i]
		a.processPodAffinityTerm(podAffinityTerm, host)
	}
}

// processWeightedPodAffinityTerms
func (a *Affinity) processWeightedPodAffinityTerms(weightedPodAffinityTerms []core.WeightedPodAffinityTerm, host *api.Host) {
	for i := range weightedPodAffinityTerms {
		podAffinityTerm := &weightedPodAffinityTerms[i].PodAffinityTerm
		a.processPodAffinityTerm(podAffinityTerm, host)
	}
}

// processPodAffinityTerm
func (a *Affinity) processPodAffinityTerm(podAffinityTerm *core.PodAffinityTerm, host *api.Host) {
	if podAffinityTerm == nil {
		return
	}
	a.processLabelSelector(podAffinityTerm.LabelSelector, host)
	podAffinityTerm.TopologyKey = a.macro.Scope(host).Line(podAffinityTerm.TopologyKey)
}
