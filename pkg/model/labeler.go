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
	"fmt"
	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com"
	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/util"
	"k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kublabels "k8s.io/apimachinery/pkg/labels"
)

const (
	// Kubernetes labels
	LabelAppName                      = clickhousealtinitycom.GroupName + "/app"
	LabelAppValue                     = "chop"
	LabelCHOP                         = clickhousealtinitycom.GroupName + "/chop"
	LabelNamespace                    = clickhousealtinitycom.GroupName + "/namespace"
	LabelCHIName                      = clickhousealtinitycom.GroupName + "/chi"
	LabelClusterName                  = clickhousealtinitycom.GroupName + "/cluster"
	LabelShardName                    = clickhousealtinitycom.GroupName + "/shard"
	LabelShardScopeIndex              = clickhousealtinitycom.GroupName + "/shardScopeIndex"
	LabelReplicaName                  = clickhousealtinitycom.GroupName + "/replica"
	LabelReplicaScopeIndex            = clickhousealtinitycom.GroupName + "/replicaScopeIndex"
	LabelCHIScopeIndex                = clickhousealtinitycom.GroupName + "/chiScopeIndex"
	LabelCHIScopeCycleSize            = clickhousealtinitycom.GroupName + "/chiScopeCycleSize"
	LabelCHIScopeCycleIndex           = clickhousealtinitycom.GroupName + "/chiScopeCycleIndex"
	LabelCHIScopeCycleOffset          = clickhousealtinitycom.GroupName + "/chiScopeCycleOffset"
	LabelClusterScopeIndex            = clickhousealtinitycom.GroupName + "/clusterScopeIndex"
	LabelClusterScopeCycleSize        = clickhousealtinitycom.GroupName + "/clusterScopeCycleSize"
	LabelClusterScopeCycleIndex       = clickhousealtinitycom.GroupName + "/clusterScopeCycleIndex"
	LabelClusterScopeCycleOffset      = clickhousealtinitycom.GroupName + "/clusterScopeCycleOffset"
	LabelConfigMap                    = clickhousealtinitycom.GroupName + "/ConfigMap"
	labelConfigMapValueCHICommon      = "ChiCommon"
	labelConfigMapValueCHICommonUsers = "ChiCommonUsers"
	labelConfigMapValueHost           = "Host"
	LabelService                      = clickhousealtinitycom.GroupName + "/Service"
	labelServiceValueCHI              = "chi"
	labelServiceValueCluster          = "cluster"
	labelServiceValueShard            = "shard"
	labelServiceValueHost             = "host"

	// Supplementary service labels - used to cooperate with k8s
	LabelZookeeperConfigVersion = clickhousealtinitycom.GroupName + "/zookeeper-version"
	LabelSettingsConfigVersion  = clickhousealtinitycom.GroupName + "/settings-version"
)

// Labeler is an entity which can label CHI artifacts
type Labeler struct {
	chop  *chop.CHOp
	chi   *chi.ClickHouseInstallation
	namer *namer
}

// NewLabeler creates new labeler with context
func NewLabeler(chop *chop.CHOp, chi *chi.ClickHouseInstallation) *Labeler {
	return &Labeler{
		chop:  chop,
		chi:   chi,
		namer: newNamer(namerContextLabels),
	}
}

func (l *Labeler) getLabelsConfigMapCHICommon() map[string]string {
	return util.MergeStringMaps(l.getLabelsCHIScope(), map[string]string{
		LabelConfigMap: labelConfigMapValueCHICommon,
	})
}

func (l *Labeler) getLabelsConfigMapCHICommonUsers() map[string]string {
	return util.MergeStringMaps(l.getLabelsCHIScope(), map[string]string{
		LabelConfigMap: labelConfigMapValueCHICommonUsers,
	})
}

func (l *Labeler) getLabelsConfigMapHost(host *chi.ChiHost) map[string]string {
	return util.MergeStringMaps(l.getLabelsHostScope(host, false), map[string]string{
		LabelConfigMap: labelConfigMapValueHost,
	})
}

func (l *Labeler) getLabelsServiceCHI() map[string]string {
	return util.MergeStringMaps(l.getLabelsCHIScope(), map[string]string{
		LabelService: labelServiceValueCHI,
	})
}

func (l *Labeler) getLabelsServiceCluster(cluster *chi.ChiCluster) map[string]string {
	return util.MergeStringMaps(l.getLabelsClusterScope(cluster), map[string]string{
		LabelService: labelServiceValueCluster,
	})
}

func (l *Labeler) getLabelsServiceShard(shard *chi.ChiShard) map[string]string {
	return util.MergeStringMaps(l.getLabelsShardScope(shard), map[string]string{
		LabelService: labelServiceValueShard,
	})
}

func (l *Labeler) getLabelsServiceHost(host *chi.ChiHost) map[string]string {
	return util.MergeStringMaps(l.getLabelsHostScope(host, false), map[string]string{
		LabelService: labelServiceValueHost,
	})
}

// getLabelsCHIScope gets labels for CHI-scoped object
func (l *Labeler) getLabelsCHIScope() map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.appendCHILabels(map[string]string{
		LabelNamespace: l.namer.getNamePartNamespace(l.chi),
		LabelAppName:   LabelAppValue,
		LabelCHOP:      l.chop.Version,
		LabelCHIName:   l.namer.getNamePartCHIName(l.chi),
	})
}

// getSelectorCHIScope gets labels to select a CHI-scoped object
func (l *Labeler) getSelectorCHIScope() map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelAppName: LabelAppValue,
		// Skip chop
		LabelCHIName: l.namer.getNamePartCHIName(l.chi),
	}
}

// getLabelsClusterScope gets labels for Cluster-scoped object
func (l *Labeler) getLabelsClusterScope(cluster *chi.ChiCluster) map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.appendCHILabels(map[string]string{
		LabelNamespace:   l.namer.getNamePartNamespace(cluster),
		LabelAppName:     LabelAppValue,
		LabelCHOP:        l.chop.Version,
		LabelCHIName:     l.namer.getNamePartCHIName(cluster),
		LabelClusterName: l.namer.getNamePartClusterName(cluster),
	})
}

// getSelectorClusterScope gets labels to select a Cluster-scoped object
func (l *Labeler) getSelectorClusterScope(cluster *chi.ChiCluster) map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelAppName: LabelAppValue,
		// Skip chop
		LabelCHIName:     l.namer.getNamePartCHIName(cluster),
		LabelClusterName: l.namer.getNamePartClusterName(cluster),
	}
}

// getLabelsShardScope gets labels for Shard-scoped object
func (l *Labeler) getLabelsShardScope(shard *chi.ChiShard) map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.appendCHILabels(map[string]string{
		LabelNamespace:   l.namer.getNamePartNamespace(shard),
		LabelAppName:     LabelAppValue,
		LabelCHOP:        l.chop.Version,
		LabelCHIName:     l.namer.getNamePartCHIName(shard),
		LabelClusterName: l.namer.getNamePartClusterName(shard),
		LabelShardName:   l.namer.getNamePartShardName(shard),
	})
}

// getSelectorShardScope gets labels to select a Shard-scoped object
func (l *Labeler) getSelectorShardScope(shard *chi.ChiShard) map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelAppName: LabelAppValue,
		// Skip chop
		LabelCHIName:     l.namer.getNamePartCHIName(shard),
		LabelClusterName: l.namer.getNamePartClusterName(shard),
		LabelShardName:   l.namer.getNamePartShardName(shard),
	}
}

// getLabelsHostScope gets labels for Host-scoped object
func (l *Labeler) getLabelsHostScope(host *chi.ChiHost, applySupplementaryServiceLabels bool) map[string]string {
	// Combine generated labels and CHI-provided labels
	labels := map[string]string{
		LabelNamespace:               l.namer.getNamePartNamespace(host),
		LabelAppName:                 LabelAppValue,
		LabelCHOP:                    l.chop.Version,
		LabelCHIName:                 l.namer.getNamePartCHIName(host),
		LabelClusterName:             l.namer.getNamePartClusterName(host),
		LabelShardName:               l.namer.getNamePartShardName(host),
		LabelShardScopeIndex:         l.namer.getNamePartShardScopeIndex(host),
		LabelReplicaName:             l.namer.getNamePartReplicaName(host),
		LabelReplicaScopeIndex:       l.namer.getNamePartReplicaScopeIndex(host),
		LabelCHIScopeIndex:           l.namer.getNamePartCHIScopeIndex(host),
		LabelCHIScopeCycleSize:       l.namer.getNamePartCHIScopeCycleSize(host),
		LabelCHIScopeCycleIndex:      l.namer.getNamePartCHIScopeCycleIndex(host),
		LabelCHIScopeCycleOffset:     l.namer.getNamePartCHIScopeCycleOffset(host),
		LabelClusterScopeIndex:       l.namer.getNamePartClusterScopeIndex(host),
		LabelClusterScopeCycleSize:   l.namer.getNamePartClusterScopeCycleSize(host),
		LabelClusterScopeCycleIndex:  l.namer.getNamePartClusterScopeCycleIndex(host),
		LabelClusterScopeCycleOffset: l.namer.getNamePartClusterScopeCycleOffset(host),
	}
	if applySupplementaryServiceLabels {
		// TODO
		// When we'll have Cluster Discovery functionality we can refactor this properly
		labels[LabelZookeeperConfigVersion] = host.Config.ZookeeperFingerprint
		labels[LabelSettingsConfigVersion] = util.Fingerprint(host.Config.SettingsFingerprint + host.Config.FilesFingerprint)
	}
	return l.appendCHILabels(labels)
}

// appendCHILabels appends CHI-provided labels to labels set
func (l *Labeler) appendCHILabels(dst map[string]string) map[string]string {
	return util.MergeStringMaps(dst, l.chi.Labels)
}

// getAnnotationsHostScope gets annotations for Host-scoped object
func (l *Labeler) getAnnotationsHostScope(host *chi.ChiHost) map[string]string {
	// We may want to append some annotations in here
	return host.GetAnnotations()
}

// getSelectorShardScope gets labels to select a Host-scoped object
func (l *Labeler) GetSelectorHostScope(host *chi.ChiHost) map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelAppName: LabelAppValue,
		// skip chop
		LabelCHIName:     l.namer.getNamePartCHIName(host),
		LabelClusterName: l.namer.getNamePartClusterName(host),
		LabelShardName:   l.namer.getNamePartShardName(host),
		LabelReplicaName: l.namer.getNamePartReplicaName(host),
		// skip StatefulSet
		// skip Zookeeper
	}
}

func (l *Labeler) prepareAffinity(podTemplate *chi.ChiPodTemplate, host *chi.ChiHost) {
	if podTemplate.Spec.Affinity == nil {
		return
	}

	// Walk over all affinity fields

	if podTemplate.Spec.Affinity.NodeAffinity != nil {
		l.processNodeSelector(podTemplate.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution, host)
		l.processPreferredSchedulingTerms(podTemplate.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution, host)
	}

	if podTemplate.Spec.Affinity.PodAffinity != nil {
		l.processPodAffinityTerms(podTemplate.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution, host)
		l.processWeightedPodAffinityTerms(podTemplate.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution, host)
	}

	if podTemplate.Spec.Affinity.PodAntiAffinity != nil {
		l.processPodAffinityTerms(podTemplate.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, host)
		l.processWeightedPodAffinityTerms(podTemplate.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, host)
	}
}

func (l *Labeler) processNodeSelector(nodeSelector *v1.NodeSelector, host *chi.ChiHost) {
	if nodeSelector == nil {
		return
	}
	for i := range nodeSelector.NodeSelectorTerms {
		nodeSelectorTerm := &nodeSelector.NodeSelectorTerms[i]
		l.processNodeSelectorTerm(nodeSelectorTerm, host)
	}
}

func (l *Labeler) processPreferredSchedulingTerms(preferredSchedulingTerms []v1.PreferredSchedulingTerm, host *chi.ChiHost) {
	for i := range preferredSchedulingTerms {
		nodeSelectorTerm := &preferredSchedulingTerms[i].Preference
		l.processNodeSelectorTerm(nodeSelectorTerm, host)
	}
}

func (l *Labeler) processNodeSelectorTerm(nodeSelectorTerm *v1.NodeSelectorTerm, host *chi.ChiHost) {
	for i := range nodeSelectorTerm.MatchExpressions {
		nodeSelectorRequirement := &nodeSelectorTerm.MatchExpressions[i]
		l.processNodeSelectorRequirement(nodeSelectorRequirement, host)
	}

	for i := range nodeSelectorTerm.MatchFields {
		nodeSelectorRequirement := &nodeSelectorTerm.MatchFields[i]
		l.processNodeSelectorRequirement(nodeSelectorRequirement, host)
	}
}

func (l *Labeler) processNodeSelectorRequirement(nodeSelectorRequirement *v1.NodeSelectorRequirement, host *chi.ChiHost) {
	nodeSelectorRequirement.Key = newNameMacroReplacerHost(host).Replace(nodeSelectorRequirement.Key)
	// Update values only, keys are not macros-ed
	for i := range nodeSelectorRequirement.Values {
		nodeSelectorRequirement.Values[i] = newNameMacroReplacerHost(host).Replace(nodeSelectorRequirement.Values[i])
	}
}

func (l *Labeler) processPodAffinityTerms(podAffinityTerms []v1.PodAffinityTerm, host *chi.ChiHost) {
	for i := range podAffinityTerms {
		podAffinityTerm := &podAffinityTerms[i]
		l.processPodAffinityTerm(podAffinityTerm, host)
	}
}

func (l *Labeler) processWeightedPodAffinityTerms(weightedPodAffinityTerms []v1.WeightedPodAffinityTerm, host *chi.ChiHost) {
	for i := range weightedPodAffinityTerms {
		podAffinityTerm := &weightedPodAffinityTerms[i].PodAffinityTerm
		l.processPodAffinityTerm(podAffinityTerm, host)
	}
}

func (l *Labeler) processPodAffinityTerm(podAffinityTerm *v1.PodAffinityTerm, host *chi.ChiHost) {
	l.processLabelSelector(podAffinityTerm.LabelSelector, host)
	podAffinityTerm.TopologyKey = newNameMacroReplacerHost(host).Replace(podAffinityTerm.TopologyKey)
}

func (l *Labeler) processLabelSelector(labelSelector *meta.LabelSelector, host *chi.ChiHost) {
	if labelSelector == nil {
		return
	}

	for k := range labelSelector.MatchLabels {
		labelSelector.MatchLabels[k] = newNameMacroReplacerHost(host).Replace(labelSelector.MatchLabels[k])
	}
	for j := range labelSelector.MatchExpressions {
		labelSelectorRequirement := &labelSelector.MatchExpressions[j]
		l.processLabelSelectorRequirement(labelSelectorRequirement, host)
	}
}

func (l *Labeler) processLabelSelectorRequirement(labelSelectorRequirement *meta.LabelSelectorRequirement, host *chi.ChiHost) {
	labelSelectorRequirement.Key = newNameMacroReplacerHost(host).Replace(labelSelectorRequirement.Key)
	// Update values only, keys are not macros-ed
	for i := range labelSelectorRequirement.Values {
		labelSelectorRequirement.Values[i] = newNameMacroReplacerHost(host).Replace(labelSelectorRequirement.Values[i])
	}
}

// TODO review usage
func GetSetFromObjectMeta(objMeta *meta.ObjectMeta) (kublabels.Set, error) {
	labelApp, ok1 := objMeta.Labels[LabelAppName]
	// skip chop
	labelCHI, ok2 := objMeta.Labels[LabelCHIName]

	if (!ok1) || (!ok2) {
		return nil, fmt.Errorf(
			"unable to make set from object. Need to have at least labels '%s' and '%s'. Available Labels: %v",
			LabelAppName, LabelCHIName, objMeta.Labels,
		)
	}

	set := kublabels.Set{
		LabelAppName: labelApp,
		// skip chop
		LabelCHIName: labelCHI,
	}

	// Add optional labels

	if labelClusterValue, ok := objMeta.Labels[LabelClusterName]; ok {
		set[LabelClusterName] = labelClusterValue
	}
	if labelShardValue, ok := objMeta.Labels[LabelShardName]; ok {
		set[LabelShardName] = labelShardValue
	}
	if labelReplicaValue, ok := objMeta.Labels[LabelReplicaName]; ok {
		set[LabelReplicaName] = labelReplicaValue
	}
	if labelConfigMapValue, ok := objMeta.Labels[LabelConfigMap]; ok {
		set[LabelConfigMap] = labelConfigMapValue
	}
	if labelServiceValue, ok := objMeta.Labels[LabelService]; ok {
		set[LabelService] = labelServiceValue
	}

	// skip StatefulSet
	// skip Zookeeper

	return set, nil
}

// TODO review usage
func GetSelectorFromObjectMeta(objMeta *meta.ObjectMeta) (kublabels.Selector, error) {
	if set, err := GetSetFromObjectMeta(objMeta); err != nil {
		return nil, err
	} else {
		return kublabels.SelectorFromSet(set), nil
	}
}

// IsCHOPGeneratedObject check whether object is generated by an operator. Check is label-based
func IsCHOPGeneratedObject(objectMeta *meta.ObjectMeta) bool {

	// ObjectMeta must have some labels
	if len(objectMeta.Labels) == 0 {
		return false
	}

	// ObjectMeta must have LabelCHOP
	_, ok := objectMeta.Labels[LabelCHOP]

	return ok
}

// GetCHINameFromObjectMeta extracts CHI name from ObjectMeta by labels
func GetCHINameFromObjectMeta(meta *meta.ObjectMeta) (string, error) {
	// ObjectMeta must have LabelCHIName:  chi.Name label
	name, ok := meta.Labels[LabelCHIName]
	if ok {
		return name, nil
	} else {
		return "", fmt.Errorf("can not find %s label in meta", LabelCHIName)
	}
}

// GetClusterNameFromObjectMeta extracts cluster name from ObjectMeta by labels
func GetClusterNameFromObjectMeta(meta *meta.ObjectMeta) (string, error) {
	// ObjectMeta must have LabelClusterName
	name, ok := meta.Labels[LabelClusterName]
	if ok {
		return name, nil
	} else {
		return "", fmt.Errorf("can not find %s label in meta", LabelCHIName)
	}
}
