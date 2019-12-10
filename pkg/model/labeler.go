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
	"github.com/altinity/clickhouse-operator/pkg/util"
	"k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kublabels "k8s.io/apimachinery/pkg/labels"
)

const (
	// Kubernetes labels
	LabelApp                          = clickhousealtinitycom.GroupName + "/app"
	LabelAppValue                     = "chop"
	LabelChop                         = clickhousealtinitycom.GroupName + "/chop"
	LabelChi                          = clickhousealtinitycom.GroupName + "/chi"
	LabelCluster                      = clickhousealtinitycom.GroupName + "/cluster"
	LabelShard                        = clickhousealtinitycom.GroupName + "/shard"
	LabelReplica                      = clickhousealtinitycom.GroupName + "/replica"
	LabelChiCycleIndex                = clickhousealtinitycom.GroupName + "/chiCycleIndex"
	LabelChiCycleOffset               = clickhousealtinitycom.GroupName + "/chiCycleOffset"
	LabelClusterCycleIndex            = clickhousealtinitycom.GroupName + "/clusterCycleIndex"
	LabelClusterCycleOffset           = clickhousealtinitycom.GroupName + "/clusterCycleOffset"
	LabelConfigMap                    = clickhousealtinitycom.GroupName + "/ConfigMap"
	LabelConfigMapValueChiCommon      = "ChiCommon"
	LabelConfigMapValueChiCommonUsers = "ChiCommonUsers"
	LabelConfigMapValueHost           = "Host"
	LabelService                      = clickhousealtinitycom.GroupName + "/Service"
	labelServiceValueChi              = "chi"
	labelServiceValueCluster          = "cluster"
	labelServiceValueShard            = "shard"
	labelServiceValueHost             = "host"

	// Supplementary service labels - used to cooperate with k8s
	LabelZookeeperConfigVersion = clickhousealtinitycom.GroupName + "/zookeeper-version"
	LabelSettingsConfigVersion  = clickhousealtinitycom.GroupName + "/settings-version"
)

// Labeler is an entity which can label CHI artifacts
type Labeler struct {
	version string
	chi     *chi.ClickHouseInstallation
	namer   *namer
}

// NewLabeler creates new labeler with context
func NewLabeler(version string, chi *chi.ClickHouseInstallation) *Labeler {
	return &Labeler{
		version: version,
		chi:     chi,
		namer:   newNamer(namerContextLabels),
	}
}

func (l *Labeler) getLabelsConfigMapChiCommon() map[string]string {
	return util.MergeStringMaps(l.getLabelsChiScope(), map[string]string{
		LabelConfigMap: LabelConfigMapValueChiCommon,
	})
}

func (l *Labeler) getLabelsConfigMapChiCommonUsers() map[string]string {
	return util.MergeStringMaps(l.getLabelsChiScope(), map[string]string{
		LabelConfigMap: LabelConfigMapValueChiCommonUsers,
	})
}

func (l *Labeler) getLabelsConfigMapHost(host *chi.ChiHost) map[string]string {
	return util.MergeStringMaps(l.getLabelsHostScope(host, false), map[string]string{
		LabelConfigMap: LabelConfigMapValueHost,
	})
}

func (l *Labeler) getLabelsServiceChi() map[string]string {
	return util.MergeStringMaps(l.getLabelsChiScope(), map[string]string{
		LabelService: labelServiceValueChi,
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

// getLabelsChiScope gets labels for CHI-scoped object
func (l *Labeler) getLabelsChiScope() map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.appendChiLabels(map[string]string{
		LabelApp:  LabelAppValue,
		LabelChop: l.version,
		LabelChi:  l.namer.getNamePartChiName(l.chi),
	})
}

// getSelectorChiScope gets labels to select a CHI-scoped object
func (l *Labeler) getSelectorChiScope() map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelApp: LabelAppValue,
		// Skip chop
		LabelChi: l.namer.getNamePartChiName(l.chi),
	}
}

// getLabelsClusterScope gets labels for Cluster-scoped object
func (l *Labeler) getLabelsClusterScope(cluster *chi.ChiCluster) map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.appendChiLabels(map[string]string{
		LabelApp:     LabelAppValue,
		LabelChop:    l.version,
		LabelChi:     l.namer.getNamePartChiName(cluster),
		LabelCluster: l.namer.getNamePartClusterName(cluster),
	})
}

// getSelectorClusterScope gets labels to select a Cluster-scoped object
func (l *Labeler) getSelectorClusterScope(cluster *chi.ChiCluster) map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelApp: LabelAppValue,
		// Skip chop
		LabelChi:     l.namer.getNamePartChiName(cluster),
		LabelCluster: l.namer.getNamePartClusterName(cluster),
	}
}

// getLabelsShardScope gets labels for Shard-scoped object
func (l *Labeler) getLabelsShardScope(shard *chi.ChiShard) map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.appendChiLabels(map[string]string{
		LabelApp:     LabelAppValue,
		LabelChop:    l.version,
		LabelChi:     l.namer.getNamePartChiName(shard),
		LabelCluster: l.namer.getNamePartClusterName(shard),
		LabelShard:   l.namer.getNamePartShardName(shard),
	})
}

// getSelectorShardScope gets labels to select a Shard-scoped object
func (l *Labeler) getSelectorShardScope(shard *chi.ChiShard) map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelApp: LabelAppValue,
		// Skip chop
		LabelChi:     l.namer.getNamePartChiName(shard),
		LabelCluster: l.namer.getNamePartClusterName(shard),
		LabelShard:   l.namer.getNamePartShardName(shard),
	}
}

// getLabelsHostScope gets labels for Host-scoped object
func (l *Labeler) getLabelsHostScope(host *chi.ChiHost, applySupplementaryServiceLabels bool) map[string]string {
	// Combine generated labels and CHI-provided labels
	labels := map[string]string{
		LabelApp:                LabelAppValue,
		LabelChop:               l.version,
		LabelChi:                l.namer.getNamePartChiName(host),
		LabelCluster:            l.namer.getNamePartClusterName(host),
		LabelShard:              l.namer.getNamePartShardName(host),
		LabelReplica:            l.namer.getNamePartReplicaName(host),
		LabelChiCycleIndex:      l.namer.getNamePartChiCycleIndex(host),
		LabelChiCycleOffset:     l.namer.getNamePartChiCycleOffset(host),
		LabelClusterCycleIndex:  l.namer.getNamePartClusterCycleIndex(host),
		LabelClusterCycleOffset: l.namer.getNamePartClusterCycleOffset(host),
	}
	if applySupplementaryServiceLabels {
		labels[LabelZookeeperConfigVersion] = host.Config.ZookeeperFingerprint
		labels[LabelSettingsConfigVersion] = host.Config.SettingsFingerprint
	}
	return l.appendChiLabels(labels)
}

// appendChiLabels appends CHI-provided labels to labels set
func (l *Labeler) appendChiLabels(dst map[string]string) map[string]string {
	return util.MergeStringMaps(dst, l.chi.Labels)
}

// getSelectorShardScope gets labels to select a Host-scoped object
func (l *Labeler) GetSelectorHostScope(host *chi.ChiHost) map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelApp: LabelAppValue,
		// skip chop
		LabelChi:     l.namer.getNamePartChiName(host),
		LabelCluster: l.namer.getNamePartClusterName(host),
		LabelShard:   l.namer.getNamePartShardName(host),
		LabelReplica: l.namer.getNamePartReplicaName(host),
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
		// TODO
		// PreferredDuringSchedulingIgnoredDuringExecution []PreferredSchedulingTerm are skipped for now
	}

	if podTemplate.Spec.Affinity.PodAffinity != nil {
		l.processPodAffinityTerms(podTemplate.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution, host)
		// TODO
		// PreferredDuringSchedulingIgnoredDuringExecution []WeightedPodAffinityTerm are skipped for now
	}

	if podTemplate.Spec.Affinity.PodAntiAffinity != nil {
		l.processPodAffinityTerms(podTemplate.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, host)
		// TODO
		// PreferredDuringSchedulingIgnoredDuringExecution []WeightedPodAffinityTerm are skipped for now
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

func (l *Labeler) processNodeSelectorTerm(nodeSelectorTerm *v1.NodeSelectorTerm, host *chi.ChiHost) {
	for j := range nodeSelectorTerm.MatchExpressions {
		nodeSelectorRequirement := &nodeSelectorTerm.MatchExpressions[j]
		l.processNodeSelectorRequirement(nodeSelectorRequirement, host)
	}

	for j := range nodeSelectorTerm.MatchFields {
		nodeSelectorRequirement := &nodeSelectorTerm.MatchExpressions[j]
		l.processNodeSelectorRequirement(nodeSelectorRequirement, host)
	}
}

func (l *Labeler) processNodeSelectorRequirement(nodeSelectorRequirement *v1.NodeSelectorRequirement, host *chi.ChiHost) {
	nodeSelectorRequirement.Key = newNameMacroReplacerHost(host).Replace(nodeSelectorRequirement.Key)
	// Update values only, keys are not macros-ed
	for key := range nodeSelectorRequirement.Values {
		nodeSelectorRequirement.Values[key] = newNameMacroReplacerHost(host).Replace(nodeSelectorRequirement.Values[key])
	}
}

func (l *Labeler) processPodAffinityTerms(podAffinityTerms []v1.PodAffinityTerm, host *chi.ChiHost) {
	for i := range podAffinityTerms {
		podAffinityTerm := &podAffinityTerms[i]
		l.processLabelSelector(podAffinityTerm.LabelSelector, host)
		podAffinityTerm.TopologyKey = newNameMacroReplacerHost(host).Replace(podAffinityTerm.TopologyKey)
	}
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
	for key := range labelSelectorRequirement.Values {
		labelSelectorRequirement.Values[key] = newNameMacroReplacerHost(host).Replace(labelSelectorRequirement.Values[key])
	}
}

// TODO review usage
func GetSetFromObjectMeta(objMeta *meta.ObjectMeta) (kublabels.Set, error) {
	labelApp, ok1 := objMeta.Labels[LabelApp]
	// skip chop
	labelChi, ok2 := objMeta.Labels[LabelChi]

	if (!ok1) || (!ok2) {
		return nil, fmt.Errorf(
			"unable to make set from object. Need to have at least labels '%s' and '%s'. Available Labels: %v",
			LabelApp, LabelChi, objMeta.Labels,
		)
	}

	set := kublabels.Set{
		LabelApp: labelApp,
		// skip chop
		LabelChi: labelChi,
	}

	// Add optional labels

	if labelClusterValue, ok := objMeta.Labels[LabelCluster]; ok {
		set[LabelCluster] = labelClusterValue
	}
	if labelShardValue, ok := objMeta.Labels[LabelShard]; ok {
		set[LabelShard] = labelShardValue
	}
	if labelReplicaValue, ok := objMeta.Labels[LabelReplica]; ok {
		set[LabelReplica] = labelReplicaValue
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

// IsChopGeneratedObject check whether object is generated by an operator. Check is label-based
func IsChopGeneratedObject(objectMeta *meta.ObjectMeta) bool {

	// ObjectMeta must have some labels
	if len(objectMeta.Labels) == 0 {
		return false
	}

	// ObjectMeta must have LabelChop
	_, ok := objectMeta.Labels[LabelChop]

	return ok
}

// GetChiNameFromObjectMeta extracts CHI name from ObjectMeta by labels
func GetChiNameFromObjectMeta(meta *meta.ObjectMeta) (string, error) {
	// ObjectMeta must have LabelChi:  chi.Name label
	name, ok := meta.Labels[LabelChi]
	if ok {
		return name, nil
	} else {
		return "", fmt.Errorf("can not find %s label in meta", LabelChi)
	}
}

// GetClusterNameFromObjectMeta extracts cluster name from ObjectMeta by labels
func GetClusterNameFromObjectMeta(meta *meta.ObjectMeta) (string, error) {
	// ObjectMeta must have LabelCluster
	name, ok := meta.Labels[LabelCluster]
	if ok {
		return name, nil
	} else {
		return "", fmt.Errorf("can not find %s label in meta", LabelChi)
	}
}
