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

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kublabels "k8s.io/apimachinery/pkg/labels"

	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com"
	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

const (
	// Kubernetes labels

	LabelReadyName                    = clickhousealtinitycom.GroupName + "/ready"
	LabelReadyValue                   = "yes"
	LabelAppName                      = clickhousealtinitycom.GroupName + "/app"
	LabelAppValue                     = "chop"
	LabelCHOP                         = clickhousealtinitycom.GroupName + "/chop"
	LabelNamespace                    = clickhousealtinitycom.GroupName + "/namespace"
	LabelCHIName                      = clickhousealtinitycom.GroupName + "/chi"
	LabelClusterName                  = clickhousealtinitycom.GroupName + "/cluster"
	LabelShardName                    = clickhousealtinitycom.GroupName + "/shard"
	LabelReplicaName                  = clickhousealtinitycom.GroupName + "/replica"
	LabelConfigMap                    = clickhousealtinitycom.GroupName + "/ConfigMap"
	labelConfigMapValueCHICommon      = "ChiCommon"
	labelConfigMapValueCHICommonUsers = "ChiCommonUsers"
	labelConfigMapValueHost           = "Host"
	LabelService                      = clickhousealtinitycom.GroupName + "/Service"
	labelServiceValueCHI              = "chi"
	labelServiceValueCluster          = "cluster"
	labelServiceValueShard            = "shard"
	labelServiceValueHost             = "host"
	LabelPVCReclaimPolicyName         = clickhousealtinitycom.GroupName + "/reclaimPolicy"

	// Supplementary service labels - used to cooperate with k8s
	LabelZookeeperConfigVersion = clickhousealtinitycom.GroupName + "/zookeeper-version"
	LabelSettingsConfigVersion  = clickhousealtinitycom.GroupName + "/settings-version"
	LabelObjectVersion          = clickhousealtinitycom.GroupName + "/object-version"

	// Optional labels
	LabelShardScopeIndex         = clickhousealtinitycom.GroupName + "/shardScopeIndex"
	LabelReplicaScopeIndex       = clickhousealtinitycom.GroupName + "/replicaScopeIndex"
	LabelCHIScopeIndex           = clickhousealtinitycom.GroupName + "/chiScopeIndex"
	LabelCHIScopeCycleSize       = clickhousealtinitycom.GroupName + "/chiScopeCycleSize"
	LabelCHIScopeCycleIndex      = clickhousealtinitycom.GroupName + "/chiScopeCycleIndex"
	LabelCHIScopeCycleOffset     = clickhousealtinitycom.GroupName + "/chiScopeCycleOffset"
	LabelClusterScopeIndex       = clickhousealtinitycom.GroupName + "/clusterScopeIndex"
	LabelClusterScopeCycleSize   = clickhousealtinitycom.GroupName + "/clusterScopeCycleSize"
	LabelClusterScopeCycleIndex  = clickhousealtinitycom.GroupName + "/clusterScopeCycleIndex"
	LabelClusterScopeCycleOffset = clickhousealtinitycom.GroupName + "/clusterScopeCycleOffset"
)

// Labeler is an entity which can label CHI artifacts
type Labeler struct {
	chi *chi.ClickHouseInstallation
}

// NewLabeler creates new labeler with context
func NewLabeler(chi *chi.ClickHouseInstallation) *Labeler {
	return &Labeler{
		chi: chi,
	}
}

// getLabelsConfigMapCHICommon
func (l *Labeler) getLabelsConfigMapCHICommon() map[string]string {
	return util.MergeStringMapsOverwrite(
		l.getLabelsCHIScope(),
		map[string]string{
			LabelConfigMap: labelConfigMapValueCHICommon,
		})
}

// getLabelsConfigMapCHICommonUsers
func (l *Labeler) getLabelsConfigMapCHICommonUsers() map[string]string {
	return util.MergeStringMapsOverwrite(
		l.getLabelsCHIScope(),
		map[string]string{
			LabelConfigMap: labelConfigMapValueCHICommonUsers,
		})
}

// getLabelsConfigMapHost
func (l *Labeler) getLabelsConfigMapHost(host *chi.ChiHost) map[string]string {
	return util.MergeStringMapsOverwrite(
		l.getLabelsHostScope(host, false),
		map[string]string{
			LabelConfigMap: labelConfigMapValueHost,
		})
}

// getLabelsServiceCHI
func (l *Labeler) getLabelsServiceCHI() map[string]string {
	return util.MergeStringMapsOverwrite(
		l.getLabelsCHIScope(),
		map[string]string{
			LabelService: labelServiceValueCHI,
		})
}

// getLabelsServiceCluster
func (l *Labeler) getLabelsServiceCluster(cluster *chi.ChiCluster) map[string]string {
	return util.MergeStringMapsOverwrite(
		l.getLabelsClusterScope(cluster),
		map[string]string{
			LabelService: labelServiceValueCluster,
		})
}

// getLabelsServiceShard
func (l *Labeler) getLabelsServiceShard(shard *chi.ChiShard) map[string]string {
	return util.MergeStringMapsOverwrite(
		l.getLabelsShardScope(shard),
		map[string]string{
			LabelService: labelServiceValueShard,
		})
}

// getLabelsServiceHost
func (l *Labeler) getLabelsServiceHost(host *chi.ChiHost) map[string]string {
	return util.MergeStringMapsOverwrite(
		l.getLabelsHostScope(host, false),
		map[string]string{
			LabelService: labelServiceValueHost,
		})
}

// getLabelsCHIScope gets labels for CHI-scoped object
func (l *Labeler) getLabelsCHIScope() map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.appendCHILabels(l.GetSelectorCHIScope())
}

var labelsNamer = newNamer(namerContextLabels)

// GetSelectorCHIScope gets labels to select a CHI-scoped object
func (l *Labeler) GetSelectorCHIScope() map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelNamespace: labelsNamer.getNamePartNamespace(l.chi),
		LabelAppName:   LabelAppValue,
		LabelCHIName:   labelsNamer.getNamePartCHIName(l.chi),
	}
}

// getSelectorCHIScopeReady gets labels to select a ready-labelled CHI-scoped object
func (l *Labeler) getSelectorCHIScopeReady() map[string]string {
	return appendReadyLabels(l.GetSelectorCHIScope())
}

// getLabelsClusterScope gets labels for Cluster-scoped object
func (l *Labeler) getLabelsClusterScope(cluster *chi.ChiCluster) map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.appendCHILabels(getSelectorClusterScope(cluster))
}

// getSelectorClusterScope gets labels to select a Cluster-scoped object
func getSelectorClusterScope(cluster *chi.ChiCluster) map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelNamespace:   labelsNamer.getNamePartNamespace(cluster),
		LabelAppName:     LabelAppValue,
		LabelCHIName:     labelsNamer.getNamePartCHIName(cluster),
		LabelClusterName: labelsNamer.getNamePartClusterName(cluster),
	}
}

// getSelectorClusterScope gets labels to select a ready-labelled Cluster-scoped object
func getSelectorClusterScopeReady(cluster *chi.ChiCluster) map[string]string {
	return appendReadyLabels(getSelectorClusterScope(cluster))
}

// getLabelsShardScope gets labels for Shard-scoped object
func (l *Labeler) getLabelsShardScope(shard *chi.ChiShard) map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.appendCHILabels(getSelectorShardScope(shard))
}

// getSelectorShardScope gets labels to select a Shard-scoped object
func getSelectorShardScope(shard *chi.ChiShard) map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelNamespace:   labelsNamer.getNamePartNamespace(shard),
		LabelAppName:     LabelAppValue,
		LabelCHIName:     labelsNamer.getNamePartCHIName(shard),
		LabelClusterName: labelsNamer.getNamePartClusterName(shard),
		LabelShardName:   labelsNamer.getNamePartShardName(shard),
	}
}

// getSelectorShardScope gets labels to select a ready-labelled Shard-scoped object
func getSelectorShardScopeReady(shard *chi.ChiShard) map[string]string {
	return appendReadyLabels(getSelectorShardScope(shard))
}

// getLabelsHostScope gets labels for Host-scoped object
func (l *Labeler) getLabelsHostScope(host *chi.ChiHost, applySupplementaryServiceLabels bool) map[string]string {
	// Combine generated labels and CHI-provided labels
	labels := GetSelectorHostScope(host)
	if chop.Config().AppendScopeLabels {
		// Optional labels
		labels[LabelShardScopeIndex] = getNamePartShardScopeIndex(host)
		labels[LabelReplicaScopeIndex] = getNamePartReplicaScopeIndex(host)
		labels[LabelCHIScopeIndex] = getNamePartCHIScopeIndex(host)
		labels[LabelCHIScopeCycleSize] = getNamePartCHIScopeCycleSize(host)
		labels[LabelCHIScopeCycleIndex] = getNamePartCHIScopeCycleIndex(host)
		labels[LabelCHIScopeCycleOffset] = getNamePartCHIScopeCycleOffset(host)
		labels[LabelClusterScopeIndex] = getNamePartClusterScopeIndex(host)
		labels[LabelClusterScopeCycleSize] = getNamePartClusterScopeCycleSize(host)
		labels[LabelClusterScopeCycleIndex] = getNamePartClusterScopeCycleIndex(host)
		labels[LabelClusterScopeCycleOffset] = getNamePartClusterScopeCycleOffset(host)
	}
	if applySupplementaryServiceLabels {
		// Optional labels
		// TODO
		// When we'll have Cluster Discovery functionality we can refactor this properly
		labels[LabelZookeeperConfigVersion] = host.Config.ZookeeperFingerprint
		labels[LabelSettingsConfigVersion] = util.Fingerprint(host.Config.SettingsFingerprint + host.Config.FilesFingerprint)
	}
	return l.appendCHILabels(labels)
}

// getLabelsHostScopeReady gets labels for Host-scoped object including Ready label
func (l *Labeler) getLabelsHostScopeReady(host *chi.ChiHost, applySupplementaryServiceLabels bool) map[string]string {
	return appendReadyLabels(l.getLabelsHostScope(host, applySupplementaryServiceLabels))
}

// getLabelsHostScopeReclaimPolicy
func (l *Labeler) getLabelsHostScopeReclaimPolicy(host *chi.ChiHost, template *chi.ChiVolumeClaimTemplate, applySupplementaryServiceLabels bool) map[string]string {
	return util.MergeStringMapsOverwrite(l.getLabelsHostScope(host, applySupplementaryServiceLabels), map[string]string{
		LabelPVCReclaimPolicyName: template.PVCReclaimPolicy.String(),
	})
}

// GetReclaimPolicy
func GetReclaimPolicy(meta meta.ObjectMeta) chi.PVCReclaimPolicy {
	defaultReclaimPolicy := chi.PVCReclaimPolicyDelete

	if value, ok := meta.Labels[LabelPVCReclaimPolicyName]; ok {
		reclaimPolicy := chi.NewPVCReclaimPolicyFromString(value)
		if reclaimPolicy.IsValid() {
			return reclaimPolicy
		}
	}

	return defaultReclaimPolicy
}

// GetSelectorHostScope gets labels to select a Host-scoped object
func GetSelectorHostScope(host *chi.ChiHost) map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelNamespace:   labelsNamer.getNamePartNamespace(host),
		LabelAppName:     LabelAppValue,
		LabelCHIName:     labelsNamer.getNamePartCHIName(host),
		LabelClusterName: labelsNamer.getNamePartClusterName(host),
		LabelShardName:   labelsNamer.getNamePartShardName(host),
		LabelReplicaName: labelsNamer.getNamePartReplicaName(host),
	}
}

// appendCHILabels appends CHI-provided labels to labels set
func (l *Labeler) appendCHILabels(dst map[string]string) map[string]string {
	sourceLabels := util.CopyMapExcept(l.chi.Labels, chop.Config().ExcludeFromPropagationLabels...)
	return util.MergeStringMapsOverwrite(dst, sourceLabels)
}

// appendReadyLabels appends "Ready" label to labels set
func appendReadyLabels(dst map[string]string) map[string]string {
	return util.MergeStringMapsOverwrite(dst, map[string]string{
		LabelReadyName: LabelReadyValue,
	})
}

// getAnnotationsHostScope gets annotations for Host-scoped object
func getAnnotationsHostScope(host *chi.ChiHost) map[string]string {
	// We may want to append some annotations in here
	return host.GetAnnotations()
}

// makeSetFromObjectMeta makes kublabels.Set from ObjectMeta
func makeSetFromObjectMeta(objMeta *meta.ObjectMeta) (kublabels.Set, error) {
	// Check mandatory labels are in place
	if !util.MapHasKeys(objMeta.Labels, LabelNamespace, LabelAppName, LabelCHIName) {
		return nil, fmt.Errorf(
			"UNABLE to make set from object. Need to have at least labels '%s', '%s' and '%s'. Available Labels: %v",
			LabelNamespace, LabelAppName, LabelCHIName, objMeta.Labels,
		)
	}

	labels := []string{
		// Mandatory labels
		LabelNamespace,
		LabelAppName,
		LabelCHIName,

		// Optional labels
		LabelClusterName,
		LabelShardName,
		LabelReplicaName,
		LabelConfigMap,
		LabelService,
	}

	set := kublabels.Set{}
	util.MergeStringMapsOverwrite(set, objMeta.Labels, labels...)

	// skip StatefulSet
	// skip Zookeeper

	return set, nil
}

// MakeSelectorFromObjectMeta
// TODO review usage
func MakeSelectorFromObjectMeta(objMeta *meta.ObjectMeta) (kublabels.Selector, error) {
	set, err := makeSetFromObjectMeta(objMeta)
	if err != nil {
		// Unable to make set
		return nil, err
	}
	return kublabels.SelectorFromSet(set), nil
}

// IsCHOPGeneratedObject check whether object is generated by an operator. Check is label-based
func IsCHOPGeneratedObject(meta *meta.ObjectMeta) bool {
	if !util.MapHasKeys(meta.Labels, LabelAppName) {
		return false
	}
	return meta.Labels[LabelAppName] == LabelAppValue
}

// GetCHINameFromObjectMeta extracts CHI name from ObjectMeta by labels
func GetCHINameFromObjectMeta(meta *meta.ObjectMeta) (string, error) {
	if !util.MapHasKeys(meta.Labels, LabelCHIName) {
		return "", fmt.Errorf("can not find %s label in meta", LabelCHIName)
	}
	return meta.Labels[LabelCHIName], nil
}

// GetClusterNameFromObjectMeta extracts cluster name from ObjectMeta by labels
func GetClusterNameFromObjectMeta(meta *meta.ObjectMeta) (string, error) {
	if !util.MapHasKeys(meta.Labels, LabelClusterName) {
		return "", fmt.Errorf("can not find %s label in meta", LabelClusterName)
	}
	return meta.Labels[LabelClusterName], nil
}

// MakeObjectVersionLabel
func MakeObjectVersionLabel(meta *meta.ObjectMeta, obj interface{}) {
	meta.Labels = util.MergeStringMapsOverwrite(
		meta.Labels,
		map[string]string{
			LabelObjectVersion: util.Fingerprint(obj),
		},
	)
}

// isObjectVersionLabelTheSame
func isObjectVersionLabelTheSame(meta *meta.ObjectMeta, value string) bool {
	if meta == nil {
		return false
	}

	l, ok := meta.Labels[LabelObjectVersion]
	if !ok {
		return false
	}

	return l == value
}

// IsObjectTheSame
func IsObjectTheSame(meta1, meta2 *meta.ObjectMeta) bool {
	if (meta1 == nil) && (meta2 == nil) {
		return true
	}
	if (meta1 != nil) && (meta2 == nil) {
		return false
	}
	if (meta1 == nil) && (meta2 != nil) {
		return false
	}

	l, ok := meta2.Labels[LabelObjectVersion]
	if !ok {
		return false
	}

	return isObjectVersionLabelTheSame(meta1, l)
}

// AppendLabelReady adds "ready" label with value = UTC now
func AppendLabelReady(meta *meta.ObjectMeta) {
	if meta == nil {
		return
	}
	util.MergeStringMapsOverwrite(
		meta.Labels,
		map[string]string{
			LabelReadyName: LabelReadyValue,
		})
}

// DeleteLabelReady deletes "ready" label
func DeleteLabelReady(meta *meta.ObjectMeta) {
	if meta == nil {
		return
	}
	util.MapDeleteKeys(meta.Labels, LabelReadyName)
}
