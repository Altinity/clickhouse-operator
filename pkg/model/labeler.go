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
	v1 "k8s.io/api/core/v1"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kublabels "k8s.io/apimachinery/pkg/labels"

	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com"
	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Set of kubernetes labels used by the operator
const (
	// Main labels

	LabelReadyName                    = clickhousealtinitycom.APIGroupName + "/" + "ready"
	LabelReadyValueReady              = "yes"
	LabelReadyValueNotReady           = "no"
	LabelAppName                      = clickhousealtinitycom.APIGroupName + "/" + "app"
	LabelAppValue                     = "chop"
	LabelCHOP                         = clickhousealtinitycom.APIGroupName + "/" + "chop"
	LabelCHOPCommit                   = clickhousealtinitycom.APIGroupName + "/" + "chop-commit"
	LabelCHOPDate                     = clickhousealtinitycom.APIGroupName + "/" + "chop-date"
	LabelNamespace                    = clickhousealtinitycom.APIGroupName + "/" + "namespace"
	LabelCHIName                      = clickhousealtinitycom.APIGroupName + "/" + "chi"
	LabelClusterName                  = clickhousealtinitycom.APIGroupName + "/" + "cluster"
	LabelShardName                    = clickhousealtinitycom.APIGroupName + "/" + "shard"
	LabelReplicaName                  = clickhousealtinitycom.APIGroupName + "/" + "replica"
	LabelConfigMap                    = clickhousealtinitycom.APIGroupName + "/" + "ConfigMap"
	labelConfigMapValueCHICommon      = "ChiCommon"
	labelConfigMapValueCHICommonUsers = "ChiCommonUsers"
	labelConfigMapValueHost           = "Host"
	LabelService                      = clickhousealtinitycom.APIGroupName + "/" + "Service"
	labelServiceValueCHI              = "chi"
	labelServiceValueCluster          = "cluster"
	labelServiceValueShard            = "shard"
	labelServiceValueHost             = "host"
	LabelPVCReclaimPolicyName         = clickhousealtinitycom.APIGroupName + "/" + "reclaimPolicy"

	// Supplementary service labels - used to cooperate with k8s

	LabelZookeeperConfigVersion = clickhousealtinitycom.APIGroupName + "/" + "zookeeper-version"
	LabelSettingsConfigVersion  = clickhousealtinitycom.APIGroupName + "/" + "settings-version"
	LabelObjectVersion          = clickhousealtinitycom.APIGroupName + "/" + "object-version"

	// Optional labels

	LabelShardScopeIndex         = clickhousealtinitycom.APIGroupName + "/" + "shardScopeIndex"
	LabelReplicaScopeIndex       = clickhousealtinitycom.APIGroupName + "/" + "replicaScopeIndex"
	LabelCHIScopeIndex           = clickhousealtinitycom.APIGroupName + "/" + "chiScopeIndex"
	LabelCHIScopeCycleSize       = clickhousealtinitycom.APIGroupName + "/" + "chiScopeCycleSize"
	LabelCHIScopeCycleIndex      = clickhousealtinitycom.APIGroupName + "/" + "chiScopeCycleIndex"
	LabelCHIScopeCycleOffset     = clickhousealtinitycom.APIGroupName + "/" + "chiScopeCycleOffset"
	LabelClusterScopeIndex       = clickhousealtinitycom.APIGroupName + "/" + "clusterScopeIndex"
	LabelClusterScopeCycleSize   = clickhousealtinitycom.APIGroupName + "/" + "clusterScopeCycleSize"
	LabelClusterScopeCycleIndex  = clickhousealtinitycom.APIGroupName + "/" + "clusterScopeCycleIndex"
	LabelClusterScopeCycleOffset = clickhousealtinitycom.APIGroupName + "/" + "clusterScopeCycleOffset"
)

// Labeler is an entity which can label CHI artifacts
type Labeler struct {
	chi *chiv1.ClickHouseInstallation
}

// NewLabeler creates new labeler with context
func NewLabeler(chi *chiv1.ClickHouseInstallation) *Labeler {
	return &Labeler{
		chi: chi,
	}
}

// getConfigMapCHICommon
func (l *Labeler) getConfigMapCHICommon() map[string]string {
	return util.MergeStringMapsOverwrite(
		l.getCHIScope(),
		map[string]string{
			LabelConfigMap: labelConfigMapValueCHICommon,
		})
}

// getConfigMapCHICommonUsers
func (l *Labeler) getConfigMapCHICommonUsers() map[string]string {
	return util.MergeStringMapsOverwrite(
		l.getCHIScope(),
		map[string]string{
			LabelConfigMap: labelConfigMapValueCHICommonUsers,
		})
}

// getConfigMapHost
func (l *Labeler) getConfigMapHost(host *chiv1.ChiHost) map[string]string {
	return util.MergeStringMapsOverwrite(
		l.getHostScope(host, false),
		map[string]string{
			LabelConfigMap: labelConfigMapValueHost,
		})
}

// getServiceCHI
func (l *Labeler) getServiceCHI(chi *chiv1.ClickHouseInstallation) map[string]string {
	return util.MergeStringMapsOverwrite(
		l.getCHIScope(),
		map[string]string{
			LabelService: labelServiceValueCHI,
		})
}

// getServiceCluster
func (l *Labeler) getServiceCluster(cluster *chiv1.Cluster) map[string]string {
	return util.MergeStringMapsOverwrite(
		l.getClusterScope(cluster),
		map[string]string{
			LabelService: labelServiceValueCluster,
		})
}

// getServiceShard
func (l *Labeler) getServiceShard(shard *chiv1.ChiShard) map[string]string {
	return util.MergeStringMapsOverwrite(
		l.getShardScope(shard),
		map[string]string{
			LabelService: labelServiceValueShard,
		})
}

// getServiceHost
func (l *Labeler) getServiceHost(host *chiv1.ChiHost) map[string]string {
	return util.MergeStringMapsOverwrite(
		l.getHostScope(host, false),
		map[string]string{
			LabelService: labelServiceValueHost,
		})
}

// getCHIScope gets labels for CHI-scoped object
func (l *Labeler) getCHIScope() map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.filterOutPredefined(l.appendCHIProvidedTo(l.GetSelectorCHIScope()))
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
	return appendKeyReady(l.GetSelectorCHIScope())
}

// getClusterScope gets labels for Cluster-scoped object
func (l *Labeler) getClusterScope(cluster *chiv1.Cluster) map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.filterOutPredefined(l.appendCHIProvidedTo(getSelectorClusterScope(cluster)))
}

// getSelectorClusterScope gets labels to select a Cluster-scoped object
func getSelectorClusterScope(cluster *chiv1.Cluster) map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelNamespace:   labelsNamer.getNamePartNamespace(cluster),
		LabelAppName:     LabelAppValue,
		LabelCHIName:     labelsNamer.getNamePartCHIName(cluster),
		LabelClusterName: labelsNamer.getNamePartClusterName(cluster),
	}
}

// getSelectorClusterScope gets labels to select a ready-labelled Cluster-scoped object
func getSelectorClusterScopeReady(cluster *chiv1.Cluster) map[string]string {
	return appendKeyReady(getSelectorClusterScope(cluster))
}

// getShardScope gets labels for Shard-scoped object
func (l *Labeler) getShardScope(shard *chiv1.ChiShard) map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.filterOutPredefined(l.appendCHIProvidedTo(getSelectorShardScope(shard)))
}

// getSelectorShardScope gets labels to select a Shard-scoped object
func getSelectorShardScope(shard *chiv1.ChiShard) map[string]string {
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
func getSelectorShardScopeReady(shard *chiv1.ChiShard) map[string]string {
	return appendKeyReady(getSelectorShardScope(shard))
}

// getHostScope gets labels for Host-scoped object
func (l *Labeler) getHostScope(host *chiv1.ChiHost, applySupplementaryServiceLabels bool) map[string]string {
	// Combine generated labels and CHI-provided labels
	labels := GetSelectorHostScope(host)
	if chop.Config().Label.Runtime.AppendScope {
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
		labels = appendConfigLabels(host, labels)
	}
	return l.filterOutPredefined(l.appendCHIProvidedTo(labels))
}

func appendConfigLabels(host *chiv1.ChiHost, labels map[string]string) map[string]string {
	if host.HasCurStatefulSet() {
		if val, exists := host.CurStatefulSet.Labels[LabelZookeeperConfigVersion]; exists {
			labels[LabelZookeeperConfigVersion] = val
		}
		if val, exists := host.CurStatefulSet.Labels[LabelSettingsConfigVersion]; exists {
			labels[LabelSettingsConfigVersion] = val
		}
	}
	//labels[LabelZookeeperConfigVersion] = host.Config.ZookeeperFingerprint
	//labels[LabelSettingsConfigVersion] = host.Config.SettingsFingerprint
	return labels
}

// getHostScopeReady gets labels for Host-scoped object including Ready label
func (l *Labeler) getHostScopeReady(host *chiv1.ChiHost, applySupplementaryServiceLabels bool) map[string]string {
	return appendKeyReady(l.getHostScope(host, applySupplementaryServiceLabels))
}

// getHostScopeReclaimPolicy gets host scope labels with PVCReclaimPolicy from template
func (l *Labeler) getHostScopeReclaimPolicy(host *chiv1.ChiHost, template *chiv1.ChiVolumeClaimTemplate, applySupplementaryServiceLabels bool) map[string]string {
	return util.MergeStringMapsOverwrite(l.getHostScope(host, applySupplementaryServiceLabels), map[string]string{
		LabelPVCReclaimPolicyName: getPVCReclaimPolicy(host, template).String(),
	})
}

// getPV
func (l *Labeler) getPV(pv *v1.PersistentVolume, host *chiv1.ChiHost) map[string]string {
	return util.MergeStringMapsOverwrite(pv.Labels, l.getHostScope(host, false))
}

// getPVC
func (l *Labeler) getPVC(
	pvc *v1.PersistentVolumeClaim,
	host *chiv1.ChiHost,
	template *chiv1.ChiVolumeClaimTemplate,
) map[string]string {
	// Prepare main labels based on template
	labels := util.MergeStringMapsOverwrite(pvc.Labels, template.ObjectMeta.Labels)
	// Append reclaim policy labels
	return util.MergeStringMapsOverwrite(
		labels,
		l.getHostScopeReclaimPolicy(host, template, false),
	)
}

// GetReclaimPolicy gets reclaim policy from meta
func GetReclaimPolicy(meta meta.ObjectMeta) chiv1.PVCReclaimPolicy {
	defaultReclaimPolicy := chiv1.PVCReclaimPolicyDelete

	if value, ok := meta.Labels[LabelPVCReclaimPolicyName]; ok {
		reclaimPolicy := chiv1.NewPVCReclaimPolicyFromString(value)
		if reclaimPolicy.IsValid() {
			return reclaimPolicy
		}
	}

	return defaultReclaimPolicy
}

// GetSelectorHostScope gets labels to select a Host-scoped object
func GetSelectorHostScope(host *chiv1.ChiHost) map[string]string {
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

// filterOutPredefined filters out predefined values
func (l *Labeler) filterOutPredefined(m map[string]string) map[string]string {
	return util.CopyMapFilter(m, nil, []string{})
}

// appendCHIProvidedTo appends CHI-provided labels to labels set
func (l *Labeler) appendCHIProvidedTo(dst map[string]string) map[string]string {
	sourceLabels := util.CopyMapFilter(l.chi.Labels, chop.Config().Label.Include, chop.Config().Label.Exclude)
	return util.MergeStringMapsOverwrite(dst, sourceLabels)
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

// MakeSelectorFromObjectMeta makes selector from meta
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

// GetCHINameFromObjectMeta extracts CHI name from ObjectMeta. Based on labels.
func GetCHINameFromObjectMeta(meta *meta.ObjectMeta) (string, error) {
	if !util.MapHasKeys(meta.Labels, LabelCHIName) {
		return "", fmt.Errorf("can not find %s label in meta", LabelCHIName)
	}
	return meta.Labels[LabelCHIName], nil
}

// GetClusterNameFromObjectMeta extracts cluster name from ObjectMeta. Based on labels.
func GetClusterNameFromObjectMeta(meta *meta.ObjectMeta) (string, error) {
	if !util.MapHasKeys(meta.Labels, LabelClusterName) {
		return "", fmt.Errorf("can not find %s label in meta", LabelClusterName)
	}
	return meta.Labels[LabelClusterName], nil
}

// MakeObjectVersion makes object version label
func MakeObjectVersion(meta *meta.ObjectMeta, obj interface{}) {
	meta.Labels = util.MergeStringMapsOverwrite(
		meta.Labels,
		map[string]string{
			LabelObjectVersion: util.Fingerprint(obj),
		},
	)
}

// GetObjectVersion gets version of the object
func GetObjectVersion(meta meta.ObjectMeta) (string, bool) {
	label, ok := meta.Labels[LabelObjectVersion]
	return label, ok
}

// isObjectVersionLabelTheSame checks whether object version in meta.Labels is the same as provided value
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

// IsObjectTheSame checks whether objects are the same
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

// appendKeyReady sets "Ready" key to Ready state (used with labels and annotations)
func appendKeyReady(dst map[string]string) map[string]string {
	return util.MergeStringMapsOverwrite(
		dst,
		map[string]string{
			LabelReadyName: LabelReadyValueReady,
		},
	)
}

// deleteKeyReady sets "Ready" key to NotReady state (used with labels and annotations)
func deleteKeyReady(dst map[string]string) map[string]string {
	return util.MergeStringMapsOverwrite(
		dst,
		map[string]string{
			LabelReadyName: LabelReadyValueNotReady,
		},
	)
}

// hasKeyReady checks whether "Ready" key has Ready state (used with labels and annotations)
func hasKeyReady(src map[string]string) bool {
	if _, ok := src[LabelReadyName]; ok {
		return src[LabelReadyName] == LabelReadyValueReady
	}
	return false
}

// AppendLabelReady appends "Ready" label to ObjectMeta.Labels.
// Returns true in case label was not in place and was added.
func AppendLabelReady(meta *meta.ObjectMeta) bool {
	if meta == nil {
		// Nowhere to add to, not added
		return false
	}
	if hasKeyReady(meta.Labels) {
		// Already in place, value not added
		return false
	}
	// Need to add
	meta.Labels = appendKeyReady(meta.Labels)
	return true
}

// DeleteLabelReady deletes "Ready" label from ObjectMeta.Labels
// Returns true in case label was in place and was deleted.
func DeleteLabelReady(meta *meta.ObjectMeta) bool {
	if meta == nil {
		// Nowhere to delete from, not deleted
		return false
	}
	if hasKeyReady(meta.Labels) {
		// In place, need to delete
		meta.Labels = deleteKeyReady(meta.Labels)
		return true
	}
	// Not available, not deleted
	return false
}

// AppendAnnotationReady appends "Ready" annotation to ObjectMeta.Annotations
// Returns true in case annotation was not in place and was added.
func AppendAnnotationReady(meta *meta.ObjectMeta) bool {
	if meta == nil {
		// Nowhere to add to, not added
		return false
	}
	if hasKeyReady(meta.Annotations) {
		// Already in place, not added
		return false
	}
	// Need to add
	meta.Annotations = appendKeyReady(meta.Annotations)
	return true
}

// DeleteAnnotationReady deletes "Ready" annotation from ObjectMeta.Annotations
// Returns true in case annotation was in place and was deleted.
func DeleteAnnotationReady(meta *meta.ObjectMeta) bool {
	if meta == nil {
		// Nowhere to delete from, not deleted
		return false
	}
	if hasKeyReady(meta.Annotations) {
		// In place, need to delete
		meta.Annotations = deleteKeyReady(meta.Annotations)
		return true
	}
	// Not available, not deleted
	return false
}
