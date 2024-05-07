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
	"fmt"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sLabels "k8s.io/apimachinery/pkg/labels"

	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Set of kubernetes labels used by the operator
const (
	// Main labels

	LabelReadyName                    = clickhouse_altinity_com.APIGroupName + "/" + "ready"
	LabelReadyValueReady              = "yes"
	LabelReadyValueNotReady           = "no"
	LabelAppName                      = clickhouse_altinity_com.APIGroupName + "/" + "app"
	LabelAppValue                     = "chop"
	LabelCHOP                         = clickhouse_altinity_com.APIGroupName + "/" + "chop"
	LabelCHOPCommit                   = clickhouse_altinity_com.APIGroupName + "/" + "chop-commit"
	LabelCHOPDate                     = clickhouse_altinity_com.APIGroupName + "/" + "chop-date"
	LabelNamespace                    = clickhouse_altinity_com.APIGroupName + "/" + "namespace"
	LabelCHIName                      = clickhouse_altinity_com.APIGroupName + "/" + "chi"
	LabelClusterName                  = clickhouse_altinity_com.APIGroupName + "/" + "cluster"
	LabelShardName                    = clickhouse_altinity_com.APIGroupName + "/" + "shard"
	LabelReplicaName                  = clickhouse_altinity_com.APIGroupName + "/" + "replica"
	LabelConfigMap                    = clickhouse_altinity_com.APIGroupName + "/" + "ConfigMap"
	labelConfigMapValueCHICommon      = "ChiCommon"
	labelConfigMapValueCHICommonUsers = "ChiCommonUsers"
	labelConfigMapValueHost           = "Host"
	LabelService                      = clickhouse_altinity_com.APIGroupName + "/" + "Service"
	labelServiceValueCHI              = "chi"
	labelServiceValueCluster          = "cluster"
	labelServiceValueShard            = "shard"
	labelServiceValueHost             = "host"
	LabelPVCReclaimPolicyName         = clickhouse_altinity_com.APIGroupName + "/" + "reclaimPolicy"

	// Supplementary service labels - used to cooperate with k8s

	LabelZookeeperConfigVersion = clickhouse_altinity_com.APIGroupName + "/" + "zookeeper-version"
	LabelSettingsConfigVersion  = clickhouse_altinity_com.APIGroupName + "/" + "settings-version"
	LabelObjectVersion          = clickhouse_altinity_com.APIGroupName + "/" + "object-version"

	// Optional labels

	LabelShardScopeIndex         = clickhouse_altinity_com.APIGroupName + "/" + "shardScopeIndex"
	LabelReplicaScopeIndex       = clickhouse_altinity_com.APIGroupName + "/" + "replicaScopeIndex"
	LabelCHIScopeIndex           = clickhouse_altinity_com.APIGroupName + "/" + "chiScopeIndex"
	LabelCHIScopeCycleSize       = clickhouse_altinity_com.APIGroupName + "/" + "chiScopeCycleSize"
	LabelCHIScopeCycleIndex      = clickhouse_altinity_com.APIGroupName + "/" + "chiScopeCycleIndex"
	LabelCHIScopeCycleOffset     = clickhouse_altinity_com.APIGroupName + "/" + "chiScopeCycleOffset"
	LabelClusterScopeIndex       = clickhouse_altinity_com.APIGroupName + "/" + "clusterScopeIndex"
	LabelClusterScopeCycleSize   = clickhouse_altinity_com.APIGroupName + "/" + "clusterScopeCycleSize"
	LabelClusterScopeCycleIndex  = clickhouse_altinity_com.APIGroupName + "/" + "clusterScopeCycleIndex"
	LabelClusterScopeCycleOffset = clickhouse_altinity_com.APIGroupName + "/" + "clusterScopeCycleOffset"
)

// Labeler is an entity which can label CHI artifacts
type Labeler struct {
	chi chi
}

// NewLabeler creates new labeler with context
func NewLabeler(chi chi) *Labeler {
	return &Labeler{
		chi: chi,
	}
}

// GetConfigMapCHICommon
func (l *Labeler) GetConfigMapCHICommon() map[string]string {
	return util.MergeStringMapsOverwrite(
		l.getCHIScope(),
		map[string]string{
			LabelConfigMap: labelConfigMapValueCHICommon,
		})
}

// GetConfigMapCHICommonUsers
func (l *Labeler) GetConfigMapCHICommonUsers() map[string]string {
	return util.MergeStringMapsOverwrite(
		l.getCHIScope(),
		map[string]string{
			LabelConfigMap: labelConfigMapValueCHICommonUsers,
		})
}

// GetConfigMapHost
func (l *Labeler) GetConfigMapHost(host *api.ChiHost) map[string]string {
	return util.MergeStringMapsOverwrite(
		l.GetHostScope(host, false),
		map[string]string{
			LabelConfigMap: labelConfigMapValueHost,
		})
}

// GetServiceCHI
func (l *Labeler) GetServiceCHI(chi chi) map[string]string {
	return util.MergeStringMapsOverwrite(
		l.getCHIScope(),
		map[string]string{
			LabelService: labelServiceValueCHI,
		})
}

// GetServiceCluster
func (l *Labeler) GetServiceCluster(cluster cluster) map[string]string {
	return util.MergeStringMapsOverwrite(
		l.GetClusterScope(cluster),
		map[string]string{
			LabelService: labelServiceValueCluster,
		})
}

// GetServiceShard
func (l *Labeler) GetServiceShard(shard shard) map[string]string {
	return util.MergeStringMapsOverwrite(
		l.getShardScope(shard),
		map[string]string{
			LabelService: labelServiceValueShard,
		})
}

// GetServiceHost
func (l *Labeler) GetServiceHost(host *api.ChiHost) map[string]string {
	return util.MergeStringMapsOverwrite(
		l.GetHostScope(host, false),
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

// GetSelectorCHIScopeReady gets labels to select a ready-labelled CHI-scoped object
func (l *Labeler) GetSelectorCHIScopeReady() map[string]string {
	return appendKeyReady(l.GetSelectorCHIScope())
}

// GetClusterScope gets labels for Cluster-scoped object
func (l *Labeler) GetClusterScope(cluster cluster) map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.filterOutPredefined(l.appendCHIProvidedTo(GetSelectorClusterScope(cluster)))
}

// GetSelectorClusterScope gets labels to select a Cluster-scoped object
func GetSelectorClusterScope(cluster cluster) map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelNamespace:   labelsNamer.getNamePartNamespace(cluster),
		LabelAppName:     LabelAppValue,
		LabelCHIName:     labelsNamer.getNamePartCHIName(cluster),
		LabelClusterName: labelsNamer.getNamePartClusterName(cluster),
	}
}

// GetSelectorClusterScope gets labels to select a ready-labelled Cluster-scoped object
func GetSelectorClusterScopeReady(cluster cluster) map[string]string {
	return appendKeyReady(GetSelectorClusterScope(cluster))
}

// getShardScope gets labels for Shard-scoped object
func (l *Labeler) getShardScope(shard shard) map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.filterOutPredefined(l.appendCHIProvidedTo(getSelectorShardScope(shard)))
}

// getSelectorShardScope gets labels to select a Shard-scoped object
func getSelectorShardScope(shard shard) map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelNamespace:   labelsNamer.getNamePartNamespace(shard),
		LabelAppName:     LabelAppValue,
		LabelCHIName:     labelsNamer.getNamePartCHIName(shard),
		LabelClusterName: labelsNamer.getNamePartClusterName(shard),
		LabelShardName:   labelsNamer.getNamePartShardName(shard),
	}
}

// GetSelectorShardScopeReady gets labels to select a ready-labelled Shard-scoped object
func GetSelectorShardScopeReady(shard shard) map[string]string {
	return appendKeyReady(getSelectorShardScope(shard))
}

// GetHostScope gets labels for Host-scoped object
func (l *Labeler) GetHostScope(host *api.ChiHost, applySupplementaryServiceLabels bool) map[string]string {
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
		// When we'll have ChkCluster Discovery functionality we can refactor this properly
		labels = appendConfigLabels(host, labels)
	}
	return l.filterOutPredefined(l.appendCHIProvidedTo(labels))
}

func appendConfigLabels(host *api.ChiHost, labels map[string]string) map[string]string {
	if !host.HasCurStatefulSet() {
		return labels
	}
	// Have CurStatefulSet
	stsLabels := host.Runtime.CurStatefulSet.GetLabels()
	if stsLabels == nil {
		return labels
	}
	// Have labels
	if val, exists := stsLabels[LabelZookeeperConfigVersion]; exists {
		labels[LabelZookeeperConfigVersion] = val
	}
	if val, exists := stsLabels[LabelSettingsConfigVersion]; exists {
		labels[LabelSettingsConfigVersion] = val
	}
	//labels[LabelZookeeperConfigVersion] = host.Config.ZookeeperFingerprint
	//labels[LabelSettingsConfigVersion] = host.Config.SettingsFingerprint
	return labels
}

// GetHostScopeReady gets labels for Host-scoped object including Ready label
func (l *Labeler) GetHostScopeReady(host *api.ChiHost, applySupplementaryServiceLabels bool) map[string]string {
	return appendKeyReady(l.GetHostScope(host, applySupplementaryServiceLabels))
}

// getHostScopeReclaimPolicy gets host scope labels with PVCReclaimPolicy from template
func (l *Labeler) getHostScopeReclaimPolicy(host *api.ChiHost, template *api.VolumeClaimTemplate, applySupplementaryServiceLabels bool) map[string]string {
	return util.MergeStringMapsOverwrite(l.GetHostScope(host, applySupplementaryServiceLabels), map[string]string{
		LabelPVCReclaimPolicyName: getPVCReclaimPolicy(host, template).String(),
	})
}

// GetPV
func (l *Labeler) GetPV(pv *core.PersistentVolume, host *api.ChiHost) map[string]string {
	return util.MergeStringMapsOverwrite(pv.GetLabels(), l.GetHostScope(host, false))
}

// GetPVC
func (l *Labeler) GetPVC(
	pvc *core.PersistentVolumeClaim,
	host *api.ChiHost,
	template *api.VolumeClaimTemplate,
) map[string]string {
	// Prepare main labels based on template
	labels := util.MergeStringMapsOverwrite(pvc.GetLabels(), template.ObjectMeta.GetLabels())
	// Append reclaim policy labels
	return util.MergeStringMapsOverwrite(
		labels,
		l.getHostScopeReclaimPolicy(host, template, false),
	)
}

// GetReclaimPolicy gets reclaim policy from meta
func GetReclaimPolicy(meta meta.Object) api.PVCReclaimPolicy {
	defaultReclaimPolicy := api.PVCReclaimPolicyDelete

	labels := meta.GetLabels()
	if labels == nil {
		return defaultReclaimPolicy
	}

	if value, ok := labels[LabelPVCReclaimPolicyName]; ok {
		reclaimPolicy := api.NewPVCReclaimPolicyFromString(value)
		if reclaimPolicy.IsValid() {
			return reclaimPolicy
		}
	}

	return defaultReclaimPolicy
}

// GetSelectorHostScope gets labels to select a Host-scoped object
func GetSelectorHostScope(host *api.ChiHost) map[string]string {
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
	sourceLabels := util.CopyMapFilter(l.chi.GetLabels(), chop.Config().Label.Include, chop.Config().Label.Exclude)
	return util.MergeStringMapsOverwrite(dst, sourceLabels)
}

// makeSetFromObjectMeta makes k8sLabels.Set from ObjectMeta
func makeSetFromObjectMeta(meta meta.Object) (k8sLabels.Set, error) {
	// Check mandatory labels are in place
	if !util.MapHasKeys(meta.GetLabels(), LabelNamespace, LabelAppName, LabelCHIName) {
		return nil, fmt.Errorf(
			"UNABLE to make set from object. Need to have at least labels '%s', '%s' and '%s'. Available Labels: %v",
			LabelNamespace, LabelAppName, LabelCHIName, meta.GetLabels(),
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

	set := k8sLabels.Set{}
	util.MergeStringMapsOverwrite(set, meta.GetLabels(), labels...)

	// skip StatefulSet
	// skip Zookeeper

	return set, nil
}

// MakeSelectorFromObjectMeta makes selector from meta
// TODO review usage
func MakeSelectorFromObjectMeta(meta meta.Object) (k8sLabels.Selector, error) {
	set, err := makeSetFromObjectMeta(meta)
	if err != nil {
		// Unable to make set
		return nil, err
	}
	return k8sLabels.SelectorFromSet(set), nil
}

// IsCHOPGeneratedObject check whether object is generated by an operator. Check is label-based
func IsCHOPGeneratedObject(meta meta.Object) bool {
	labels := meta.GetLabels()
	if !util.MapHasKeys(labels, LabelAppName) {
		return false
	}
	return labels[LabelAppName] == LabelAppValue
}

// GetCHINameFromObjectMeta extracts CHI name from ObjectMeta. Based on labels.
func GetCHINameFromObjectMeta(meta meta.Object) (string, error) {
	labels := meta.GetLabels()
	if !util.MapHasKeys(labels, LabelCHIName) {
		return "", fmt.Errorf("can not find %s label in meta", LabelCHIName)
	}
	return labels[LabelCHIName], nil
}

// GetClusterNameFromObjectMeta extracts cluster name from ObjectMeta. Based on labels.
func GetClusterNameFromObjectMeta(meta meta.Object) (string, error) {
	labels := meta.GetLabels()
	if !util.MapHasKeys(labels, LabelClusterName) {
		return "", fmt.Errorf("can not find %s label in meta", LabelClusterName)
	}
	return labels[LabelClusterName], nil
}

// MakeObjectVersion makes object version label
func MakeObjectVersion(meta meta.Object, obj interface{}) {
	meta.SetLabels(
		util.MergeStringMapsOverwrite(
			meta.GetLabels(),
			map[string]string{
				LabelObjectVersion: util.Fingerprint(obj),
			},
		),
	)
}

// GetObjectVersion gets version of the object
func GetObjectVersion(meta meta.Object) (string, bool) {
	labels := meta.GetLabels()
	if labels == nil {
		return "", false
	}
	label, ok := labels[LabelObjectVersion]
	return label, ok
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
func AppendLabelReady(meta meta.Object) bool {
	if meta == nil {
		// Nowhere to add to, not added
		return false
	}
	if hasKeyReady(meta.GetLabels()) {
		// Already in place, value not added
		return false
	}
	// Need to add
	meta.SetLabels(appendKeyReady(meta.GetLabels()))
	return true
}

// DeleteLabelReady deletes "Ready" label from ObjectMeta.Labels
// Returns true in case label was in place and was deleted.
func DeleteLabelReady(meta meta.Object) bool {
	if meta == nil {
		// Nowhere to delete from, not deleted
		return false
	}
	if hasKeyReady(meta.GetLabels()) {
		// In place, need to delete
		meta.SetLabels(deleteKeyReady(meta.GetLabels()))
		return true
	}
	// Not available, not deleted
	return false
}

// AppendAnnotationReady appends "Ready" annotation to ObjectMeta.Annotations
// Returns true in case annotation was not in place and was added.
func AppendAnnotationReady(meta meta.Object) bool {
	if meta == nil {
		// Nowhere to add to, not added
		return false
	}
	if hasKeyReady(meta.GetAnnotations()) {
		// Already in place, not added
		return false
	}
	// Need to add
	meta.SetAnnotations(appendKeyReady(meta.GetAnnotations()))
	return true
}

// DeleteAnnotationReady deletes "Ready" annotation from ObjectMeta.Annotations
// Returns true in case annotation was in place and was deleted.
func DeleteAnnotationReady(meta meta.Object) bool {
	if meta == nil {
		// Nowhere to delete from, not deleted
		return false
	}
	if hasKeyReady(meta.GetAnnotations()) {
		// In place, need to delete
		meta.SetAnnotations(deleteKeyReady(meta.GetAnnotations()))
		return true
	}
	// Not available, not deleted
	return false
}
