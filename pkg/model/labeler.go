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
	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kublabels "k8s.io/apimachinery/pkg/labels"
)

// Labeler is an entity which can label CHI artifacts
type Labeler struct {
	version string
	chi     *chi.ClickHouseInstallation
}

// NewLabeler creates new labeler with context
func NewLabeler(version string, chi *chi.ClickHouseInstallation) *Labeler {
	return &Labeler{
		version: version,
		chi:     chi,
	}
}

// getLabelsChiScope gets labels for CHI-scoped object
func (l *Labeler) getLabelsChiScope() map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.appendChiLabels(map[string]string{
		LabelApp:  LabelAppValue,
		LabelChop: l.version,
		LabelChi:  getNamePartChiName(l.chi),
	})
}

// getSelectorChiScope gets labels to select a CHI-scoped object
func (l *Labeler) getSelectorChiScope() map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelApp: LabelAppValue,
		// Skip chop
		LabelChi: getNamePartChiName(l.chi),
	}
}

// getLabelsClusterScope gets labels for Cluster-scoped object
func (l *Labeler) getLabelsClusterScope(cluster *chi.ChiCluster) map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.appendChiLabels(map[string]string{
		LabelApp:     LabelAppValue,
		LabelChop:    l.version,
		LabelChi:     getNamePartChiName(cluster),
		LabelCluster: getNamePartClusterName(cluster),
	})
}

// getSelectorClusterScope gets labels to select a Cluster-scoped object
func (l *Labeler) getSelectorClusterScope(cluster *chi.ChiCluster) map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelApp: LabelAppValue,
		// Skip chop
		LabelChi:     getNamePartChiName(cluster),
		LabelCluster: getNamePartClusterName(cluster),
	}
}

// getLabelsShardScope gets labels for Shard-scoped object
func (l *Labeler) getLabelsShardScope(shard *chi.ChiShard) map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.appendChiLabels(map[string]string{
		LabelApp:     LabelAppValue,
		LabelChop:    l.version,
		LabelChi:     getNamePartChiName(shard),
		LabelCluster: getNamePartClusterName(shard),
		LabelShard:   getNamePartShardName(shard),
	})
}

// getSelectorShardScope gets labels to select a Shard-scoped object
func (l *Labeler) getSelectorShardScope(shard *chi.ChiShard) map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelApp: LabelAppValue,
		// Skip chop
		LabelChi:     getNamePartChiName(shard),
		LabelCluster: getNamePartClusterName(shard),
		LabelShard:   getNamePartShardName(shard),
	}
}

// getLabelsHostScope gets labels for Host-scoped object
func (l *Labeler) getLabelsHostScope(host *chi.ChiHost, applySupplementaryServiceLabels bool) map[string]string {
	// Combine generated labels and CHI-provided labels
	labels := map[string]string{
		LabelApp:     LabelAppValue,
		LabelChop:    l.version,
		LabelChi:     getNamePartChiName(host),
		LabelCluster: getNamePartClusterName(host),
		LabelShard:   getNamePartShardName(host),
		LabelReplica: getNamePartReplicaName(host),
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
		LabelChi:     getNamePartChiName(host),
		LabelCluster: getNamePartClusterName(host),
		LabelShard:   getNamePartShardName(host),
		LabelReplica: getNamePartReplicaName(host),
		// skip StatefulSet
		// skip Zookeeper
	}
}

// TODO review usage
func GetSetFromObjectMeta(objMeta *meta.ObjectMeta) (kublabels.Set, error) {
	labelApp, ok1 := objMeta.Labels[LabelApp]
	// skip chop
	labelChi, ok2 := objMeta.Labels[LabelChi]

	if (!ok1) || (!ok2) {
		return nil, fmt.Errorf("unable to make set from object. Need to have at least APP and CHI. Labels: %v", objMeta.Labels)
	}

	set := kublabels.Set{
		LabelApp: labelApp,
		// skip chop
		LabelChi: labelChi,
	}

	// Add optional labels

	if labelCluster, ok := objMeta.Labels[LabelCluster]; ok {
		set[LabelCluster] = labelCluster
	}
	if labelShard, ok := objMeta.Labels[LabelShard]; ok {
		set[LabelShard] = labelShard
	}
	if labelReplica, ok := objMeta.Labels[LabelReplica]; ok {
		set[LabelReplica] = labelReplica
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
