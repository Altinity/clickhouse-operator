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

package labeler

import (
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer/short"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/volume"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// getCRScope gets labels for CR-scoped object
func (l *Labeler) getCRScope() map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.filterOutLabelsToBeSkipped(l.appendCRProvidedLabels(l.getSelectorCRScope()))
}

// getClusterScope gets labels for Cluster-scoped object
func (l *Labeler) getClusterScope(cluster api.ICluster) map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.filterOutLabelsToBeSkipped(l.appendCRProvidedLabels(l.getSelectorClusterScope(cluster)))
}

// getShardScope gets labels for Shard-scoped object
func (l *Labeler) getShardScope(shard api.IShard) map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.filterOutLabelsToBeSkipped(l.appendCRProvidedLabels(l.getSelectorShardScope(shard)))
}

// getHostScope gets labels for Host-scoped object
func (l *Labeler) getHostScope(host *api.Host, applySupplementaryServiceLabels bool) map[string]string {
	// Combine generated labels and CHI-provided labels
	labels := l.getSelectorHostScope(host)
	if l.AppendScope {
		// Optional labels
		labels[LabelShardScopeIndex] = short.NameLabel(short.ShardScopeIndex, host)
		labels[LabelReplicaScopeIndex] = short.NameLabel(short.ReplicaScopeIndex, host)
		labels[LabelCHIScopeIndex] = short.NameLabel(short.CHIScopeIndex, host)
		labels[LabelCHIScopeCycleSize] = short.NameLabel(short.CHIScopeCycleSize, host)
		labels[LabelCHIScopeCycleIndex] = short.NameLabel(short.CHIScopeCycleIndex, host)
		labels[LabelCHIScopeCycleOffset] = short.NameLabel(short.CHIScopeCycleOffset, host)
		labels[LabelClusterScopeIndex] = short.NameLabel(short.ClusterScopeIndex, host)
		labels[LabelClusterScopeCycleSize] = short.NameLabel(short.ClusterScopeCycleSize, host)
		labels[LabelClusterScopeCycleIndex] = short.NameLabel(short.ClusterScopeCycleIndex, host)
		labels[LabelClusterScopeCycleOffset] = short.NameLabel(short.ClusterScopeCycleOffset, host)
	}
	if applySupplementaryServiceLabels {
		// Optional labels
		// TODO
		// When we'll have ChkCluster Discovery functionality we can refactor this properly
		labels = appendConfigLabels(host, labels)
	}
	return l.filterOutLabelsToBeSkipped(l.appendCRProvidedLabels(labels))
}

// getHostScopeReady gets labels for Host-scoped object including Ready label
func (l *Labeler) getHostScopeReady(host *api.Host, applySupplementaryServiceLabels bool) map[string]string {
	return appendKeyReady(l.getHostScope(host, applySupplementaryServiceLabels))
}

// getHostScopeReclaimPolicy gets host scope labels with PVCReclaimPolicy from template
func (l *Labeler) getHostScopeReclaimPolicy(host *api.Host, template *api.VolumeClaimTemplate, applySupplementaryServiceLabels bool) map[string]string {
	return util.MergeStringMapsOverwrite(l.getHostScope(host, applySupplementaryServiceLabels), map[string]string{
		LabelPVCReclaimPolicyName: volume.GetPVCReclaimPolicy(host, template).String(),
	})
}

// filterOutLabelsToBeSkipped filters out predefined values
func (l *Labeler) filterOutLabelsToBeSkipped(m map[string]string) map[string]string {
	return util.CopyMapFilter(m, nil, []string{})
}

// appendCRProvidedLabels appends CHI-provided labels to labels set
func (l *Labeler) appendCRProvidedLabels(dst map[string]string) map[string]string {
	sourceLabels := util.CopyMapFilter(
		// Start with CR-provided labels
		l.cr.GetLabels(),
		// Respect include-exclude policies
		l.Include,
		l.Exclude,
	)
	// Merge on top of provided dst
	return util.MergeStringMapsOverwrite(dst, sourceLabels)
}
