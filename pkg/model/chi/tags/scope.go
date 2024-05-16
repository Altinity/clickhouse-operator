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

package tags

import (
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/volume"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// getCHIScope gets labels for CHI-scoped object
func (l *Labeler) getCHIScope() map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.filterOutPredefined(l.appendCHIProvidedTo(l.GetSelectorCHIScope()))
}

// getClusterScope gets labels for Cluster-scoped object
func (l *Labeler) getClusterScope(cluster api.ICluster) map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.filterOutPredefined(l.appendCHIProvidedTo(getSelectorClusterScope(cluster)))
}

// getShardScope gets labels for Shard-scoped object
func (l *Labeler) getShardScope(shard api.IShard) map[string]string {
	// Combine generated labels and CHI-provided labels
	return l.filterOutPredefined(l.appendCHIProvidedTo(getSelectorShardScope(shard)))
}

// getHostScope gets labels for Host-scoped object
func (l *Labeler) getHostScope(host *api.Host, applySupplementaryServiceLabels bool) map[string]string {
	// Combine generated labels and CHI-provided labels
	labels := GetSelectorHostScope(host)
	if chop.Config().Label.Runtime.AppendScope {
		// Optional labels
		labels[LabelShardScopeIndex] = namer.NamePartLabel(namer.NamePartShardScopeIndex, host)
		labels[LabelReplicaScopeIndex] = namer.NamePartLabel(namer.NamePartReplicaScopeIndex, host)
		labels[LabelCHIScopeIndex] = namer.NamePartLabel(namer.NamePartCHIScopeIndex, host)
		labels[LabelCHIScopeCycleSize] = namer.NamePartLabel(namer.NamePartCHIScopeCycleSize, host)
		labels[LabelCHIScopeCycleIndex] = namer.NamePartLabel(namer.NamePartCHIScopeCycleIndex, host)
		labels[LabelCHIScopeCycleOffset] = namer.NamePartLabel(namer.NamePartCHIScopeCycleOffset, host)
		labels[LabelClusterScopeIndex] = namer.NamePartLabel(namer.NamePartClusterScopeIndex, host)
		labels[LabelClusterScopeCycleSize] = namer.NamePartLabel(namer.NamePartClusterScopeCycleSize, host)
		labels[LabelClusterScopeCycleIndex] = namer.NamePartLabel(namer.NamePartClusterScopeCycleIndex, host)
		labels[LabelClusterScopeCycleOffset] = namer.NamePartLabel(namer.NamePartClusterScopeCycleOffset, host)
	}
	if applySupplementaryServiceLabels {
		// Optional labels
		// TODO
		// When we'll have ChkCluster Discovery functionality we can refactor this properly
		labels = appendConfigLabels(host, labels)
	}
	return l.filterOutPredefined(l.appendCHIProvidedTo(labels))
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
