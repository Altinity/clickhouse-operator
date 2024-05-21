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
	"github.com/altinity/clickhouse-operator/pkg/model/common/namer/short"
)

// getSelectorCRScope gets labels to select a CR-scoped object
func (l *Labeler) getSelectorCRScope() map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelNamespace: short.NameLabel(short.Namespace, l.cr),
		LabelAppName:   LabelAppValue,
		LabelCHIName:   short.NameLabel(short.CRName, l.cr),
	}
}

// getSelectorCRScopeReady gets labels to select a ready-labelled CR-scoped object
func (l *Labeler) getSelectorCRScopeReady() map[string]string {
	return appendKeyReady(l.getSelectorCRScope())
}

// getSelectorClusterScope gets labels to select a Cluster-scoped object
func (l *Labeler) getSelectorClusterScope(cluster api.ICluster) map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelNamespace:   short.NameLabel(short.Namespace, cluster),
		LabelAppName:     LabelAppValue,
		LabelCHIName:     short.NameLabel(short.CRName, cluster),
		LabelClusterName: short.NameLabel(short.ClusterName, cluster),
	}
}

// getSelectorClusterScopeReady gets labels to select a ready-labelled Cluster-scoped object
func (l *Labeler) getSelectorClusterScopeReady(cluster api.ICluster) map[string]string {
	return appendKeyReady(l.getSelectorClusterScope(cluster))
}

// getSelectorShardScope gets labels to select a Shard-scoped object
func (l *Labeler) getSelectorShardScope(shard api.IShard) map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelNamespace:   short.NameLabel(short.Namespace, shard),
		LabelAppName:     LabelAppValue,
		LabelCHIName:     short.NameLabel(short.CRName, shard),
		LabelClusterName: short.NameLabel(short.ClusterName, shard),
		LabelShardName:   short.NameLabel(short.ShardName, shard),
	}
}

// getSelectorShardScopeReady gets labels to select a ready-labelled Shard-scoped object
func (l *Labeler) getSelectorShardScopeReady(shard api.IShard) map[string]string {
	return appendKeyReady(l.getSelectorShardScope(shard))
}

// getSelectorHostScope gets labels to select a Host-scoped object
func (l *Labeler) getSelectorHostScope(host *api.Host) map[string]string {
	// Do not include CHI-provided labels
	return map[string]string{
		LabelNamespace:   short.NameLabel(short.Namespace, host),
		LabelAppName:     LabelAppValue,
		LabelCHIName:     short.NameLabel(short.CRName, host),
		LabelClusterName: short.NameLabel(short.ClusterName, host),
		LabelShardName:   short.NameLabel(short.ShardName, host),
		LabelReplicaName: short.NameLabel(short.ReplicaName, host),
	}
}
