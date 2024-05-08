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
	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"strconv"
	"strings"

	"github.com/altinity/clickhouse-operator/pkg/util"
)

const (
	// macrosNamespace is a sanitized namespace name where ClickHouseInstallation runs
	macrosNamespace = "{namespace}"

	// macrosChiName is a sanitized ClickHouseInstallation name
	macrosChiName = "{chi}"
	// macrosChiID is a sanitized ID made of original ClickHouseInstallation name
	macrosChiID = "{chiID}"

	// macrosClusterName is a sanitized cluster name
	macrosClusterName = "{cluster}"
	// macrosClusterID is a sanitized ID made of original cluster name
	macrosClusterID = "{clusterID}"
	// macrosClusterIndex is an index of the cluster in the CHI - integer number, converted into string
	macrosClusterIndex = "{clusterIndex}"

	// macrosShardName is a sanitized shard name
	macrosShardName = "{shard}"
	// macrosShardID is a sanitized ID made of original shard name
	macrosShardID = "{shardID}"
	// macrosShardIndex is an index of the shard in the cluster - integer number, converted into string
	macrosShardIndex = "{shardIndex}"

	// macrosReplicaName is a sanitized replica name
	macrosReplicaName = "{replica}"
	// macrosReplicaID is a sanitized ID made of original replica name
	macrosReplicaID = "{replicaID}"
	// macrosReplicaIndex is an index of the replica in the cluster - integer number, converted into string
	macrosReplicaIndex = "{replicaIndex}"

	// macrosHostName is a sanitized host name
	macrosHostName = "{host}"
	// macrosHostID is a sanitized ID made of original host name
	macrosHostID = "{hostID}"
	// macrosChiScopeIndex is an index of the host on the CHI-scope
	macrosChiScopeIndex = "{chiScopeIndex}"
	// macrosChiScopeCycleIndex is an index of the host in the CHI-scope cycle - integer number, converted into string
	macrosChiScopeCycleIndex = "{chiScopeCycleIndex}"
	// macrosChiScopeCycleOffset is an offset of the host in the CHI-scope cycle - integer number, converted into string
	macrosChiScopeCycleOffset = "{chiScopeCycleOffset}"
	// macrosClusterScopeIndex is an index of the host on the cluster-scope
	macrosClusterScopeIndex = "{clusterScopeIndex}"
	// macrosClusterScopeCycleIndex is an index of the host in the Cluster-scope cycle - integer number, converted into string
	macrosClusterScopeCycleIndex = "{clusterScopeCycleIndex}"
	// macrosClusterScopeCycleOffset is an offset of the host in the Cluster-scope cycle - integer number, converted into string
	macrosClusterScopeCycleOffset = "{clusterScopeCycleOffset}"
	// macrosShardScopeIndex is an index of the host on the shard-scope
	macrosShardScopeIndex = "{shardScopeIndex}"
	// macrosReplicaScopeIndex is an index of the host on the replica-scope
	macrosReplicaScopeIndex = "{replicaScopeIndex}"
	// macrosClusterScopeCycleHeadPointsToPreviousCycleTail is {clusterScopeIndex} of previous Cycle Tail
	macrosClusterScopeCycleHeadPointsToPreviousCycleTail = "{clusterScopeCycleHeadPointsToPreviousCycleTail}"
)

// MacrosEngine
type MacrosEngine struct {
	names *namer
	scope any
}

// Macro
func Macro(scope any) *MacrosEngine {
	m := new(MacrosEngine)
	m.names = newNamer(namerContextNames)
	m.scope = scope
	return m
}

// Line expands line with macros(es)
func (m *MacrosEngine) Line(line string) string {
	switch t := m.scope.(type) {
	case v1.IChi:
		return m.newLineMacroReplacerChi(t).Replace(line)
	case v1.ICluster:
		return m.newLineMacroReplacerCluster(t).Replace(line)
	case v1.IShard:
		return m.newLineMacroReplacerShard(t).Replace(line)
	case v1.IHost:
		return m.newLineMacroReplacerHost(t).Replace(line)
	default:
		return "unknown scope"
	}
}

// Map expands map with macros(es)
func (m *MacrosEngine) Map(_map map[string]string) map[string]string {
	switch t := m.scope.(type) {
	case v1.IChi:
		return m.newMapMacroReplacerChi(t).Replace(_map)
	case v1.ICluster:
		return m.newMapMacroReplacerCluster(t).Replace(_map)
	case v1.IShard:
		return m.newMapMacroReplacerShard(t).Replace(_map)
	case v1.IHost:
		return m.newMapMacroReplacerHost(t).Replace(_map)
	default:
		return map[string]string{
			"unknown scope": "unknown scope",
		}
	}
}

// newLineMacroReplacerChi
func (m *MacrosEngine) newLineMacroReplacerChi(chi v1.IChi) *strings.Replacer {
	return strings.NewReplacer(
		macrosNamespace, m.names.namePartNamespace(chi.GetNamespace()),
		macrosChiName, m.names.namePartChiName(chi.GetName()),
		macrosChiID, m.names.namePartChiNameID(chi.GetName()),
	)
}

// newMapMacroReplacerChi
func (m *MacrosEngine) newMapMacroReplacerChi(chi v1.IChi) *util.MapReplacer {
	return util.NewMapReplacer(m.newLineMacroReplacerChi(chi))
}

// newLineMacroReplacerCluster
func (m *MacrosEngine) newLineMacroReplacerCluster(cluster v1.ICluster) *strings.Replacer {
	return strings.NewReplacer(
		macrosNamespace, m.names.namePartNamespace(cluster.GetRuntime().GetAddress().GetNamespace()),
		macrosChiName, m.names.namePartChiName(cluster.GetRuntime().GetAddress().GetRootName()),
		macrosChiID, m.names.namePartChiNameID(cluster.GetRuntime().GetAddress().GetRootName()),
		macrosClusterName, m.names.namePartClusterName(cluster.GetRuntime().GetAddress().GetClusterName()),
		macrosClusterID, m.names.namePartClusterNameID(cluster.GetRuntime().GetAddress().GetClusterName()),
		macrosClusterIndex, strconv.Itoa(cluster.GetRuntime().GetAddress().GetClusterIndex()),
	)
}

// newMapMacroReplacerCluster
func (m *MacrosEngine) newMapMacroReplacerCluster(cluster v1.ICluster) *util.MapReplacer {
	return util.NewMapReplacer(m.newLineMacroReplacerCluster(cluster))
}

// newLineMacroReplacerShard
func (m *MacrosEngine) newLineMacroReplacerShard(shard v1.IShard) *strings.Replacer {
	return strings.NewReplacer(
		macrosNamespace, m.names.namePartNamespace(shard.GetRuntime().GetAddress().GetNamespace()),
		macrosChiName, m.names.namePartChiName(shard.GetRuntime().GetAddress().GetRootName()),
		macrosChiID, m.names.namePartChiNameID(shard.GetRuntime().GetAddress().GetRootName()),
		macrosClusterName, m.names.namePartClusterName(shard.GetRuntime().GetAddress().GetClusterName()),
		macrosClusterID, m.names.namePartClusterNameID(shard.GetRuntime().GetAddress().GetClusterName()),
		macrosClusterIndex, strconv.Itoa(shard.GetRuntime().GetAddress().GetClusterIndex()),
		macrosShardName, m.names.namePartShardName(shard.GetRuntime().GetAddress().GetShardName()),
		macrosShardID, m.names.namePartShardNameID(shard.GetRuntime().GetAddress().GetShardName()),
		macrosShardIndex, strconv.Itoa(shard.GetRuntime().GetAddress().GetShardIndex()),
	)
}

// newMapMacroReplacerShard
func (m *MacrosEngine) newMapMacroReplacerShard(shard v1.IShard) *util.MapReplacer {
	return util.NewMapReplacer(m.newLineMacroReplacerShard(shard))
}

// clusterScopeIndexOfPreviousCycleTail gets cluster-scope index of previous cycle tail
func clusterScopeIndexOfPreviousCycleTail(host v1.IHost) int {
	if host.GetRuntime().GetAddress().GetClusterScopeCycleOffset() == 0 {
		// This is the cycle head - the first host of the cycle
		// We need to point to previous host in this cluster - which would be previous cycle tail

		if host.GetRuntime().GetAddress().GetClusterScopeIndex() == 0 {
			// This is the very first host in the cluster - head of the first cycle
			// No previous host available, so just point to the same host, mainly because label must be an empty string
			// or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character
			// So we can't set it to "-1"
			return host.GetRuntime().GetAddress().GetClusterScopeIndex()
		}

		// This is head of non-first cycle, point to previous host in the cluster - which would be previous cycle tail
		return host.GetRuntime().GetAddress().GetClusterScopeIndex() - 1
	}

	// This is not cycle head - just point to the same host
	return host.GetRuntime().GetAddress().GetClusterScopeIndex()
}

// newLineMacroReplacerHost
func (m *MacrosEngine) newLineMacroReplacerHost(host v1.IHost) *strings.Replacer {
	return strings.NewReplacer(
		macrosNamespace, m.names.namePartNamespace(host.GetRuntime().GetAddress().GetNamespace()),
		macrosChiName, m.names.namePartChiName(host.GetRuntime().GetAddress().GetRootName()),
		macrosChiID, m.names.namePartChiNameID(host.GetRuntime().GetAddress().GetRootName()),
		macrosClusterName, m.names.namePartClusterName(host.GetRuntime().GetAddress().GetClusterName()),
		macrosClusterID, m.names.namePartClusterNameID(host.GetRuntime().GetAddress().GetClusterName()),
		macrosClusterIndex, strconv.Itoa(host.GetRuntime().GetAddress().GetClusterIndex()),
		macrosShardName, m.names.namePartShardName(host.GetRuntime().GetAddress().GetShardName()),
		macrosShardID, m.names.namePartShardNameID(host.GetRuntime().GetAddress().GetShardName()),
		macrosShardIndex, strconv.Itoa(host.GetRuntime().GetAddress().GetShardIndex()),
		macrosShardScopeIndex, strconv.Itoa(host.GetRuntime().GetAddress().GetShardScopeIndex()), // TODO use appropriate namePart function
		macrosReplicaName, m.names.namePartReplicaName(host.GetRuntime().GetAddress().GetReplicaName()),
		macrosReplicaID, m.names.namePartReplicaNameID(host.GetRuntime().GetAddress().GetReplicaName()),
		macrosReplicaIndex, strconv.Itoa(host.GetRuntime().GetAddress().GetReplicaIndex()),
		macrosReplicaScopeIndex, strconv.Itoa(host.GetRuntime().GetAddress().GetReplicaScopeIndex()), // TODO use appropriate namePart function
		macrosHostName, m.names.namePartHostName(host.GetRuntime().GetAddress().GetHostName()),
		macrosHostID, m.names.namePartHostNameID(host.GetRuntime().GetAddress().GetHostName()),
		macrosChiScopeIndex, strconv.Itoa(host.GetRuntime().GetAddress().GetRootScopeIndex()), // TODO use appropriate namePart function
		macrosChiScopeCycleIndex, strconv.Itoa(host.GetRuntime().GetAddress().GetRootScopeCycleIndex()), // TODO use appropriate namePart function
		macrosChiScopeCycleOffset, strconv.Itoa(host.GetRuntime().GetAddress().GetRootScopeCycleOffset()), // TODO use appropriate namePart function
		macrosClusterScopeIndex, strconv.Itoa(host.GetRuntime().GetAddress().GetClusterScopeIndex()), // TODO use appropriate namePart function
		macrosClusterScopeCycleIndex, strconv.Itoa(host.GetRuntime().GetAddress().GetClusterScopeCycleIndex()), // TODO use appropriate namePart function
		macrosClusterScopeCycleOffset, strconv.Itoa(host.GetRuntime().GetAddress().GetClusterScopeCycleOffset()), // TODO use appropriate namePart function
		macrosClusterScopeCycleHeadPointsToPreviousCycleTail, strconv.Itoa(clusterScopeIndexOfPreviousCycleTail(host)),
	)
}

// newMapMacroReplacerHost
func (m *MacrosEngine) newMapMacroReplacerHost(host v1.IHost) *util.MapReplacer {
	return util.NewMapReplacer(m.newLineMacroReplacerHost(host))
}
