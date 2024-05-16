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

package namer

import (
	"strconv"
	"strings"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

const (
	// MacrosNamespace is a sanitized namespace name where ClickHouseInstallation runs
	MacrosNamespace = "{namespace}"

	// MacrosChiName is a sanitized ClickHouseInstallation name
	MacrosChiName = "{chi}"
	// MacrosChiID is a sanitized ID made of original ClickHouseInstallation name
	MacrosChiID = "{chiID}"

	// MacrosClusterName is a sanitized cluster name
	MacrosClusterName = "{cluster}"
	// MacrosClusterID is a sanitized ID made of original cluster name
	MacrosClusterID = "{clusterID}"
	// MacrosClusterIndex is an index of the cluster in the CHI - integer number, converted into string
	MacrosClusterIndex = "{clusterIndex}"

	// MacrosShardName is a sanitized shard name
	MacrosShardName = "{shard}"
	// MacrosShardID is a sanitized ID made of original shard name
	MacrosShardID = "{shardID}"
	// MacrosShardIndex is an index of the shard in the cluster - integer number, converted into string
	MacrosShardIndex = "{shardIndex}"

	// MacrosReplicaName is a sanitized replica name
	MacrosReplicaName = "{replica}"
	// MacrosReplicaID is a sanitized ID made of original replica name
	MacrosReplicaID = "{replicaID}"
	// MacrosReplicaIndex is an index of the replica in the cluster - integer number, converted into string
	MacrosReplicaIndex = "{replicaIndex}"

	// MacrosHostName is a sanitized host name
	MacrosHostName = "{host}"
	// MacrosHostID is a sanitized ID made of original host name
	MacrosHostID = "{hostID}"
	// MacrosChiScopeIndex is an index of the host on the CHI-scope
	MacrosChiScopeIndex = "{chiScopeIndex}"
	// MacrosChiScopeCycleIndex is an index of the host in the CHI-scope cycle - integer number, converted into string
	MacrosChiScopeCycleIndex = "{chiScopeCycleIndex}"
	// MacrosChiScopeCycleOffset is an offset of the host in the CHI-scope cycle - integer number, converted into string
	MacrosChiScopeCycleOffset = "{chiScopeCycleOffset}"
	// MacrosClusterScopeIndex is an index of the host on the cluster-scope
	MacrosClusterScopeIndex = "{clusterScopeIndex}"
	// MacrosClusterScopeCycleIndex is an index of the host in the Cluster-scope cycle - integer number, converted into string
	MacrosClusterScopeCycleIndex = "{clusterScopeCycleIndex}"
	// MacrosClusterScopeCycleOffset is an offset of the host in the Cluster-scope cycle - integer number, converted into string
	MacrosClusterScopeCycleOffset = "{clusterScopeCycleOffset}"
	// MacrosShardScopeIndex is an index of the host on the shard-scope
	MacrosShardScopeIndex = "{shardScopeIndex}"
	// MacrosReplicaScopeIndex is an index of the host on the replica-scope
	MacrosReplicaScopeIndex = "{replicaScopeIndex}"
	// MacrosClusterScopeCycleHeadPointsToPreviousCycleTail is {clusterScopeIndex} of previous Cycle Tail
	MacrosClusterScopeCycleHeadPointsToPreviousCycleTail = "{clusterScopeCycleHeadPointsToPreviousCycleTail}"
)

// MacrosEngine
type MacrosEngine struct {
	names *namer
	scope any
}

// Macro
func Macro(scope any) *MacrosEngine {
	m := new(MacrosEngine)
	m.names = NewNamer(NamerContextNames)
	m.scope = scope
	return m
}

// Line expands line with macros(es)
func (m *MacrosEngine) Line(line string) string {
	switch t := m.scope.(type) {
	case api.ICustomResource:
		return m.newLineMacroReplacerChi(t).Replace(line)
	case api.ICluster:
		return m.newLineMacroReplacerCluster(t).Replace(line)
	case api.IShard:
		return m.newLineMacroReplacerShard(t).Replace(line)
	case api.IHost:
		return m.newLineMacroReplacerHost(t).Replace(line)
	default:
		return "unknown scope"
	}
}

// Map expands map with macros(es)
func (m *MacrosEngine) Map(_map map[string]string) map[string]string {
	switch t := m.scope.(type) {
	case api.ICustomResource:
		return m.newMapMacroReplacerChi(t).Replace(_map)
	case api.ICluster:
		return m.newMapMacroReplacerCluster(t).Replace(_map)
	case api.IShard:
		return m.newMapMacroReplacerShard(t).Replace(_map)
	case api.IHost:
		return m.newMapMacroReplacerHost(t).Replace(_map)
	default:
		return map[string]string{
			"unknown scope": "unknown scope",
		}
	}
}

// newLineMacroReplacerChi
func (m *MacrosEngine) newLineMacroReplacerChi(chi api.ICustomResource) *strings.Replacer {
	return strings.NewReplacer(
		MacrosNamespace, m.names.namePartNamespace(chi.GetNamespace()),
		MacrosChiName, m.names.namePartChiName(chi.GetName()),
		MacrosChiID, m.names.namePartChiNameID(chi.GetName()),
	)
}

// newMapMacroReplacerChi
func (m *MacrosEngine) newMapMacroReplacerChi(chi api.ICustomResource) *util.MapReplacer {
	return util.NewMapReplacer(m.newLineMacroReplacerChi(chi))
}

// newLineMacroReplacerCluster
func (m *MacrosEngine) newLineMacroReplacerCluster(cluster api.ICluster) *strings.Replacer {
	return strings.NewReplacer(
		MacrosNamespace, m.names.namePartNamespace(cluster.GetRuntime().GetAddress().GetNamespace()),
		MacrosChiName, m.names.namePartChiName(cluster.GetRuntime().GetAddress().GetRootName()),
		MacrosChiID, m.names.namePartChiNameID(cluster.GetRuntime().GetAddress().GetRootName()),
		MacrosClusterName, m.names.namePartClusterName(cluster.GetRuntime().GetAddress().GetClusterName()),
		MacrosClusterID, m.names.namePartClusterNameID(cluster.GetRuntime().GetAddress().GetClusterName()),
		MacrosClusterIndex, strconv.Itoa(cluster.GetRuntime().GetAddress().GetClusterIndex()),
	)
}

// newMapMacroReplacerCluster
func (m *MacrosEngine) newMapMacroReplacerCluster(cluster api.ICluster) *util.MapReplacer {
	return util.NewMapReplacer(m.newLineMacroReplacerCluster(cluster))
}

// newLineMacroReplacerShard
func (m *MacrosEngine) newLineMacroReplacerShard(shard api.IShard) *strings.Replacer {
	return strings.NewReplacer(
		MacrosNamespace, m.names.namePartNamespace(shard.GetRuntime().GetAddress().GetNamespace()),
		MacrosChiName, m.names.namePartChiName(shard.GetRuntime().GetAddress().GetRootName()),
		MacrosChiID, m.names.namePartChiNameID(shard.GetRuntime().GetAddress().GetRootName()),
		MacrosClusterName, m.names.namePartClusterName(shard.GetRuntime().GetAddress().GetClusterName()),
		MacrosClusterID, m.names.namePartClusterNameID(shard.GetRuntime().GetAddress().GetClusterName()),
		MacrosClusterIndex, strconv.Itoa(shard.GetRuntime().GetAddress().GetClusterIndex()),
		MacrosShardName, m.names.namePartShardName(shard.GetRuntime().GetAddress().GetShardName()),
		MacrosShardID, m.names.namePartShardNameID(shard.GetRuntime().GetAddress().GetShardName()),
		MacrosShardIndex, strconv.Itoa(shard.GetRuntime().GetAddress().GetShardIndex()),
	)
}

// newMapMacroReplacerShard
func (m *MacrosEngine) newMapMacroReplacerShard(shard api.IShard) *util.MapReplacer {
	return util.NewMapReplacer(m.newLineMacroReplacerShard(shard))
}

// clusterScopeIndexOfPreviousCycleTail gets cluster-scope index of previous cycle tail
func clusterScopeIndexOfPreviousCycleTail(host api.IHost) int {
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
func (m *MacrosEngine) newLineMacroReplacerHost(host api.IHost) *strings.Replacer {
	return strings.NewReplacer(
		MacrosNamespace, m.names.namePartNamespace(host.GetRuntime().GetAddress().GetNamespace()),
		MacrosChiName, m.names.namePartChiName(host.GetRuntime().GetAddress().GetRootName()),
		MacrosChiID, m.names.namePartChiNameID(host.GetRuntime().GetAddress().GetRootName()),
		MacrosClusterName, m.names.namePartClusterName(host.GetRuntime().GetAddress().GetClusterName()),
		MacrosClusterID, m.names.namePartClusterNameID(host.GetRuntime().GetAddress().GetClusterName()),
		MacrosClusterIndex, strconv.Itoa(host.GetRuntime().GetAddress().GetClusterIndex()),
		MacrosShardName, m.names.namePartShardName(host.GetRuntime().GetAddress().GetShardName()),
		MacrosShardID, m.names.namePartShardNameID(host.GetRuntime().GetAddress().GetShardName()),
		MacrosShardIndex, strconv.Itoa(host.GetRuntime().GetAddress().GetShardIndex()),
		MacrosShardScopeIndex, strconv.Itoa(host.GetRuntime().GetAddress().GetShardScopeIndex()), // TODO use appropriate namePart function
		MacrosReplicaName, m.names.namePartReplicaName(host.GetRuntime().GetAddress().GetReplicaName()),
		MacrosReplicaID, m.names.namePartReplicaNameID(host.GetRuntime().GetAddress().GetReplicaName()),
		MacrosReplicaIndex, strconv.Itoa(host.GetRuntime().GetAddress().GetReplicaIndex()),
		MacrosReplicaScopeIndex, strconv.Itoa(host.GetRuntime().GetAddress().GetReplicaScopeIndex()), // TODO use appropriate namePart function
		MacrosHostName, m.names.namePartHostName(host.GetRuntime().GetAddress().GetHostName()),
		MacrosHostID, m.names.namePartHostNameID(host.GetRuntime().GetAddress().GetHostName()),
		MacrosChiScopeIndex, strconv.Itoa(host.GetRuntime().GetAddress().GetRootScopeIndex()), // TODO use appropriate namePart function
		MacrosChiScopeCycleIndex, strconv.Itoa(host.GetRuntime().GetAddress().GetRootScopeCycleIndex()), // TODO use appropriate namePart function
		MacrosChiScopeCycleOffset, strconv.Itoa(host.GetRuntime().GetAddress().GetRootScopeCycleOffset()), // TODO use appropriate namePart function
		MacrosClusterScopeIndex, strconv.Itoa(host.GetRuntime().GetAddress().GetClusterScopeIndex()), // TODO use appropriate namePart function
		MacrosClusterScopeCycleIndex, strconv.Itoa(host.GetRuntime().GetAddress().GetClusterScopeCycleIndex()), // TODO use appropriate namePart function
		MacrosClusterScopeCycleOffset, strconv.Itoa(host.GetRuntime().GetAddress().GetClusterScopeCycleOffset()), // TODO use appropriate namePart function
		MacrosClusterScopeCycleHeadPointsToPreviousCycleTail, strconv.Itoa(clusterScopeIndexOfPreviousCycleTail(host)),
	)
}

// newMapMacroReplacerHost
func (m *MacrosEngine) newMapMacroReplacerHost(host api.IHost) *util.MapReplacer {
	return util.NewMapReplacer(m.newLineMacroReplacerHost(host))
}
