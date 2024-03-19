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
	"strconv"
	"strings"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
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
	names   *namer
	chi     *api.ClickHouseInstallation
	cluster *api.Cluster
	shard   *api.ChiShard
	host    *api.ChiHost
}

// Macro
func Macro(scope interface{}) *MacrosEngine {
	m := new(MacrosEngine)
	m.names = newNamer(namerContextNames)
	switch typed := scope.(type) {
	case *api.ClickHouseInstallation:
		m.chi = typed
	case *api.Cluster:
		m.cluster = typed
	case *api.ChiShard:
		m.shard = typed
	case *api.ChiHost:
		m.host = typed
	}
	return m
}

// Line expands line with macros(es)
func (m *MacrosEngine) Line(line string) string {
	switch {
	case m.chi != nil:
		return m.newLineMacroReplacerChi().Replace(line)
	case m.cluster != nil:
		return m.newLineMacroReplacerCluster().Replace(line)
	case m.shard != nil:
		return m.newLineMacroReplacerShard().Replace(line)
	case m.host != nil:
		return m.newLineMacroReplacerHost().Replace(line)
	}
	return "unknown scope"
}

// Map expands map with macros(es)
func (m *MacrosEngine) Map(_map map[string]string) map[string]string {
	switch {
	case m.chi != nil:
		return m.newMapMacroReplacerChi().Replace(_map)
	case m.cluster != nil:
		return m.newMapMacroReplacerCluster().Replace(_map)
	case m.shard != nil:
		return m.newMapMacroReplacerShard().Replace(_map)
	case m.host != nil:
		return m.newMapMacroReplacerHost().Replace(_map)
	default:
		return map[string]string{
			"unknown scope": "unknown scope",
		}
	}
}

// newLineMacroReplacerChi
func (m *MacrosEngine) newLineMacroReplacerChi() *strings.Replacer {
	return strings.NewReplacer(
		macrosNamespace, m.names.namePartNamespace(m.chi.Namespace),
		macrosChiName, m.names.namePartChiName(m.chi.Name),
		macrosChiID, m.names.namePartChiNameID(m.chi.Name),
	)
}

// newMapMacroReplacerChi
func (m *MacrosEngine) newMapMacroReplacerChi() *util.MapReplacer {
	return util.NewMapReplacer(m.newLineMacroReplacerChi())
}

// newLineMacroReplacerCluster
func (m *MacrosEngine) newLineMacroReplacerCluster() *strings.Replacer {
	return strings.NewReplacer(
		macrosNamespace, m.names.namePartNamespace(m.cluster.Runtime.Address.Namespace),
		macrosChiName, m.names.namePartChiName(m.cluster.Runtime.Address.CHIName),
		macrosChiID, m.names.namePartChiNameID(m.cluster.Runtime.Address.CHIName),
		macrosClusterName, m.names.namePartClusterName(m.cluster.Runtime.Address.ClusterName),
		macrosClusterID, m.names.namePartClusterNameID(m.cluster.Runtime.Address.ClusterName),
		macrosClusterIndex, strconv.Itoa(m.cluster.Runtime.Address.ClusterIndex),
	)
}

// newMapMacroReplacerCluster
func (m *MacrosEngine) newMapMacroReplacerCluster() *util.MapReplacer {
	return util.NewMapReplacer(m.newLineMacroReplacerCluster())
}

// newLineMacroReplacerShard
func (m *MacrosEngine) newLineMacroReplacerShard() *strings.Replacer {
	return strings.NewReplacer(
		macrosNamespace, m.names.namePartNamespace(m.shard.Runtime.Address.Namespace),
		macrosChiName, m.names.namePartChiName(m.shard.Runtime.Address.CHIName),
		macrosChiID, m.names.namePartChiNameID(m.shard.Runtime.Address.CHIName),
		macrosClusterName, m.names.namePartClusterName(m.shard.Runtime.Address.ClusterName),
		macrosClusterID, m.names.namePartClusterNameID(m.shard.Runtime.Address.ClusterName),
		macrosClusterIndex, strconv.Itoa(m.shard.Runtime.Address.ClusterIndex),
		macrosShardName, m.names.namePartShardName(m.shard.Runtime.Address.ShardName),
		macrosShardID, m.names.namePartShardNameID(m.shard.Runtime.Address.ShardName),
		macrosShardIndex, strconv.Itoa(m.shard.Runtime.Address.ShardIndex),
	)
}

// newMapMacroReplacerShard
func (m *MacrosEngine) newMapMacroReplacerShard() *util.MapReplacer {
	return util.NewMapReplacer(m.newLineMacroReplacerShard())
}

// clusterScopeIndexOfPreviousCycleTail gets cluster-scope index of previous cycle tail
func clusterScopeIndexOfPreviousCycleTail(host *api.ChiHost) int {
	if host.Runtime.Address.ClusterScopeCycleOffset == 0 {
		// This is the cycle head - the first host of the cycle
		// We need to point to previous host in this cluster - which would be previous cycle tail

		if host.Runtime.Address.ClusterScopeIndex == 0 {
			// This is the very first host in the cluster - head of the first cycle
			// No previous host available, so just point to the same host, mainly because label must be an empty string
			// or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character
			// So we can't set it to "-1"
			return host.Runtime.Address.ClusterScopeIndex
		}

		// This is head of non-first cycle, point to previous host in the cluster - which would be previous cycle tail
		return host.Runtime.Address.ClusterScopeIndex - 1
	}

	// This is not cycle head - just point to the same host
	return host.Runtime.Address.ClusterScopeIndex
}

// newLineMacroReplacerHost
func (m *MacrosEngine) newLineMacroReplacerHost() *strings.Replacer {
	return strings.NewReplacer(
		macrosNamespace, m.names.namePartNamespace(m.host.Runtime.Address.Namespace),
		macrosChiName, m.names.namePartChiName(m.host.Runtime.Address.CHIName),
		macrosChiID, m.names.namePartChiNameID(m.host.Runtime.Address.CHIName),
		macrosClusterName, m.names.namePartClusterName(m.host.Runtime.Address.ClusterName),
		macrosClusterID, m.names.namePartClusterNameID(m.host.Runtime.Address.ClusterName),
		macrosClusterIndex, strconv.Itoa(m.host.Runtime.Address.ClusterIndex),
		macrosShardName, m.names.namePartShardName(m.host.Runtime.Address.ShardName),
		macrosShardID, m.names.namePartShardNameID(m.host.Runtime.Address.ShardName),
		macrosShardIndex, strconv.Itoa(m.host.Runtime.Address.ShardIndex),
		macrosShardScopeIndex, strconv.Itoa(m.host.Runtime.Address.ShardScopeIndex), // TODO use appropriate namePart function
		macrosReplicaName, m.names.namePartReplicaName(m.host.Runtime.Address.ReplicaName),
		macrosReplicaID, m.names.namePartReplicaNameID(m.host.Runtime.Address.ReplicaName),
		macrosReplicaIndex, strconv.Itoa(m.host.Runtime.Address.ReplicaIndex),
		macrosReplicaScopeIndex, strconv.Itoa(m.host.Runtime.Address.ReplicaScopeIndex), // TODO use appropriate namePart function
		macrosHostName, m.names.namePartHostName(m.host.Runtime.Address.HostName),
		macrosHostID, m.names.namePartHostNameID(m.host.Runtime.Address.HostName),
		macrosChiScopeIndex, strconv.Itoa(m.host.Runtime.Address.CHIScopeIndex), // TODO use appropriate namePart function
		macrosChiScopeCycleIndex, strconv.Itoa(m.host.Runtime.Address.CHIScopeCycleIndex), // TODO use appropriate namePart function
		macrosChiScopeCycleOffset, strconv.Itoa(m.host.Runtime.Address.CHIScopeCycleOffset), // TODO use appropriate namePart function
		macrosClusterScopeIndex, strconv.Itoa(m.host.Runtime.Address.ClusterScopeIndex), // TODO use appropriate namePart function
		macrosClusterScopeCycleIndex, strconv.Itoa(m.host.Runtime.Address.ClusterScopeCycleIndex), // TODO use appropriate namePart function
		macrosClusterScopeCycleOffset, strconv.Itoa(m.host.Runtime.Address.ClusterScopeCycleOffset), // TODO use appropriate namePart function
		macrosClusterScopeCycleHeadPointsToPreviousCycleTail, strconv.Itoa(clusterScopeIndexOfPreviousCycleTail(m.host)),
	)
}

// newMapMacroReplacerHost
func (m *MacrosEngine) newMapMacroReplacerHost() *util.MapReplacer {
	return util.NewMapReplacer(m.newLineMacroReplacerHost())
}
