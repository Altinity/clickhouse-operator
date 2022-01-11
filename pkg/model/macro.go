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
	"strconv"
	"strings"

	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
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

// macrosEngine
type macrosEngine struct {
	names   *namer
	chi     *chop.ClickHouseInstallation
	cluster *chop.ChiCluster
	shard   *chop.ChiShard
	host    *chop.ChiHost
}

// macro
func macro(scope interface{}) *macrosEngine {
	m := new(macrosEngine)
	m.names = newNamer(namerContextNames)
	switch t := scope.(type) {
	case *chop.ClickHouseInstallation:
		m.chi = t
	case *chop.ChiCluster:
		m.cluster = t
	case *chop.ChiShard:
		m.shard = t
	case *chop.ChiHost:
		m.host = t
	}
	return m
}

// Line expands line with macros(es)
func (m *macrosEngine) Line(line string) string {
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
func (m *macrosEngine) Map(_map map[string]string) map[string]string {
	switch {
	case m.chi != nil:
		return m.newMapMacroReplacerChi().Replace(_map)
	case m.cluster != nil:
		return m.newMapMacroReplacerCluster().Replace(_map)
	case m.shard != nil:
		return m.newMapMacroReplacerShard().Replace(_map)
	case m.host != nil:
		return m.newMapMacroReplacerHost().Replace(_map)
	}
	return map[string]string{
		"unknown scope": "unknown scope",
	}
}

// newLineMacroReplacerChi
func (m *macrosEngine) newLineMacroReplacerChi() *strings.Replacer {
	return strings.NewReplacer(
		macrosNamespace, m.names.namePartNamespace(m.chi.Namespace),
		macrosChiName, m.names.namePartChiName(m.chi.Name),
		macrosChiID, m.names.namePartChiNameID(m.chi.Name),
	)
}

// newMapMacroReplacerChi
func (m *macrosEngine) newMapMacroReplacerChi() *util.MapReplacer {
	return util.NewMapReplacer(m.newLineMacroReplacerChi())
}

// newLineMacroReplacerCluster
func (m *macrosEngine) newLineMacroReplacerCluster() *strings.Replacer {
	return strings.NewReplacer(
		macrosNamespace, m.names.namePartNamespace(m.cluster.Address.Namespace),
		macrosChiName, m.names.namePartChiName(m.cluster.Address.CHIName),
		macrosChiID, m.names.namePartChiNameID(m.cluster.Address.CHIName),
		macrosClusterName, m.names.namePartClusterName(m.cluster.Address.ClusterName),
		macrosClusterID, m.names.namePartClusterNameID(m.cluster.Address.ClusterName),
		macrosClusterIndex, strconv.Itoa(m.cluster.Address.ClusterIndex),
	)
}

// newMapMacroReplacerCluster
func (m *macrosEngine) newMapMacroReplacerCluster() *util.MapReplacer {
	return util.NewMapReplacer(m.newLineMacroReplacerCluster())
}

// newLineMacroReplacerShard
func (m *macrosEngine) newLineMacroReplacerShard() *strings.Replacer {
	return strings.NewReplacer(
		macrosNamespace, m.names.namePartNamespace(m.shard.Address.Namespace),
		macrosChiName, m.names.namePartChiName(m.shard.Address.CHIName),
		macrosChiID, m.names.namePartChiNameID(m.shard.Address.CHIName),
		macrosClusterName, m.names.namePartClusterName(m.shard.Address.ClusterName),
		macrosClusterID, m.names.namePartClusterNameID(m.shard.Address.ClusterName),
		macrosClusterIndex, strconv.Itoa(m.shard.Address.ClusterIndex),
		macrosShardName, m.names.namePartShardName(m.shard.Address.ShardName),
		macrosShardID, m.names.namePartShardNameID(m.shard.Address.ShardName),
		macrosShardIndex, strconv.Itoa(m.shard.Address.ShardIndex),
	)
}

// newMapMacroReplacerShard
func (m *macrosEngine) newMapMacroReplacerShard() *util.MapReplacer {
	return util.NewMapReplacer(m.newLineMacroReplacerShard())
}

// clusterScopeIndexOfPreviousCycleTail gets cluster-scope index of previous cycle tail
func clusterScopeIndexOfPreviousCycleTail(host *chop.ChiHost) int {
	if host.Address.ClusterScopeCycleOffset == 0 {
		// This is the cycle head - the first host of the cycle
		// We need to point to previous host in this cluster - which would be previous cycle tail

		if host.Address.ClusterScopeIndex == 0 {
			// This is the very first host in the cluster - head of the first cycle
			// No previous host available, so just point to the same host, mainly because label must be an empty string
			// or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character
			// So we can't set it to "-1"
			return host.Address.ClusterScopeIndex
		}

		// This is head of non-first cycle, point to previous host in the cluster - which would be previous cycle tail
		return host.Address.ClusterScopeIndex - 1
	}

	// This is not cycle head - just point to the same host
	return host.Address.ClusterScopeIndex
}

// newLineMacroReplacerHost
func (m *macrosEngine) newLineMacroReplacerHost() *strings.Replacer {
	return strings.NewReplacer(
		macrosNamespace, m.names.namePartNamespace(m.host.Address.Namespace),
		macrosChiName, m.names.namePartChiName(m.host.Address.CHIName),
		macrosChiID, m.names.namePartChiNameID(m.host.Address.CHIName),
		macrosClusterName, m.names.namePartClusterName(m.host.Address.ClusterName),
		macrosClusterID, m.names.namePartClusterNameID(m.host.Address.ClusterName),
		macrosClusterIndex, strconv.Itoa(m.host.Address.ClusterIndex),
		macrosShardName, m.names.namePartShardName(m.host.Address.ShardName),
		macrosShardID, m.names.namePartShardNameID(m.host.Address.ShardName),
		macrosShardIndex, strconv.Itoa(m.host.Address.ShardIndex),
		macrosShardScopeIndex, strconv.Itoa(m.host.Address.ShardScopeIndex), // TODO use appropriate namePart function
		macrosReplicaName, m.names.namePartReplicaName(m.host.Address.ReplicaName),
		macrosReplicaID, m.names.namePartReplicaNameID(m.host.Address.ReplicaName),
		macrosReplicaIndex, strconv.Itoa(m.host.Address.ReplicaIndex),
		macrosReplicaScopeIndex, strconv.Itoa(m.host.Address.ReplicaScopeIndex), // TODO use appropriate namePart function
		macrosHostName, m.names.namePartHostName(m.host.Address.HostName),
		macrosHostID, m.names.namePartHostNameID(m.host.Address.HostName),
		macrosChiScopeIndex, strconv.Itoa(m.host.Address.CHIScopeIndex), // TODO use appropriate namePart function
		macrosChiScopeCycleIndex, strconv.Itoa(m.host.Address.CHIScopeCycleIndex), // TODO use appropriate namePart function
		macrosChiScopeCycleOffset, strconv.Itoa(m.host.Address.CHIScopeCycleOffset), // TODO use appropriate namePart function
		macrosClusterScopeIndex, strconv.Itoa(m.host.Address.ClusterScopeIndex), // TODO use appropriate namePart function
		macrosClusterScopeCycleIndex, strconv.Itoa(m.host.Address.ClusterScopeCycleIndex), // TODO use appropriate namePart function
		macrosClusterScopeCycleOffset, strconv.Itoa(m.host.Address.ClusterScopeCycleOffset), // TODO use appropriate namePart function
		macrosClusterScopeCycleHeadPointsToPreviousCycleTail, strconv.Itoa(clusterScopeIndexOfPreviousCycleTail(m.host)),
	)
}

// newMapMacroReplacerHost
func (m *macrosEngine) newMapMacroReplacerHost() *util.MapReplacer {
	return util.NewMapReplacer(m.newLineMacroReplacerHost())
}
