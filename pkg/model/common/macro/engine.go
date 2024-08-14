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

package macro

import (
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"strconv"
	"strings"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/common/namer/short"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Engine
type Engine struct {
	namer  *short.Namer
	macros types.List
	scope  any
}

// New
func New(macros types.List) *Engine {
	return &Engine{
		namer:  short.NewNamer(short.TargetNames),
		macros: macros,
		scope:  nil,
	}
}

func (e *Engine) Scope(scope any) *Engine {
	return &Engine{
		namer:  e.namer,
		macros: e.macros,
		scope:  scope,
	}
}

func (e *Engine) Get(macros string) string {
	return e.macros.Get(macros)
}

// Line expands line with macros(es)
func (e *Engine) Line(line string) string {
	switch t := e.scope.(type) {
	case api.ICustomResource:
		return e.newLineMacroReplacerCR(t).Replace(line)
	case api.ICluster:
		return e.newLineMacroReplacerCluster(t).Replace(line)
	case api.IShard:
		return e.newLineMacroReplacerShard(t).Replace(line)
	case api.IHost:
		return e.newLineMacroReplacerHost(t).Replace(line)
	default:
		return "unknown scope"
	}
}

// Map expands map with macros(es)
func (e *Engine) Map(_map map[string]string) map[string]string {
	switch t := e.scope.(type) {
	case api.ICustomResource:
		return e.newMapMacroReplacerCR(t).Replace(_map)
	case api.ICluster:
		return e.newMapMacroReplacerCluster(t).Replace(_map)
	case api.IShard:
		return e.newMapMacroReplacerShard(t).Replace(_map)
	case api.IHost:
		return e.newMapMacroReplacerHost(t).Replace(_map)
	default:
		return map[string]string{
			"unknown scope": "unknown scope",
		}
	}
}

// newLineMacroReplacerCR
func (e *Engine) newLineMacroReplacerCR(cr api.ICustomResource) *strings.Replacer {
	return strings.NewReplacer(
		e.Get(MacrosNamespace), e.namer.Name(short.Namespace, cr),
		e.Get(MacrosCRName), e.namer.Name(short.CRName, cr),
	)
}

// newMapMacroReplacerCR
func (e *Engine) newMapMacroReplacerCR(cr api.ICustomResource) *util.MapReplacer {
	return util.NewMapReplacer(e.newLineMacroReplacerCR(cr))
}

// newLineMacroReplacerCluster
func (e *Engine) newLineMacroReplacerCluster(cluster api.ICluster) *strings.Replacer {
	return strings.NewReplacer(
		e.Get(MacrosNamespace), e.namer.Name(short.Namespace, cluster),
		e.Get(MacrosCRName), e.namer.Name(short.CRName, cluster),
		e.Get(MacrosClusterName), e.namer.Name(short.ClusterName, cluster),
		e.Get(MacrosClusterIndex), strconv.Itoa(cluster.GetRuntime().GetAddress().GetClusterIndex()),
	)
}

// newMapMacroReplacerCluster
func (e *Engine) newMapMacroReplacerCluster(cluster api.ICluster) *util.MapReplacer {
	return util.NewMapReplacer(e.newLineMacroReplacerCluster(cluster))
}

// newLineMacroReplacerShard
func (e *Engine) newLineMacroReplacerShard(shard api.IShard) *strings.Replacer {
	return strings.NewReplacer(
		e.Get(MacrosNamespace), e.namer.Name(short.Namespace, shard),
		e.Get(MacrosCRName), e.namer.Name(short.CRName, shard),
		e.Get(MacrosClusterName), e.namer.Name(short.ClusterName, shard),
		e.Get(MacrosClusterIndex), strconv.Itoa(shard.GetRuntime().GetAddress().GetClusterIndex()),
		e.Get(MacrosShardName), e.namer.Name(short.ShardName, shard),
		e.Get(MacrosShardIndex), strconv.Itoa(shard.GetRuntime().GetAddress().GetShardIndex()),
	)
}

// newMapMacroReplacerShard
func (e *Engine) newMapMacroReplacerShard(shard api.IShard) *util.MapReplacer {
	return util.NewMapReplacer(e.newLineMacroReplacerShard(shard))
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
func (e *Engine) newLineMacroReplacerHost(host api.IHost) *strings.Replacer {
	return strings.NewReplacer(
		e.Get(MacrosNamespace), e.namer.Name(short.Namespace, host),
		e.Get(MacrosCRName), e.namer.Name(short.CRName, host),
		e.Get(MacrosClusterName), e.namer.Name(short.ClusterName, host),
		e.Get(MacrosClusterIndex), strconv.Itoa(host.GetRuntime().GetAddress().GetClusterIndex()),
		e.Get(MacrosShardName), e.namer.Name(short.ShardName, host),
		e.Get(MacrosShardIndex), strconv.Itoa(host.GetRuntime().GetAddress().GetShardIndex()),
		e.Get(MacrosShardScopeIndex), strconv.Itoa(host.GetRuntime().GetAddress().GetShardScopeIndex()), // TODO use appropriate namePart function
		e.Get(MacrosReplicaName), e.namer.Name(short.ReplicaName, host),
		e.Get(MacrosReplicaIndex), strconv.Itoa(host.GetRuntime().GetAddress().GetReplicaIndex()),
		e.Get(MacrosReplicaScopeIndex), strconv.Itoa(host.GetRuntime().GetAddress().GetReplicaScopeIndex()), // TODO use appropriate namePart function
		e.Get(MacrosHostName), e.namer.Name(short.HostName, host),
		e.Get(MacrosCRScopeIndex), strconv.Itoa(host.GetRuntime().GetAddress().GetCRScopeIndex()), // TODO use appropriate namePart function
		e.Get(MacrosCRScopeCycleIndex), strconv.Itoa(host.GetRuntime().GetAddress().GetCRScopeCycleIndex()), // TODO use appropriate namePart function
		e.Get(MacrosCRScopeCycleOffset), strconv.Itoa(host.GetRuntime().GetAddress().GetCRScopeCycleOffset()), // TODO use appropriate namePart function
		e.Get(MacrosClusterScopeIndex), strconv.Itoa(host.GetRuntime().GetAddress().GetClusterScopeIndex()), // TODO use appropriate namePart function
		e.Get(MacrosClusterScopeCycleIndex), strconv.Itoa(host.GetRuntime().GetAddress().GetClusterScopeCycleIndex()), // TODO use appropriate namePart function
		e.Get(MacrosClusterScopeCycleOffset), strconv.Itoa(host.GetRuntime().GetAddress().GetClusterScopeCycleOffset()), // TODO use appropriate namePart function
		e.Get(MacrosClusterScopeCycleHeadPointsToPreviousCycleTail), strconv.Itoa(clusterScopeIndexOfPreviousCycleTail(host)),
	)
}

// newMapMacroReplacerHost
func (e *Engine) newMapMacroReplacerHost(host api.IHost) *util.MapReplacer {
	return util.NewMapReplacer(e.newLineMacroReplacerHost(host))
}
