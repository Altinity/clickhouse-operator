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
	"strconv"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/common/namer/short"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Engine
type Engine struct {
	namer      *short.Namer
	macrosList types.List
	scope      any
}

// New
func New(macrosList types.List) *Engine {
	return &Engine{
		namer:      short.NewNamer(short.TargetNames),
		macrosList: macrosList,
		scope:      nil,
	}
}

// Scope produces scoped macro engine
func (e *Engine) Scope(scope any) interfaces.IMacro {
	return &Engine{
		namer:      e.namer,
		macrosList: e.macrosList,
		scope:      scope,
	}
}

// Get gets macros by its name.
// Accepts macros name, returns macros expandable to be expanded, such as "{chi}" or "{chk}"
func (e *Engine) Get(macrosName string) string {
	return e.macrosList.Get(macrosName)
}

// Line expands line with macros(es)
func (e *Engine) Line(lineToExpand string) string {
	return e.Replacer().Line(lineToExpand)
}

// Map expands map with macros(es)
func (e *Engine) Map(mapToExpand map[string]string) map[string]string {
	return e.Replacer().Map(mapToExpand)
}

// Replacer produces ready-to-use replacer
func (e *Engine) Replacer() *util.Replacer {
	switch t := e.scope.(type) {
	case api.ICustomResource:
		return e.newReplacerCR(t)
	case api.ICluster:
		return e.newReplacerCluster(t)
	case api.IShard:
		return e.newReplacerShard(t)
	case api.IHost:
		return e.newReplacerHost(t)
	}
	return nil
}

// newReplacerCR
func (e *Engine) newReplacerCR(cr api.ICustomResource) *util.Replacer {
	return util.NewReplacer(map[string]string{
		e.Get(MacrosNamespace): e.namer.Name(short.Namespace, cr),
		e.Get(MacrosCRName):    e.namer.Name(short.CRName, cr),
	})
}

// newReplacerCluster
func (e *Engine) newReplacerCluster(cluster api.ICluster) *util.Replacer {
	return util.NewReplacer(map[string]string{
		e.Get(MacrosNamespace):    e.namer.Name(short.Namespace, cluster),
		e.Get(MacrosCRName):       e.namer.Name(short.CRName, cluster),
		e.Get(MacrosClusterName):  e.namer.Name(short.ClusterName, cluster),
		e.Get(MacrosClusterIndex): strconv.Itoa(cluster.GetRuntime().GetAddress().GetClusterIndex()),
	})
}

// newReplacerShard
func (e *Engine) newReplacerShard(shard api.IShard) *util.Replacer {
	return util.NewReplacer(map[string]string{
		e.Get(MacrosNamespace):    e.namer.Name(short.Namespace, shard),
		e.Get(MacrosCRName):       e.namer.Name(short.CRName, shard),
		e.Get(MacrosClusterName):  e.namer.Name(short.ClusterName, shard),
		e.Get(MacrosClusterIndex): strconv.Itoa(shard.GetRuntime().GetAddress().GetClusterIndex()),
		e.Get(MacrosShardName):    e.namer.Name(short.ShardName, shard),
		e.Get(MacrosShardIndex):   strconv.Itoa(shard.GetRuntime().GetAddress().GetShardIndex()),
	})
}

// newReplacerHost
func (e *Engine) newReplacerHost(host api.IHost) *util.Replacer {
	return util.NewReplacer(map[string]string{
		e.Get(MacrosNamespace):                                      e.namer.Name(short.Namespace, host),
		e.Get(MacrosCRName):                                         e.namer.Name(short.CRName, host),
		e.Get(MacrosClusterName):                                    e.namer.Name(short.ClusterName, host),
		e.Get(MacrosClusterIndex):                                   strconv.Itoa(host.GetRuntime().GetAddress().GetClusterIndex()),
		e.Get(MacrosShardName):                                      e.namer.Name(short.ShardName, host),
		e.Get(MacrosShardIndex):                                     strconv.Itoa(host.GetRuntime().GetAddress().GetShardIndex()),
		e.Get(MacrosShardScopeIndex):                                strconv.Itoa(host.GetRuntime().GetAddress().GetShardScopeIndex()), // TODO use appropriate namePart function
		e.Get(MacrosReplicaName):                                    e.namer.Name(short.ReplicaName, host),
		e.Get(MacrosReplicaIndex):                                   strconv.Itoa(host.GetRuntime().GetAddress().GetReplicaIndex()),
		e.Get(MacrosReplicaScopeIndex):                              strconv.Itoa(host.GetRuntime().GetAddress().GetReplicaScopeIndex()), // TODO use appropriate namePart function
		e.Get(MacrosHostName):                                       e.namer.Name(short.HostName, host),
		e.Get(MacrosCRScopeIndex):                                   strconv.Itoa(host.GetRuntime().GetAddress().GetCRScopeIndex()),            // TODO use appropriate namePart function
		e.Get(MacrosCRScopeCycleIndex):                              strconv.Itoa(host.GetRuntime().GetAddress().GetCRScopeCycleIndex()),       // TODO use appropriate namePart function
		e.Get(MacrosCRScopeCycleOffset):                             strconv.Itoa(host.GetRuntime().GetAddress().GetCRScopeCycleOffset()),      // TODO use appropriate namePart function
		e.Get(MacrosClusterScopeIndex):                              strconv.Itoa(host.GetRuntime().GetAddress().GetClusterScopeIndex()),       // TODO use appropriate namePart function
		e.Get(MacrosClusterScopeCycleIndex):                         strconv.Itoa(host.GetRuntime().GetAddress().GetClusterScopeCycleIndex()),  // TODO use appropriate namePart function
		e.Get(MacrosClusterScopeCycleOffset):                        strconv.Itoa(host.GetRuntime().GetAddress().GetClusterScopeCycleOffset()), // TODO use appropriate namePart function
		e.Get(MacrosClusterScopeCycleHeadPointsToPreviousCycleTail): strconv.Itoa(clusterScopeIndexOfPreviousCycleTail(host)),
	})
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
