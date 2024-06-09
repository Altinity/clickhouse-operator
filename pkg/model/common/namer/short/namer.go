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

package short

import (
	"strconv"
	"strings"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type Target string

type Namer struct {
	target Target
}

// NewNamer creates new namer with specified context
func NewNamer(target Target) *Namer {
	return &Namer{
		target: target,
	}
}

var labelNamer = NewNamer(TargetLabels)

func NameLabel(what NameType, params ...any) string {
	return labelNamer.Name(what, params...)
}

// namePartNamespace
func (n *Namer) namePartNamespace(name string) string {
	return sanitize(util.StringHead(name, n.lenCR()))
}

// namePartCRName
func (n *Namer) namePartCRName(name string) string {
	return sanitize(util.StringHead(name, n.lenCR()))
}

// namePartClusterName
func (n *Namer) namePartClusterName(name string) string {
	return sanitize(util.StringHead(name, n.lenCluster()))
}

// namePartShardName
func (n *Namer) namePartShardName(name string) string {
	return sanitize(util.StringHead(name, n.lenShard()))
}

// namePartReplicaName
func (n *Namer) namePartReplicaName(name string) string {
	return sanitize(util.StringHead(name, n.lenReplica()))
}

// namePartHostName
func (n *Namer) namePartHostName(name string) string {
	return sanitize(util.StringHead(name, n.lenReplica()))
}

func (n *Namer) Name(what NameType, params ...any) string {
	switch what {
	case Namespace:
		return n.getNamePartNamespace(params[0])
	case CRName:
		return n.getNamePartCRName(params[0])
	case ClusterName:
		return n.getNamePartClusterName(params[0])
	case ShardName:
		return n.getNamePartShardName(params[0])
	case ReplicaName:
		host := params[0].(*api.Host)
		return n.getNamePartReplicaName(host)
	case HostName:
		host := params[0].(*api.Host)
		return n.getNamePartHostName(host)

	case CRScopeCycleSize:
		host := params[0].(*api.Host)
		return n.getNamePartCRScopeCycleSize(host)
	case CRScopeCycleIndex:
		host := params[0].(*api.Host)
		return n.getNamePartCRScopeCycleIndex(host)
	case CRScopeCycleOffset:
		host := params[0].(*api.Host)
		return n.getNamePartCRScopeCycleOffset(host)

	case ClusterScopeCycleSize:
		host := params[0].(*api.Host)
		return n.getNamePartClusterScopeCycleSize(host)
	case ClusterScopeCycleIndex:
		host := params[0].(*api.Host)
		return n.getNamePartClusterScopeCycleIndex(host)
	case ClusterScopeCycleOffset:
		host := params[0].(*api.Host)
		return n.getNamePartClusterScopeCycleOffset(host)

	case CRScopeIndex:
		host := params[0].(*api.Host)
		return n.getNamePartCRScopeIndex(host)
	case ClusterScopeIndex:
		host := params[0].(*api.Host)
		return n.getNamePartClusterScopeIndex(host)
	case ShardScopeIndex:
		host := params[0].(*api.Host)
		return n.getNamePartShardScopeIndex(host)
	case ReplicaScopeIndex:
		host := params[0].(*api.Host)
		return n.getNamePartReplicaScopeIndex(host)
	}
	panic("unknown name part")
}

// getNamePartNamespace
func (n *Namer) getNamePartNamespace(obj interface{}) string {
	switch obj.(type) {
	case api.ICustomResource:
		cr := obj.(api.ICustomResource)
		return n.namePartCRName(cr.GetNamespace())
	case api.ICluster:
		cluster := obj.(api.ICluster)
		return n.namePartCRName(cluster.GetRuntime().GetAddress().GetNamespace())
	case api.IShard:
		shard := obj.(api.IShard)
		return n.namePartCRName(shard.GetRuntime().GetAddress().GetNamespace())
	case *api.Host:
		host := obj.(*api.Host)
		return n.namePartCRName(host.GetRuntime().GetAddress().GetNamespace())
	}

	return "ERROR"
}

// getNamePartCRName
func (n *Namer) getNamePartCRName(obj interface{}) string {
	switch obj.(type) {
	case api.ICustomResource:
		cr := obj.(api.ICustomResource)
		return n.namePartCRName(cr.GetName())
	case api.ICluster:
		cluster := obj.(api.ICluster)
		return n.namePartCRName(cluster.GetRuntime().GetAddress().GetCRName())
	case api.IShard:
		shard := obj.(api.IShard)
		return n.namePartCRName(shard.GetRuntime().GetAddress().GetCRName())
	case *api.Host:
		host := obj.(*api.Host)
		return n.namePartCRName(host.GetRuntime().GetAddress().GetCRName())
	}

	return "ERROR"
}

// getNamePartClusterName
func (n *Namer) getNamePartClusterName(obj interface{}) string {
	switch obj.(type) {
	case api.ICluster:
		cluster := obj.(api.ICluster)
		return n.namePartClusterName(cluster.GetRuntime().GetAddress().GetClusterName())
	case api.IShard:
		shard := obj.(api.IShard)
		return n.namePartClusterName(shard.GetRuntime().GetAddress().GetClusterName())
	case *api.Host:
		host := obj.(*api.Host)
		return n.namePartClusterName(host.GetRuntime().GetAddress().GetClusterName())
	}

	return "ERROR"
}

// getNamePartShardName
func (n *Namer) getNamePartShardName(obj interface{}) string {
	switch obj.(type) {
	case api.IShard:
		shard := obj.(api.IShard)
		return n.namePartShardName(shard.GetRuntime().GetAddress().GetShardName())
	case *api.Host:
		host := obj.(*api.Host)
		return n.namePartShardName(host.GetRuntime().GetAddress().GetShardName())
	}

	return "ERROR"
}

// getNamePartReplicaName
func (n *Namer) getNamePartReplicaName(host *api.Host) string {
	return n.namePartReplicaName(host.GetRuntime().GetAddress().GetReplicaName())
}

// getNamePartHostName
func (n *Namer) getNamePartHostName(host *api.Host) string {
	return n.namePartHostName(host.GetRuntime().GetAddress().GetHostName())
}

// getNamePartCRScopeCycleSize
func (n *Namer) getNamePartCRScopeCycleSize(host *api.Host) string {
	return strconv.Itoa(host.GetRuntime().GetAddress().GetCRScopeCycleSize())
}

// getNamePartCRScopeCycleIndex
func (n *Namer) getNamePartCRScopeCycleIndex(host *api.Host) string {
	return strconv.Itoa(host.GetRuntime().GetAddress().GetCRScopeCycleIndex())
}

// getNamePartCRScopeCycleOffset
func (n *Namer) getNamePartCRScopeCycleOffset(host *api.Host) string {
	return strconv.Itoa(host.GetRuntime().GetAddress().GetCRScopeCycleOffset())
}

// getNamePartClusterScopeCycleSize
func (n *Namer) getNamePartClusterScopeCycleSize(host *api.Host) string {
	return strconv.Itoa(host.GetRuntime().GetAddress().GetClusterScopeCycleSize())
}

// getNamePartClusterScopeCycleIndex
func (n *Namer) getNamePartClusterScopeCycleIndex(host *api.Host) string {
	return strconv.Itoa(host.GetRuntime().GetAddress().GetClusterScopeCycleIndex())
}

// getNamePartClusterScopeCycleOffset
func (n *Namer) getNamePartClusterScopeCycleOffset(host *api.Host) string {
	return strconv.Itoa(host.GetRuntime().GetAddress().GetClusterScopeCycleOffset())
}

// getNamePartCRScopeIndex
func (n *Namer) getNamePartCRScopeIndex(host *api.Host) string {
	return strconv.Itoa(host.GetRuntime().GetAddress().GetCRScopeIndex())
}

// getNamePartClusterScopeIndex
func (n *Namer) getNamePartClusterScopeIndex(host *api.Host) string {
	return strconv.Itoa(host.GetRuntime().GetAddress().GetClusterScopeIndex())
}

// getNamePartShardScopeIndex
func (n *Namer) getNamePartShardScopeIndex(host *api.Host) string {
	return strconv.Itoa(host.GetRuntime().GetAddress().GetShardScopeIndex())
}

// getNamePartReplicaScopeIndex
func (n *Namer) getNamePartReplicaScopeIndex(host *api.Host) string {
	return strconv.Itoa(host.GetRuntime().GetAddress().GetReplicaScopeIndex())
}

func (n *Namer) lenCR() int {
	if n.target == TargetLabels {
		return namePartCRMaxLenLabelsCtx
	} else {
		return namePartCRMaxLenNamesCtx
	}
}

func (n *Namer) lenCluster() int {
	if n.target == TargetLabels {
		return namePartClusterMaxLenLabelsCtx
	} else {
		return namePartClusterMaxLenNamesCtx
	}
}

func (n *Namer) lenShard() int {
	if n.target == TargetLabels {
		return namePartShardMaxLenLabelsCtx
	} else {
		return namePartShardMaxLenNamesCtx
	}

}

func (n *Namer) lenReplica() int {
	if n.target == TargetLabels {
		return namePartReplicaMaxLenLabelsCtx
	} else {
		return namePartReplicaMaxLenNamesCtx
	}
}

// sanitize makes string fulfil kubernetes naming restrictions
// String can't end with '-', '_' and '.'
func sanitize(s string) string {
	return strings.Trim(s, "-_.")
}
