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

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// namePartNamespace
func (n *namer) namePartNamespace(name string) string {
	return sanitize(util.StringHead(name, n.lenCHI()))
}

// namePartChiName
func (n *namer) namePartChiName(name string) string {
	return sanitize(util.StringHead(name, n.lenCHI()))
}

// namePartChiNameID
func (n *namer) namePartChiNameID(name string) string {
	return util.CreateStringID(name, n.lenCHI())
}

// namePartClusterName
func (n *namer) namePartClusterName(name string) string {
	return sanitize(util.StringHead(name, n.lenCluster()))
}

// namePartClusterNameID
func (n *namer) namePartClusterNameID(name string) string {
	return util.CreateStringID(name, n.lenCluster())
}

// namePartShardName
func (n *namer) namePartShardName(name string) string {
	return sanitize(util.StringHead(name, n.lenShard()))
}

// namePartShardNameID
func (n *namer) namePartShardNameID(name string) string {
	return util.CreateStringID(name, n.lenShard())
}

// namePartReplicaName
func (n *namer) namePartReplicaName(name string) string {
	return sanitize(util.StringHead(name, n.lenReplica()))
}

// namePartReplicaNameID
func (n *namer) namePartReplicaNameID(name string) string {
	return util.CreateStringID(name, n.lenReplica())
}

// namePartHostName
func (n *namer) namePartHostName(name string) string {
	return sanitize(util.StringHead(name, n.lenReplica()))
}

// namePartHostNameID
func (n *namer) namePartHostNameID(name string) string {
	return util.CreateStringID(name, n.lenReplica())
}

var labelsNamer = NewNamer(TargetLabels)

func NamePartLabel(what NamePartType, params ...any) string {
	return labelsNamer.NamePart(what, params...)
}

func (n *namer) NamePart(what NamePartType, params ...any) string {
	switch what {
	case NamePartNamespace:
		return n.getNamePartNamespace(params[0])
	case NamePartCHIName:
		return n.getNamePartCHIName(params[0])
	case NamePartClusterName:
		return n.getNamePartClusterName(params[0])
	case NamePartShardName:
		return n.getNamePartShardName(params[0])
	case NamePartReplicaName:
		host := params[0].(*api.Host)
		return n.getNamePartReplicaName(host)
	case NamePartHostName:
		host := params[0].(*api.Host)
		return n.getNamePartHostName(host)

	case NamePartCHIScopeCycleSize:
		host := params[0].(*api.Host)
		return n.getNamePartCHIScopeCycleSize(host)
	case NamePartCHIScopeCycleIndex:
		host := params[0].(*api.Host)
		return n.getNamePartCHIScopeCycleIndex(host)
	case NamePartCHIScopeCycleOffset:
		host := params[0].(*api.Host)
		return n.getNamePartCHIScopeCycleOffset(host)

	case NamePartClusterScopeCycleSize:
		host := params[0].(*api.Host)
		return n.getNamePartClusterScopeCycleSize(host)
	case NamePartClusterScopeCycleIndex:
		host := params[0].(*api.Host)
		return n.getNamePartClusterScopeCycleIndex(host)
	case NamePartClusterScopeCycleOffset:
		host := params[0].(*api.Host)
		return n.getNamePartClusterScopeCycleOffset(host)

	case NamePartCHIScopeIndex:
		host := params[0].(*api.Host)
		return n.getNamePartCHIScopeIndex(host)
	case NamePartClusterScopeIndex:
		host := params[0].(*api.Host)
		return n.getNamePartClusterScopeIndex(host)
	case NamePartShardScopeIndex:
		host := params[0].(*api.Host)
		return n.getNamePartShardScopeIndex(host)
	case NamePartReplicaScopeIndex:
		host := params[0].(*api.Host)
		return n.getNamePartReplicaScopeIndex(host)
	}
	return "unknown part"
}

// getNamePartNamespace
func (n *namer) getNamePartNamespace(obj interface{}) string {
	switch obj.(type) {
	case *api.ClickHouseInstallation:
		chi := obj.(*api.ClickHouseInstallation)
		return n.namePartChiName(chi.Namespace)
	case *api.Cluster:
		cluster := obj.(*api.Cluster)
		return n.namePartChiName(cluster.Runtime.Address.Namespace)
	case *api.ChiShard:
		shard := obj.(*api.ChiShard)
		return n.namePartChiName(shard.Runtime.Address.Namespace)
	case *api.Host:
		host := obj.(*api.Host)
		return n.namePartChiName(host.Runtime.Address.Namespace)
	}

	return "ERROR"
}

// getNamePartCHIName
func (n *namer) getNamePartCHIName(obj interface{}) string {
	switch obj.(type) {
	case *api.ClickHouseInstallation:
		chi := obj.(*api.ClickHouseInstallation)
		return n.namePartChiName(chi.Name)
	case *api.Cluster:
		cluster := obj.(*api.Cluster)
		return n.namePartChiName(cluster.Runtime.Address.CHIName)
	case *api.ChiShard:
		shard := obj.(*api.ChiShard)
		return n.namePartChiName(shard.Runtime.Address.CHIName)
	case *api.Host:
		host := obj.(*api.Host)
		return n.namePartChiName(host.Runtime.Address.CHIName)
	}

	return "ERROR"
}

// getNamePartClusterName
func (n *namer) getNamePartClusterName(obj interface{}) string {
	switch obj.(type) {
	case *api.Cluster:
		cluster := obj.(*api.Cluster)
		return n.namePartClusterName(cluster.Runtime.Address.ClusterName)
	case *api.ChiShard:
		shard := obj.(*api.ChiShard)
		return n.namePartClusterName(shard.Runtime.Address.ClusterName)
	case *api.Host:
		host := obj.(*api.Host)
		return n.namePartClusterName(host.Runtime.Address.ClusterName)
	}

	return "ERROR"
}

// getNamePartShardName
func (n *namer) getNamePartShardName(obj interface{}) string {
	switch obj.(type) {
	case *api.ChiShard:
		shard := obj.(*api.ChiShard)
		return n.namePartShardName(shard.Runtime.Address.ShardName)
	case *api.Host:
		host := obj.(*api.Host)
		return n.namePartShardName(host.Runtime.Address.ShardName)
	}

	return "ERROR"
}

// getNamePartReplicaName
func (n *namer) getNamePartReplicaName(host *api.Host) string {
	return n.namePartReplicaName(host.Runtime.Address.ReplicaName)
}

// getNamePartHostName
func (n *namer) getNamePartHostName(host *api.Host) string {
	return n.namePartHostName(host.Runtime.Address.HostName)
}

// getNamePartCHIScopeCycleSize
func (n *namer) getNamePartCHIScopeCycleSize(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.CHIScopeCycleSize)
}

// getNamePartCHIScopeCycleIndex
func (n *namer) getNamePartCHIScopeCycleIndex(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.CHIScopeCycleIndex)
}

// getNamePartCHIScopeCycleOffset
func (n *namer) getNamePartCHIScopeCycleOffset(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.CHIScopeCycleOffset)
}

// getNamePartClusterScopeCycleSize
func (n *namer) getNamePartClusterScopeCycleSize(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.ClusterScopeCycleSize)
}

// getNamePartClusterScopeCycleIndex
func (n *namer) getNamePartClusterScopeCycleIndex(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.ClusterScopeCycleIndex)
}

// getNamePartClusterScopeCycleOffset
func (n *namer) getNamePartClusterScopeCycleOffset(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.ClusterScopeCycleOffset)
}

// getNamePartCHIScopeIndex
func (n *namer) getNamePartCHIScopeIndex(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.CHIScopeIndex)
}

// getNamePartClusterScopeIndex
func (n *namer) getNamePartClusterScopeIndex(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.ClusterScopeIndex)
}

// getNamePartShardScopeIndex
func (n *namer) getNamePartShardScopeIndex(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.ShardScopeIndex)
}

// getNamePartReplicaScopeIndex
func (n *namer) getNamePartReplicaScopeIndex(host *api.Host) string {
	return strconv.Itoa(host.Runtime.Address.ReplicaScopeIndex)
}
