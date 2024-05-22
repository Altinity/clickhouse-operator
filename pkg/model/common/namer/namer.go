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
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/common/interfaces"
)

type Namer struct {
}

// New creates new Namer with specified context
func New() *Namer {
	return &Namer{}
}

func (n *Namer) Names(what interfaces.NameType, params ...any) []string {
	switch what {
	case interfaces.NameFQDNs:
		obj := params[0]
		scope := params[1]
		excludeSelf := params[2].(bool)
		return createFQDNs(obj, scope, excludeSelf)
	}
	panic("unknown names type")
}

func (n *Namer) Name(what interfaces.NameType, params ...any) string {
	switch what {
	case interfaces.NameCRService:
		cr := params[0].(api.ICustomResource)
		return createCRServiceName(cr)
	case interfaces.NameCRServiceFQDN:
		cr := params[0].(api.ICustomResource)
		namespaceDomainPattern := params[1].(*api.String)
		return createCRServiceFQDN(cr, namespaceDomainPattern)
	case interfaces.NameClusterService:
		cluster := params[0].(api.ICluster)
		return createClusterServiceName(cluster)
	case interfaces.NameShardService:
		shard := params[0].(api.IShard)
		return createShardServiceName(shard)
	case interfaces.NameShard:
		shard := params[0].(api.IShard)
		index := params[1].(int)
		return createShardName(shard, index)
	case interfaces.NameReplica:
		replica := params[0].(api.IReplica)
		index := params[1].(int)
		return createReplicaName(replica, index)
	case interfaces.NameHost:
		host := params[0].(*api.Host)
		shard := params[1].(api.IShard)
		shardIndex := params[2].(int)
		replica := params[3].(api.IReplica)
		replicaIndex := params[4].(int)
		return createHostName(host, shard, shardIndex, replica, replicaIndex)
	case interfaces.NameHostTemplate:
		host := params[0].(*api.Host)
		return createHostTemplateName(host)
	case interfaces.NameInstanceHostname:
		host := params[0].(*api.Host)
		return createInstanceHostname(host)
	case interfaces.NameStatefulSet:
		host := params[0].(*api.Host)
		return createStatefulSetName(host)
	case interfaces.NameStatefulSetService:
		host := params[0].(*api.Host)
		return createStatefulSetServiceName(host)
	case interfaces.NamePodHostname:
		host := params[0].(*api.Host)
		return createPodHostname(host)
	case interfaces.NameFQDN:
		host := params[0].(*api.Host)
		return createFQDN(host)
	case interfaces.NamePodHostnameRegexp:
		cr := params[0].(api.ICustomResource)
		template := params[1].(string)
		return createPodHostnameRegexp(cr, template)
	case interfaces.NamePod:
		return createPodName(params[0])
	case interfaces.NamePVCNameByVolumeClaimTemplate:
		host := params[0].(*api.Host)
		volumeClaimTemplate := params[1].(*api.VolumeClaimTemplate)
		return createPVCNameByVolumeClaimTemplate(host, volumeClaimTemplate)
	case interfaces.NameClusterAutoSecret:
		cluster := params[0].(api.ICluster)
		return createClusterAutoSecretName(cluster)
	}

	panic("unknown name type")
}
