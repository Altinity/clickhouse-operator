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

import api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"

type namer struct {
}

// New creates new namer with specified context
func New() *namer {
	return &namer{}
}

func (n *namer) Names(what NameType, params ...any) []string {
	switch what {
	case NameFQDNs:
		obj := params[0]
		scope := params[1]
		excludeSelf := params[2].(bool)
		return createFQDNs(obj, scope, excludeSelf)
	}
	panic("unknown names type")
}

func (n *namer) Name(what NameType, params ...any) string {
	switch what {
	case NameCRService:
		cr := params[0].(api.ICustomResource)
		return createCRServiceName(cr)
	case NameCRServiceFQDN:
		cr := params[0].(api.ICustomResource)
		namespaceDomainPattern := params[1].(*api.String)
		return createCRServiceFQDN(cr, namespaceDomainPattern)
	case NameClusterService:
		cluster := params[0].(api.ICluster)
		return createClusterServiceName(cluster)
	case NameShardService:
		shard := params[0].(api.IShard)
		return createShardServiceName(shard)
	case NameShard:
		shard := params[0].(api.IShard)
		index := params[1].(int)
		return createShardName(shard, index)
	case NameReplica:
		replica := params[0].(api.IReplica)
		index := params[1].(int)
		return createReplicaName(replica, index)
	case NameHost:
		host := params[0].(*api.Host)
		shard := params[1].(api.IShard)
		shardIndex := params[2].(int)
		replica := params[3].(api.IReplica)
		replicaIndex := params[4].(int)
		return createHostName(host, shard, shardIndex, replica, replicaIndex)
	case NameHostTemplate:
		host := params[0].(*api.Host)
		return createHostTemplateName(host)
	case NameInstanceHostname:
		host := params[0].(*api.Host)
		return createInstanceHostname(host)
	case NameStatefulSet:
		host := params[0].(*api.Host)
		return createStatefulSetName(host)
	case NameStatefulSetService:
		host := params[0].(*api.Host)
		return createStatefulSetServiceName(host)
	case NamePodHostname:
		host := params[0].(*api.Host)
		return createPodHostname(host)
	case NameFQDN:
		host := params[0].(*api.Host)
		return createFQDN(host)
	case NamePodHostnameRegexp:
		cr := params[0].(api.ICustomResource)
		template := params[1].(string)
		return createPodHostnameRegexp(cr, template)
	case NamePod:
		return createPodName(params[0])
	case NamePVCNameByVolumeClaimTemplate:
		host := params[0].(*api.Host)
		volumeClaimTemplate := params[1].(*api.VolumeClaimTemplate)
		return createPVCNameByVolumeClaimTemplate(host, volumeClaimTemplate)
	case NameClusterAutoSecret:
		cluster := params[0].(api.ICluster)
		return createClusterAutoSecretName(cluster)
	}

	panic("unknown name type")
}
