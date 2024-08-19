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
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/macro"
	commonMacro "github.com/altinity/clickhouse-operator/pkg/model/common/macro"
	commonNamer "github.com/altinity/clickhouse-operator/pkg/model/common/namer"
)

type Namer struct {
	commonNamer *commonNamer.Namer
	macro       interfaces.IMacro
}

// New creates new namer with specified context
func New() *Namer {
	me := commonMacro.New(macro.List)
	return &Namer{
		commonNamer: commonNamer.New(me),
		macro:       me,
	}
}

func (n *Namer) Name(what interfaces.NameType, params ...any) string {
	switch what {
	case interfaces.NameConfigMapHost:
		host := params[0].(*api.Host)
		return n.createConfigMapNameHost(host)
	case interfaces.NameConfigMapCommon:
		cr := params[0].(api.ICustomResource)
		return n.createConfigMapNameCommon(cr)
	case interfaces.NameConfigMapCommonUsers:
		cr := params[0].(api.ICustomResource)
		return n.createConfigMapNameCommonUsers(cr)

	case interfaces.NameCRService:
		cr := params[0].(api.ICustomResource)
		return n.createCRServiceName(cr)
	case interfaces.NameCRServiceFQDN:
		cr := params[0].(api.ICustomResource)
		namespaceDomainPattern := params[1].(*types.String)
		return n.createCRServiceFQDN(cr, namespaceDomainPattern)
	case interfaces.NameClusterService:
		cluster := params[0].(api.ICluster)
		return n.createClusterServiceName(cluster)
	case interfaces.NameShardService:
		shard := params[0].(api.IShard)
		return n.createShardServiceName(shard)
	case interfaces.NameInstanceHostname:
		host := params[0].(*api.Host)
		return n.createInstanceHostname(host)
	case interfaces.NameStatefulSet:
		host := params[0].(*api.Host)
		return n.createStatefulSetName(host)
	case interfaces.NameStatefulSetService:
		host := params[0].(*api.Host)
		return n.createStatefulSetServiceName(host)
	case interfaces.NamePodHostname:
		host := params[0].(*api.Host)
		return n.createPodHostname(host)
	case interfaces.NameFQDN:
		host := params[0].(*api.Host)
		return n.createFQDN(host)
	case interfaces.NamePod:
		return n.createPodName(params[0])
	case interfaces.NamePVCNameByVolumeClaimTemplate:
		host := params[0].(*api.Host)
		volumeClaimTemplate := params[1].(*api.VolumeClaimTemplate)
		return n.createPVCNameByVolumeClaimTemplate(host, volumeClaimTemplate)

	default:
		return n.commonNamer.Name(what, params...)
	}

	panic("unknown name type")
}

func (n *Namer) Names(what interfaces.NameType, params ...any) []string {
	switch what {
	case interfaces.NameFQDNs:
		obj := params[0]
		scope := params[1]
		excludeSelf := params[2].(bool)
		return n.createFQDNs(obj, scope, excludeSelf)
	default:
		return n.commonNamer.Names(what, params...)
	}
	panic("unknown names type")
}
