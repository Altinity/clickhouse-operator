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
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

type Namer struct {
	macro interfaces.IMacro
}

// New creates new Namer with specified context
func New(macro interfaces.IMacro) *Namer {
	return &Namer{
		macro: macro,
	}
}

func (n *Namer) Names(what interfaces.NameType, params ...any) []string {
	return nil
}

func (n *Namer) Name(what interfaces.NameType, params ...any) string {
	switch what {
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
	case interfaces.NamePodHostnameRegexp:
		cr := params[0].(api.ICustomResource)
		template := params[1].(string)
		return n.createPodHostnameRegexp(cr, template)
	case interfaces.NameClusterAutoSecret:
		cluster := params[0].(api.ICluster)
		return createClusterAutoSecretName(cluster)
	}

	panic("unknown name type")
}
