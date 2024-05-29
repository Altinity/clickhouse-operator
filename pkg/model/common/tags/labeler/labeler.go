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

package labeler

import (
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

// Labeler is an entity which can label CHI artifacts
type Labeler struct {
	Config
	cr api.ICustomResource
}

type Config struct {
	AppendScope bool
	Include     []string
	Exclude     []string
}

// NewLabeler creates new labeler with context
func NewLabeler(cr api.ICustomResource, config Config) *Labeler {
	return &Labeler{
		Config: config,
		cr:     cr,
	}
}

func (l *Labeler) Label(what interfaces.LabelType, params ...any) map[string]string {
	switch what {
	case interfaces.LabelServiceCR:
		return l.labelServiceCR()
	case interfaces.LabelServiceCluster:
		return l.labelServiceCluster(params...)
	case interfaces.LabelServiceShard:
		return l.labelServiceShard(params...)
	case interfaces.LabelServiceHost:
		return l.labelServiceHost(params...)

	case interfaces.LabelExistingPV:
		return l.labelExistingPV(params...)

	case interfaces.LabelNewPVC:
		return l.labelNewPVC(params...)
	case interfaces.LabelExistingPVC:
		return l.labelExistingPVC(params...)

	case interfaces.LabelPDB:
		return l.labelPDB(params...)

	case interfaces.LabelSTS:
		return l.labelSTS(params...)

	case interfaces.LabelPodTemplate:
		return l.labelPodTemplate(params...)
	}
	panic("unknown label type")
}

func (l *Labeler) Selector(what interfaces.SelectorType, params ...any) map[string]string {
	switch what {
	case interfaces.SelectorCRScope:
		return l.getSelectorCRScope()
	case interfaces.SelectorCRScopeReady:
		return l.getSelectorCRScopeReady()
	case interfaces.SelectorClusterScope:
		var cluster api.ICluster
		if len(params) > 0 {
			cluster = params[0].(api.ICluster)
			return l.getSelectorClusterScope(cluster)
		}
	case interfaces.SelectorClusterScopeReady:
		var cluster api.ICluster
		if len(params) > 0 {
			cluster = params[0].(api.ICluster)
			return l.getSelectorClusterScopeReady(cluster)
		}
	case interfaces.SelectorShardScopeReady:
		var shard api.IShard
		if len(params) > 0 {
			shard = params[0].(api.IShard)
			return l.getSelectorShardScopeReady(shard)
		}
	case interfaces.SelectorHostScope:
		var host *api.Host
		if len(params) > 0 {
			host = params[0].(*api.Host)
			return l.getSelectorHostScope(host)
		}
	}
	panic("unknown selector type")
}
