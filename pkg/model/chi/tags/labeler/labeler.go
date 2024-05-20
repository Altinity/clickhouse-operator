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

import api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"

type LabelType string

const (
	LabelConfigMapCommon      LabelType = "Label cm common"
	LabelConfigMapCommonUsers LabelType = "Label cm common users"
	LabelConfigMapHost        LabelType = "Label cm host"

	LabelServiceCR      LabelType = "Label svc chi"
	LabelServiceCluster LabelType = "Label svc cluster"
	LabelServiceShard   LabelType = "Label svc shard"
	LabelServiceHost    LabelType = "Label svc host"

	LabelExistingPV  LabelType = "Label existing pv"
	LabelNewPVC      LabelType = "Label new pvc"
	LabelExistingPVC LabelType = "Label existing pvc"

	LabelPDB LabelType = "Label pdb"

	LabelSTS LabelType = "Label STS"

	LabelPodTemplate LabelType = "Label PodTemplate"
)

type SelectorType string

const (
	SelectorCHIScope          SelectorType = "SelectorCHIScope"
	SelectorCHIScopeReady     SelectorType = "SelectorCHIScopeReady"
	SelectorClusterScope      SelectorType = "SelectorClusterScope"
	SelectorClusterScopeReady SelectorType = "SelectorClusterScopeReady"
	SelectorShardScopeReady   SelectorType = "SelectorShardScopeReady"
	SelectorHostScope         SelectorType = "getSelectorHostScope"
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

func (l *Labeler) Label(what LabelType, params ...any) map[string]string {
	switch what {
	case LabelServiceCR:
		return l.labelServiceCR()
	case LabelServiceCluster:
		return l.labelServiceCluster(params...)
	case LabelServiceShard:
		return l.labelServiceShard(params...)
	case LabelServiceHost:
		return l.labelServiceHost(params...)

	case LabelExistingPV:
		return l.labelExistingPV(params...)

	case LabelNewPVC:
		return l.labelNewPVC(params...)
	case LabelExistingPVC:
		return l.labelExistingPVC(params...)

	case LabelPDB:
		return l.labelPDB(params...)

	case LabelSTS:
		return l.labelSTS(params...)

	case LabelPodTemplate:
		return l.labelPodTemplate(params...)
	}
	panic("unknown label type")
}

func (l *Labeler) Selector(what SelectorType, params ...any) map[string]string {
	switch what {
	case SelectorCHIScope:
		return l.getSelectorCRScope()
	case SelectorCHIScopeReady:
		return l.getSelectorCRScopeReady()
	case SelectorClusterScope:
		var cluster api.ICluster
		if len(params) > 0 {
			cluster = params[0].(api.ICluster)
			return l.getSelectorClusterScope(cluster)
		}
	case SelectorClusterScopeReady:
		var cluster api.ICluster
		if len(params) > 0 {
			cluster = params[0].(api.ICluster)
			return l.getSelectorClusterScopeReady(cluster)
		}
	case SelectorShardScopeReady:
		var shard api.IShard
		if len(params) > 0 {
			shard = params[0].(api.IShard)
			return l.getSelectorShardScopeReady(shard)
		}
	case SelectorHostScope:
		var host *api.Host
		if len(params) > 0 {
			host = params[0].(*api.Host)
			return l.getSelectorHostScope(host)
		}
	}
	panic("unknown selector type")
}
