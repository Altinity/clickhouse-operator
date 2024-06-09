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

package annotator

import (
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/common/tags/annotator"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// Annotator is an entity which can annotate CHI artifacts
type Keeper struct {
	*annotator.Annotator
	cr apiChi.ICustomResource
}

// NewAnnotatorKeeper creates new annotator with context
func NewAnnotatorKeeper(cr apiChi.ICustomResource, config annotator.Config) *Keeper {
	return &Keeper{
		Annotator: annotator.NewAnnotator(cr, config),
		cr:        cr,
	}
}

func (a *Keeper) Annotate(what interfaces.AnnotateType, params ...any) map[string]string {
	switch what {
	case interfaces.AnnotateConfigMapCommon:
		return a.GetCRScope()
	case interfaces.AnnotateConfigMapCommonUsers:
		return a.GetCRScope()
	case interfaces.AnnotateConfigMapHost:
		var host *apiChi.Host
		if len(params) > 0 {
			host = params[0].(*apiChi.Host)
			return a.GetHostScope(host)
		}
	default:
		return a.Annotator.Annotate(what, params...)
	}
	panic("unknown annotate type")
}

func getPodAnnotations(chk *apiChk.ClickHouseKeeperInstallation) map[string]string {
	return map[string]string{}

	//// Fetch annotations from Pod template (if any)
	//annotations := chk2.getPodTemplateAnnotations(chk)
	//
	//// In case no Prometheus port specified - nothing to add to annotations
	//port := chk.Spec.GetPrometheusPort()
	//if port == -1 {
	//	return annotations
	//}
	//
	//// Prometheus port specified, append it to annotations
	//if annotations == nil {
	//	annotations = map[string]string{}
	//}
	//annotations["prometheus.io/port"] = fmt.Sprintf("%d", port)
	//annotations["prometheus.io/scrape"] = "true"
	//return annotations
}
