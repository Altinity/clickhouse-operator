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
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/common/tags/annotator"
)

// Annotator is an entity which can annotate CHI artifacts
type Annotator struct {
	*annotator.Annotator
	cr api.ICustomResource
}

// New creates new annotator with context
func New(cr api.ICustomResource, config ...*annotator.Config) *Annotator {
	return &Annotator{
		Annotator: annotator.New(cr, config...),
		cr:        cr,
	}
}

func (a *Annotator) Annotate(what interfaces.AnnotateType, params ...any) map[string]string {
	switch what {
	case interfaces.AnnotateConfigMapCommon:
		return a.GetCRScope()
	case interfaces.AnnotateConfigMapStorage:
		return a.GetCRScope()
	case interfaces.AnnotateConfigMapCommonUsers:
		return a.GetCRScope()
	case interfaces.AnnotateConfigMapHost:
		var host *api.Host
		if len(params) > 0 {
			host = params[0].(*api.Host)
			return a.GetHostScope(host)
		}
	default:
		return a.Annotator.Annotate(what, params...)
	}
	panic("unknown annotate type")
}
