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

// Labeler is an entity which can label CHI artifacts
type LabelerClickHouse struct {
	*Labeler
}

// NewLabelerClickHouse creates new labeler with context
func NewLabelerClickHouse(cr api.ICustomResource, config Config) *LabelerClickHouse {
	return &LabelerClickHouse{
		Labeler: NewLabeler(cr, config),
	}
}

func (l *LabelerClickHouse) Label(what LabelType, params ...any) map[string]string {
	switch what {
	case LabelConfigMapCommon:
		return l.labelConfigMapCHICommon()
	case LabelConfigMapCommonUsers:
		return l.labelConfigMapCHICommonUsers()
	case LabelConfigMapHost:
		return l.labelConfigMapHost(params...)

	default:
		return l.Labeler.Label(what, params...)
	}
	panic("unknown label type")
}

func (l *LabelerClickHouse) Selector(what SelectorType, params ...any) map[string]string {
	return l.Labeler.Selector(what, params...)
}
