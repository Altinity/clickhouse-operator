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
	"github.com/altinity/clickhouse-operator/pkg/model/common/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/common/tags/labeler"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Labeler is an entity which can label CHI artifacts
type LabelerClickHouse struct {
	*labeler.Labeler
}

// NewLabelerClickHouse creates new labeler with context
func NewLabelerClickHouse(cr api.ICustomResource, config labeler.Config) *LabelerClickHouse {
	return &LabelerClickHouse{
		Labeler: labeler.NewLabeler(cr, config),
	}
}

func (l *LabelerClickHouse) Label(what interfaces.LabelType, params ...any) map[string]string {
	switch what {
	case interfaces.LabelConfigMapCommon:
		return l.labelConfigMapCHICommon()
	case interfaces.LabelConfigMapCommonUsers:
		return l.labelConfigMapCHICommonUsers()
	case interfaces.LabelConfigMapHost:
		return l.labelConfigMapHost(params...)

	default:
		return l.Labeler.Label(what, params...)
	}
	panic("unknown label type")
}

func (l *LabelerClickHouse) Selector(what interfaces.SelectorType, params ...any) map[string]string {
	return l.Labeler.Selector(what, params...)
}

// labelConfigMapCHICommon
func (l *LabelerClickHouse) labelConfigMapCHICommon() map[string]string {
	return util.MergeStringMapsOverwrite(
		l.GetCRScope(),
		map[string]string{
			labeler.LabelConfigMap: labeler.LabelConfigMapValueCHICommon,
		})
}

// labelConfigMapCHICommonUsers
func (l *LabelerClickHouse) labelConfigMapCHICommonUsers() map[string]string {
	return util.MergeStringMapsOverwrite(
		l.GetCRScope(),
		map[string]string{
			labeler.LabelConfigMap: labeler.LabelConfigMapValueCHICommonUsers,
		})
}

func (l *LabelerClickHouse) labelConfigMapHost(params ...any) map[string]string {
	var host *api.Host
	if len(params) > 0 {
		host = params[0].(*api.Host)
		return l._labelConfigMapHost(host)
	}
	panic("not enough params for labeler")
}

// _labelConfigMapHost
func (l *LabelerClickHouse) _labelConfigMapHost(host *api.Host) map[string]string {
	return util.MergeStringMapsOverwrite(
		l.GetHostScope(host, false),
		map[string]string{
			labeler.LabelConfigMap: labeler.LabelConfigMapValueHost,
		})
}
