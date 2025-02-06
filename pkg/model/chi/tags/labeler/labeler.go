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
	"github.com/altinity/clickhouse-operator/pkg/model/common/tags/labeler"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Labeler is an entity which can label CHI artifacts
type Labeler struct {
	*labeler.Labeler
}

// New creates new labeler with context
func New(cr api.ICustomResource, config ...*labeler.Config) *Labeler {
	return &Labeler{
		Labeler: labeler.New(cr, list, config...),
	}
}

func (l *Labeler) Label(what interfaces.LabelType, params ...any) map[string]string {
	switch what {
	case interfaces.LabelConfigMapCommon:
		return l.labelConfigMapCRCommon()
	case interfaces.LabelConfigMapCommonUsers:
		return l.labelConfigMapCRCommonUsers()
	case interfaces.LabelConfigMapHost:
		return l.labelConfigMapHost(params...)
	case interfaces.LabelConfigMapStorage:
		return l.labelConfigMapCRStorage()

	default:
		return l.Labeler.Label(what, params...)
	}
	panic("unknown label type")
}

func (l *Labeler) Selector(what interfaces.SelectorType, params ...any) map[string]string {
	return l.Labeler.Selector(what, params...)
}

// labelConfigMapCRCommon
func (l *Labeler) labelConfigMapCRCommon() map[string]string {
	return util.MergeStringMapsOverwrite(
		l.GetCRScope(),
		map[string]string{
			l.Get(labeler.LabelConfigMap): l.Get(labeler.LabelConfigMapValueCRCommon),
		})
}

// labelConfigMapCRCommonUsers
func (l *Labeler) labelConfigMapCRCommonUsers() map[string]string {
	return util.MergeStringMapsOverwrite(
		l.GetCRScope(),
		map[string]string{
			l.Get(labeler.LabelConfigMap): l.Get(labeler.LabelConfigMapValueCRCommonUsers),
		})
}

func (l *Labeler) labelConfigMapHost(params ...any) map[string]string {
	var host *api.Host
	if len(params) > 0 {
		host = params[0].(*api.Host)
		return l._labelConfigMapHost(host)
	}
	panic("not enough params for labeler")
}

// labelConfigMapCRStorage
func (l *Labeler) labelConfigMapCRStorage() map[string]string {
	return util.MergeStringMapsOverwrite(
		l.GetCRScope(),
		map[string]string{
			l.Get(labeler.LabelConfigMap): l.Get(labeler.LabelConfigMapValueCRStorage),
		})
}

// _labelConfigMapHost
func (l *Labeler) _labelConfigMapHost(host *api.Host) map[string]string {
	return util.MergeStringMapsOverwrite(
		l.GetHostScope(host, false),
		map[string]string{
			l.Get(labeler.LabelConfigMap): l.Get(labeler.LabelConfigMapValueHost),
		})
}
