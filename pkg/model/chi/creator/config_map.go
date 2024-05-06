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

package creator

import (
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chi"
)

// CreateConfigMapCHICommon creates new core.ConfigMap
func (c *Creator) CreateConfigMapCHICommon(options *model.ClickHouseConfigFilesGeneratorOptions) *core.ConfigMap {
	cm := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{
			Name:            model.CreateConfigMapCommonName(c.chi),
			Namespace:       c.chi.GetNamespace(),
			Labels:          model.Macro(c.chi).Map(c.labels.GetConfigMapCHICommon()),
			Annotations:     model.Macro(c.chi).Map(c.annotations.GetConfigMapCHICommon()),
			OwnerReferences: getOwnerReferences(c.chi),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: c.chConfigFilesGenerator.CreateConfigFilesGroupCommon(options),
	}
	// And after the object is ready we can put version label
	model.MakeObjectVersion(&cm.ObjectMeta, cm)
	return cm
}

// CreateConfigMapCHICommonUsers creates new core.ConfigMap
func (c *Creator) CreateConfigMapCHICommonUsers() *core.ConfigMap {
	cm := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{
			Name:            model.CreateConfigMapCommonUsersName(c.chi),
			Namespace:       c.chi.GetNamespace(),
			Labels:          model.Macro(c.chi).Map(c.labels.GetConfigMapCHICommonUsers()),
			Annotations:     model.Macro(c.chi).Map(c.annotations.GetConfigMapCHICommonUsers()),
			OwnerReferences: getOwnerReferences(c.chi),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: c.chConfigFilesGenerator.CreateConfigFilesGroupUsers(),
	}
	// And after the object is ready we can put version label
	model.MakeObjectVersion(&cm.ObjectMeta, cm)
	return cm
}

// CreateConfigMapHost creates new core.ConfigMap
func (c *Creator) CreateConfigMapHost(host *api.ChiHost) *core.ConfigMap {
	cm := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{
			Name:            model.CreateConfigMapHostName(host),
			Namespace:       host.Runtime.Address.Namespace,
			Labels:          model.Macro(host).Map(c.labels.GetConfigMapHost(host)),
			Annotations:     model.Macro(host).Map(c.annotations.GetConfigMapHost(host)),
			OwnerReferences: getOwnerReferences(c.chi),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: c.chConfigFilesGenerator.CreateConfigFilesGroupHost(host),
	}
	// And after the object is ready we can put version label
	model.MakeObjectVersion(&cm.ObjectMeta, cm)
	return cm
}
