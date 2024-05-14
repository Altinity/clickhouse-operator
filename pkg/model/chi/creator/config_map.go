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
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chi"
)

type ConfigMapType string

const (
	ConfigMapCHICommon      ConfigMapType = "chi common"
	ConfigMapCHICommonUsers ConfigMapType = "chi common users"
	ConfigMapCHIHost        ConfigMapType = "chi host"
)

func (c *Creator) CreateConfigMap(what ConfigMapType, params ...any) *core.ConfigMap {
	switch what {
	case ConfigMapCHICommon:
		var options *config.ClickHouseConfigFilesGeneratorOptions
		if len(params) > 0 {
			options = params[0].(*config.ClickHouseConfigFilesGeneratorOptions)
		}
		return c.createConfigMapCHICommon(options)
	case ConfigMapCHICommonUsers:
		return c.createConfigMapCHICommonUsers()
	case ConfigMapCHIHost:
		var host *api.Host
		if len(params) > 0 {
			host = params[0].(*api.Host)
		}
		return c.createConfigMapCHIHost(host)
	default:
		return nil
	}
}

// createConfigMapCHICommon creates new core.ConfigMap
func (c *Creator) createConfigMapCHICommon(options *config.ClickHouseConfigFilesGeneratorOptions) *core.ConfigMap {
	cm := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{
			Name:            model.CreateConfigMapCommonName(c.chi),
			Namespace:       c.chi.GetNamespace(),
			Labels:          model.Macro(c.chi).Map(c.labels.GetConfigMapCHICommon()),
			Annotations:     model.Macro(c.chi).Map(c.annotations.Annotate(model.AnnotateConfigMapCommon)),
			OwnerReferences: createOwnerReferences(c.chi),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: c.configFilesGenerator.CreateConfigFilesGroupCommon(options),
	}
	// And after the object is ready we can put version label
	model.MakeObjectVersion(cm.GetObjectMeta(), cm)
	return cm
}

// createConfigMapCHICommonUsers creates new core.ConfigMap
func (c *Creator) createConfigMapCHICommonUsers() *core.ConfigMap {
	cm := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{
			Name:            model.CreateConfigMapCommonUsersName(c.chi),
			Namespace:       c.chi.GetNamespace(),
			Labels:          model.Macro(c.chi).Map(c.labels.GetConfigMapCHICommonUsers()),
			Annotations:     model.Macro(c.chi).Map(c.annotations.Annotate(model.AnnotateConfigMapCommon)),
			OwnerReferences: createOwnerReferences(c.chi),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: c.configFilesGenerator.CreateConfigFilesGroupUsers(),
	}
	// And after the object is ready we can put version label
	model.MakeObjectVersion(cm.GetObjectMeta(), cm)
	return cm
}

// createConfigMapCHIHost creates new core.ConfigMap
func (c *Creator) createConfigMapCHIHost(host *api.Host) *core.ConfigMap {
	cm := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{
			Name:            model.CreateConfigMapHostName(host),
			Namespace:       host.GetRuntime().GetAddress().GetNamespace(),
			Labels:          model.Macro(host).Map(c.labels.GetConfigMapHost(host)),
			Annotations:     model.Macro(host).Map(c.annotations.Annotate(model.AnnotateConfigMapHost, host)),
			OwnerReferences: createOwnerReferences(c.chi),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: c.configFilesGenerator.CreateConfigFilesGroupHost(host),
	}
	// And after the object is ready we can put version label
	model.MakeObjectVersion(cm.GetObjectMeta(), cm)
	return cm
}
