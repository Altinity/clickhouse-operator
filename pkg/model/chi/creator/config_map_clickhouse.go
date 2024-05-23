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
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/common/creator"
	"github.com/altinity/clickhouse-operator/pkg/model/common/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/common/namer/macro"
	commonLabeler "github.com/altinity/clickhouse-operator/pkg/model/common/tags/labeler"
)

type ConfigMapManagerClickHouse struct {
	cr                   api.ICustomResource
	tagger               interfaces.ITagger
	configFilesGenerator interfaces.IConfigFilesGenerator
}

func NewConfigMapManagerClickHouse() *ConfigMapManagerClickHouse {
	return &ConfigMapManagerClickHouse{}
}

func (m *ConfigMapManagerClickHouse) CreateConfigMap(what interfaces.ConfigMapType, params ...any) *core.ConfigMap {
	switch what {
	case interfaces.ConfigMapCHICommon:
		var options *config.FilesGeneratorOptionsClickHouse
		if len(params) > 0 {
			options = params[0].(*config.FilesGeneratorOptionsClickHouse)
			return m.createConfigMapCHICommon(options)
		}
	case interfaces.ConfigMapCHICommonUsers:
		return m.createConfigMapCHICommonUsers()
	case interfaces.ConfigMapCHIHost:
		var host *api.Host
		if len(params) > 0 {
			host = params[0].(*api.Host)
			return m.createConfigMapCHIHost(host)
		}
	}
	panic("unknown config map type")
}

func (m *ConfigMapManagerClickHouse) SetCR(cr api.ICustomResource) {
	m.cr = cr
}
func (m *ConfigMapManagerClickHouse) SetTagger(tagger interfaces.ITagger) {
	m.tagger = tagger
}
func (m *ConfigMapManagerClickHouse) SetConfigFilesGenerator(configFilesGenerator interfaces.IConfigFilesGenerator) {
	m.configFilesGenerator = configFilesGenerator
}

// createConfigMapCHICommon creates new core.ConfigMap
func (m *ConfigMapManagerClickHouse) createConfigMapCHICommon(options *config.FilesGeneratorOptionsClickHouse) *core.ConfigMap {
	cm := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{
			Name:            namer.NewClickHouse().Name(interfaces.NameConfigMapCommon, m.cr),
			Namespace:       m.cr.GetNamespace(),
			Labels:          macro.Macro(m.cr).Map(m.tagger.Label(interfaces.LabelConfigMapCommon)),
			Annotations:     macro.Macro(m.cr).Map(m.tagger.Annotate(interfaces.AnnotateConfigMapCommon)),
			OwnerReferences: creator.CreateOwnerReferences(m.cr),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: m.configFilesGenerator.CreateConfigFiles(interfaces.FilesGroupCommon, options),
	}
	// And after the object is ready we can put version label
	commonLabeler.MakeObjectVersion(cm.GetObjectMeta(), cm)
	return cm
}

// createConfigMapCHICommonUsers creates new core.ConfigMap
func (m *ConfigMapManagerClickHouse) createConfigMapCHICommonUsers() *core.ConfigMap {
	cm := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{
			Name:            namer.NewClickHouse().Name(interfaces.NameConfigMapCommonUsers, m.cr),
			Namespace:       m.cr.GetNamespace(),
			Labels:          macro.Macro(m.cr).Map(m.tagger.Label(interfaces.LabelConfigMapCommonUsers)),
			Annotations:     macro.Macro(m.cr).Map(m.tagger.Annotate(interfaces.AnnotateConfigMapCommonUsers)),
			OwnerReferences: creator.CreateOwnerReferences(m.cr),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: m.configFilesGenerator.CreateConfigFiles(interfaces.FilesGroupUsers),
	}
	// And after the object is ready we can put version label
	commonLabeler.MakeObjectVersion(cm.GetObjectMeta(), cm)
	return cm
}

// createConfigMapCHIHost creates new core.ConfigMap
func (m *ConfigMapManagerClickHouse) createConfigMapCHIHost(host *api.Host) *core.ConfigMap {
	cm := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{
			Name:            namer.NewClickHouse().Name(interfaces.NameConfigMapHost, host),
			Namespace:       host.GetRuntime().GetAddress().GetNamespace(),
			Labels:          macro.Macro(host).Map(m.tagger.Label(interfaces.LabelConfigMapHost, host)),
			Annotations:     macro.Macro(host).Map(m.tagger.Annotate(interfaces.AnnotateConfigMapHost, host)),
			OwnerReferences: creator.CreateOwnerReferences(m.cr),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: m.configFilesGenerator.CreateConfigFiles(interfaces.FilesGroupHost, host),
	}
	// And after the object is ready we can put version label
	commonLabeler.MakeObjectVersion(cm.GetObjectMeta(), cm)
	return cm
}
