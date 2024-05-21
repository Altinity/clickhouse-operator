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
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags/annotator"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	commonConfig "github.com/altinity/clickhouse-operator/pkg/model/common/config"
	"github.com/altinity/clickhouse-operator/pkg/model/common/namer/macro"
)

type ConfigMapManagerClickHouse struct {
	cr                   api.ICustomResource
	tagger               iTagger
	configFilesGenerator *config.FilesGeneratorClickHouse
}

func NewConfigMapManagerClickHouse() *ConfigMapManagerClickHouse {
	return &ConfigMapManagerClickHouse{}
}

func (m *ConfigMapManagerClickHouse) CreateConfigMap(what ConfigMapType, params ...any) *core.ConfigMap {
	switch what {
	case ConfigMapCHICommon:
		var options *config.FilesGeneratorOptionsClickHouse
		if len(params) > 0 {
			options = params[0].(*config.FilesGeneratorOptionsClickHouse)
			return m.createConfigMapCHICommon(options)
		}
	case ConfigMapCHICommonUsers:
		return m.createConfigMapCHICommonUsers()
	case ConfigMapCHIHost:
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
func (m *ConfigMapManagerClickHouse) SetTagger(tagger iTagger) {
	m.tagger = tagger
}
func (m *ConfigMapManagerClickHouse) SetConfigFilesGenerator(configFilesGenerator any) {
	m.configFilesGenerator = configFilesGenerator.(*config.FilesGeneratorClickHouse)
}

// createConfigMapCHICommon creates new core.ConfigMap
func (m *ConfigMapManagerClickHouse) createConfigMapCHICommon(options *config.FilesGeneratorOptionsClickHouse) *core.ConfigMap {
	cm := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{
			Name:            namer.NewClickHouse().Name(namer.NameConfigMapCommon, m.cr),
			Namespace:       m.cr.GetNamespace(),
			Labels:          macro.Macro(m.cr).Map(m.tagger.Label(labeler.LabelConfigMapCommon)),
			Annotations:     macro.Macro(m.cr).Map(m.tagger.Annotate(annotator.AnnotateConfigMapCommon)),
			OwnerReferences: createOwnerReferences(m.cr),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: m.configFilesGenerator.CreateConfigFiles(commonConfig.FilesGroupCommon, options),
	}
	// And after the object is ready we can put version label
	labeler.MakeObjectVersion(cm.GetObjectMeta(), cm)
	return cm
}

// createConfigMapCHICommonUsers creates new core.ConfigMap
func (m *ConfigMapManagerClickHouse) createConfigMapCHICommonUsers() *core.ConfigMap {
	cm := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{
			Name:            namer.NewClickHouse().Name(namer.NameConfigMapCommonUsers, m.cr),
			Namespace:       m.cr.GetNamespace(),
			Labels:          macro.Macro(m.cr).Map(m.tagger.Label(labeler.LabelConfigMapCommonUsers)),
			Annotations:     macro.Macro(m.cr).Map(m.tagger.Annotate(annotator.AnnotateConfigMapCommonUsers)),
			OwnerReferences: createOwnerReferences(m.cr),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: m.configFilesGenerator.CreateConfigFiles(commonConfig.FilesGroupUsers),
	}
	// And after the object is ready we can put version label
	labeler.MakeObjectVersion(cm.GetObjectMeta(), cm)
	return cm
}

// createConfigMapCHIHost creates new core.ConfigMap
func (m *ConfigMapManagerClickHouse) createConfigMapCHIHost(host *api.Host) *core.ConfigMap {
	cm := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{
			Name:            namer.NewClickHouse().Name(namer.NameConfigMapHost, host),
			Namespace:       host.GetRuntime().GetAddress().GetNamespace(),
			Labels:          macro.Macro(host).Map(m.tagger.Label(labeler.LabelConfigMapHost, host)),
			Annotations:     macro.Macro(host).Map(m.tagger.Annotate(annotator.AnnotateConfigMapHost, host)),
			OwnerReferences: createOwnerReferences(m.cr),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: m.configFilesGenerator.CreateConfigFiles(commonConfig.FilesGroupHost, host),
	}
	// And after the object is ready we can put version label
	labeler.MakeObjectVersion(cm.GetObjectMeta(), cm)
	return cm
}
