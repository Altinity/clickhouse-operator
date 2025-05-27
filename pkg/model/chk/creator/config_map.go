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
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/config"
	macrosList "github.com/altinity/clickhouse-operator/pkg/model/chk/macro"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/tags/labeler"
	"github.com/altinity/clickhouse-operator/pkg/model/common/macro"
)

type ConfigMapManager struct {
	cr                   api.ICustomResource
	or                   interfaces.IOwnerReferencesManager
	tagger               interfaces.ITagger
	configFilesGenerator interfaces.IConfigFilesGenerator
	macro                interfaces.IMacro
	namer                interfaces.INameManager
	labeler              interfaces.ILabeler
}

func NewConfigMapManager() *ConfigMapManager {
	return &ConfigMapManager{
		or:      NewOwnerReferencer(),
		macro:   macro.New(macrosList.Get()),
		namer:   namer.New(),
		labeler: nil,
	}
}

func (m *ConfigMapManager) CreateConfigMap(what interfaces.ConfigMapType, params ...any) *core.ConfigMap {
	switch what {
	case interfaces.ConfigMapCommon:
		var options *config.FilesGeneratorOptions
		if len(params) > 0 {
			options = params[0].(*config.FilesGeneratorOptions)
			return m.createConfigMapCommon(options)
		}
	case interfaces.ConfigMapCommonUsers:
		return m.createConfigMapCommonUsers()
	case interfaces.ConfigMapHost:
		var host *api.Host
		var options *config.FilesGeneratorOptions
		if len(params) > 0 {
			host = params[0].(*api.Host)
			options = config.NewFilesGeneratorOptions().SetHost(host)
			return m.createConfigMapHost(host, options)
		}
	}
	panic("unknown config map type")
}

func (m *ConfigMapManager) SetCR(cr api.ICustomResource) {
	m.cr = cr
	m.labeler = labeler.New(cr)
}
func (m *ConfigMapManager) SetTagger(tagger interfaces.ITagger) {
	m.tagger = tagger
}
func (m *ConfigMapManager) SetConfigFilesGenerator(configFilesGenerator interfaces.IConfigFilesGenerator) {
	m.configFilesGenerator = configFilesGenerator
}

// createConfigMapCommon creates new core.ConfigMap
func (m *ConfigMapManager) createConfigMapCommon(options *config.FilesGeneratorOptions) *core.ConfigMap {
	cm := &core.ConfigMap{
		TypeMeta: meta.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: meta.ObjectMeta{
			Name:            m.namer.Name(interfaces.NameConfigMapCommon, m.cr),
			Namespace:       m.cr.GetNamespace(),
			Labels:          m.macro.Scope(m.cr).Map(m.tagger.Label(interfaces.LabelConfigMapCommon)),
			Annotations:     m.macro.Scope(m.cr).Map(m.tagger.Annotate(interfaces.AnnotateConfigMapCommon)),
			OwnerReferences: m.or.CreateOwnerReferences(m.cr),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: m.configFilesGenerator.CreateConfigFiles(interfaces.FilesGroupCommon, options),
	}
	// And after the object is ready we can put version label
	m.labeler.MakeObjectVersion(cm.GetObjectMeta(), cm)
	return cm
}

// createConfigMapCommonUsers creates new core.ConfigMap
func (m *ConfigMapManager) createConfigMapCommonUsers() *core.ConfigMap {
	cm := &core.ConfigMap{
		TypeMeta: meta.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: meta.ObjectMeta{
			Name:            m.namer.Name(interfaces.NameConfigMapCommonUsers, m.cr),
			Namespace:       m.cr.GetNamespace(),
			Labels:          m.macro.Scope(m.cr).Map(m.tagger.Label(interfaces.LabelConfigMapCommonUsers)),
			Annotations:     m.macro.Scope(m.cr).Map(m.tagger.Annotate(interfaces.AnnotateConfigMapCommonUsers)),
			OwnerReferences: m.or.CreateOwnerReferences(m.cr),
		},
		// Data contains several sections which are to be several xml chopConfig files
		Data: m.configFilesGenerator.CreateConfigFiles(interfaces.FilesGroupUsers),
	}
	// And after the object is ready we can put version label
	m.labeler.MakeObjectVersion(cm.GetObjectMeta(), cm)
	return cm
}

// createConfigMapHost creates config map for a host
func (m *ConfigMapManager) createConfigMapHost(host *api.Host, options *config.FilesGeneratorOptions) *core.ConfigMap {
	cm := &core.ConfigMap{
		TypeMeta: meta.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: meta.ObjectMeta{
			Name:            m.namer.Name(interfaces.NameConfigMapHost, host),
			Namespace:       host.GetRuntime().GetAddress().GetNamespace(),
			Labels:          m.macro.Scope(host).Map(m.tagger.Label(interfaces.LabelConfigMapHost, host)),
			Annotations:     m.macro.Scope(host).Map(m.tagger.Annotate(interfaces.AnnotateConfigMapHost, host)),
			OwnerReferences: m.or.CreateOwnerReferences(m.cr),
		},
		Data: m.configFilesGenerator.CreateConfigFiles(interfaces.FilesGroupHost, options),
	}
	// And after the object is ready we can put version label
	m.labeler.MakeObjectVersion(cm.GetObjectMeta(), cm)
	return cm
}
