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
)

type ConfigMapManagerKeeper struct {
	cr                   api.ICustomResource
	tagger               interfaces.ITagger
	configFilesGenerator interfaces.IConfigFilesGenerator
}

func NewConfigMapManagerKeeper() *ConfigMapManagerKeeper {
	return &ConfigMapManagerKeeper{}
}

func (m *ConfigMapManagerKeeper) CreateConfigMap(what interfaces.ConfigMapType, params ...any) *core.ConfigMap {
	switch what {
	case interfaces.ConfigMapConfig:
		var options *config.FilesGeneratorOptionsKeeper
		if len(params) > 0 {
			options = params[0].(*config.FilesGeneratorOptionsKeeper)
			return m.common(options)
		}
	}
	panic("unknown config map type")
}

func (m *ConfigMapManagerKeeper) SetCR(cr api.ICustomResource) {
	m.cr = cr
}
func (m *ConfigMapManagerKeeper) SetTagger(tagger interfaces.ITagger) {
	m.tagger = tagger
}
func (m *ConfigMapManagerKeeper) SetConfigFilesGenerator(configFilesGenerator interfaces.IConfigFilesGenerator) {
	m.configFilesGenerator = configFilesGenerator
}

// CreateConfigMap returns a config map containing ClickHouse Keeper config XML
func (m *ConfigMapManagerKeeper) common(options *config.FilesGeneratorOptionsKeeper) *core.ConfigMap {
	return &core.ConfigMap{
		TypeMeta: meta.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: meta.ObjectMeta{
			Name:      m.cr.GetName(),
			Namespace: m.cr.GetNamespace(),
		},
		Data: m.configFilesGenerator.CreateConfigFiles(interfaces.FilesGroupCommon, options),
	}
}
