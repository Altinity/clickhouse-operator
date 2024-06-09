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

package config

import (
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// FilesGeneratorKeeper specifies keeper configuration generator object
type FilesGeneratorKeeper struct {
	configGenerator *GeneratorKeeper
}

// NewConfigFilesGeneratorKeeper creates new keeper configuration generator object
func NewConfigFilesGeneratorKeeper(cr api.ICustomResource, opts *GeneratorOptions) *FilesGeneratorKeeper {
	return &FilesGeneratorKeeper{
		configGenerator: newConfigGeneratorKeeper(cr, opts),
	}
}

func (c *FilesGeneratorKeeper) CreateConfigFiles(what interfaces.FilesGroupType, params ...any) map[string]string {
	switch what {
	case interfaces.FilesGroupCommon:
		return c.createConfigFilesGroupCommon(nil)
	}
	return nil
}

// createConfigFilesGroupCommon creates common config files
func (c *FilesGeneratorKeeper) createConfigFilesGroupCommon(settings *api.Settings) map[string]string {
	configSections := make(map[string]string)
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configMain), c.configGenerator.getConfig(settings))

	return configSections
}

// createConfigSectionFilename creates filename of a configuration file.
// filename depends on a section which it will contain
func createConfigSectionFilename(section string) string {
	return ConfigFilename
}
