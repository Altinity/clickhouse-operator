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

// FilesGenerator specifies keeper configuration generator object
type FilesGenerator struct {
	configGenerator *Generator
}

// NewFilesGenerator creates new configuration files generator object
func NewFilesGenerator(cr api.ICustomResource, namer interfaces.INameManager, opts *GeneratorOptions) *FilesGenerator {
	return &FilesGenerator{
		configGenerator: newGenerator(cr, namer, opts),
	}
}

func (c *FilesGenerator) CreateConfigFiles(what interfaces.FilesGroupType, params ...any) map[string]string {
	switch what {
	case interfaces.FilesGroupHost:
		var options *FilesGeneratorOptions
		if len(params) > 0 {
			options = params[0].(*FilesGeneratorOptions)
			return c.createConfigFilesGroupHost(options)
		}
	}
	return nil
}

// createConfigFilesGroupHost creates host config files
func (c *FilesGenerator) createConfigFilesGroupHost(options *FilesGeneratorOptions) map[string]string {
	// Prepare for this replica deployment chopConfig files map as filename->content
	configSections := make(map[string]string)
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configSettings), c.configGenerator.getHostConfig(options.GetHost(), options.GetSettings()))

	return configSections
}

// createConfigSectionFilename creates filename of a configuration file.
// filename depends on a section which it will contain
func createConfigSectionFilename(section string) string {
	return "chop-generated-" + section + ".xml"
	//return "keeper_config.xml"
}
