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
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// FilesGenerator specifies configuration generator object
type FilesGenerator struct {
	// ClickHouse config generator
	configGenerator *Generator
	// clickhouse-operator configuration
	chopConfig *api.OperatorConfig
	namer      interfaces.INameManager
}

// NewFilesGenerator creates new configuration files generator object
func NewFilesGenerator(cr api.ICustomResource, namer interfaces.INameManager, opts *GeneratorOptions) *FilesGenerator {
	return &FilesGenerator{
		configGenerator: newGenerator(cr, namer, opts),
		chopConfig:      chop.Config(),
		namer:           namer,
	}
}

func (c *FilesGenerator) CreateConfigFiles(what interfaces.FilesGroupType, params ...any) map[string]string {
	switch what {
	case interfaces.FilesGroupCommon:
		var options *FilesGeneratorOptions
		if len(params) > 0 {
			options = params[0].(*FilesGeneratorOptions)
			return c.createConfigFilesGroupCommon(options)
		}
	case interfaces.FilesGroupUsers:
		return c.createConfigFilesGroupUsers()
	case interfaces.FilesGroupHost:
		var host *api.Host
		if len(params) > 0 {
			host = params[0].(*api.Host)
			return c.createConfigFilesGroupHost(host)
		}
	}
	return nil
}

// createConfigFilesGroupCommon creates common config files
func (c *FilesGenerator) createConfigFilesGroupCommon(options *FilesGeneratorOptions) map[string]string {
	if options == nil {
		options = defaultFilesGeneratorOptions()
	}
	configSections := make(map[string]string)
	// commonConfigSections maps section name to section XML chopConfig of the following sections:
	// 1. remote servers
	// 2. common settings
	// 3. common files
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configRemoteServers), c.configGenerator.getRemoteServers(options.GetRemoteServersOptions()))
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configSettings), c.configGenerator.getSettingsGlobal())
	util.MergeStringMapsOverwrite(configSections, c.configGenerator.getSectionFromFiles(api.SectionCommon, true, nil))
	// Extra user-specified config files
	util.MergeStringMapsOverwrite(configSections, c.chopConfig.ClickHouse.Config.File.Runtime.CommonConfigFiles)

	return configSections
}

// createConfigFilesGroupUsers creates users config files
func (c *FilesGenerator) createConfigFilesGroupUsers() map[string]string {
	configSections := make(map[string]string)
	// commonUsersConfigSections maps section name to section XML chopConfig of the following sections:
	// 1. users
	// 2. quotas
	// 3. profiles
	// 4. user files
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configUsers), c.configGenerator.getUsers())
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configQuotas), c.configGenerator.getQuotas())
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configProfiles), c.configGenerator.getProfiles())
	util.MergeStringMapsOverwrite(configSections, c.configGenerator.getSectionFromFiles(api.SectionUsers, false, nil))
	// Extra user-specified config files
	util.MergeStringMapsOverwrite(configSections, c.chopConfig.ClickHouse.Config.File.Runtime.UsersConfigFiles)

	return configSections
}

// createConfigFilesGroupHost creates host config files
func (c *FilesGenerator) createConfigFilesGroupHost(host *api.Host) map[string]string {
	// Prepare for this replica deployment chopConfig files map as filename->content
	configSections := make(map[string]string)
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configMacros), c.configGenerator.getHostMacros(host))
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configHostnamePorts), c.configGenerator.getHostHostnameAndPorts(host))
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configZookeeper), c.configGenerator.getHostZookeeper(host))
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configSettings), c.configGenerator.getSettings(host))
	util.MergeStringMapsOverwrite(configSections, c.configGenerator.getSectionFromFiles(api.SectionHost, true, host))
	// Extra user-specified config files
	util.MergeStringMapsOverwrite(configSections, c.chopConfig.ClickHouse.Config.File.Runtime.HostConfigFiles)

	return configSections
}

// createConfigSectionFilename creates filename of a configuration file.
// filename depends on a section which it will contain
func createConfigSectionFilename(section string) string {
	return "chop-generated-" + section + ".xml"
}
