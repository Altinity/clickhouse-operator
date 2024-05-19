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
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// FilesGeneratorClickHouse specifies clickhouse configuration generator object
type FilesGeneratorClickHouse struct {
	// ClickHouse config generator
	configGenerator *GeneratorClickHouse
	// clickhouse-operator configuration
	chopConfig *api.OperatorConfig
}

// NewConfigFilesGeneratorClickHouse creates new clickhouse configuration generator object
func NewConfigFilesGeneratorClickHouse(cr api.ICustomResource, opts *GeneratorOptions) *FilesGeneratorClickHouse {
	return &FilesGeneratorClickHouse{
		configGenerator: newConfigGeneratorClickHouse(cr, opts),
		chopConfig:      chop.Config(),
	}
}

func (c *FilesGeneratorClickHouse) CreateConfigFiles(what FilesGroupType, params ...any) map[string]string {
	switch what {
	case FilesGroupCommon:
		var options *FilesGeneratorOptionsClickHouse
		if len(params) > 0 {
			options = params[0].(*FilesGeneratorOptionsClickHouse)
			return c.createConfigFilesGroupCommon(options)
		}
	case FilesGroupUsers:
		return c.createConfigFilesGroupUsers()
	case FilesGroupHost:
		var host *api.Host
		if len(params) > 0 {
			host = params[0].(*api.Host)
			return c.createConfigFilesGroupHost(host)
		}
	}
	return nil
}

// createConfigFilesGroupCommon creates common config files
func (c *FilesGeneratorClickHouse) createConfigFilesGroupCommon(options *FilesGeneratorOptionsClickHouse) map[string]string {
	if options == nil {
		options = defaultConfigFilesGeneratorOptionsClickHouse()
	}
	commonConfigSections := make(map[string]string)
	// commonConfigSections maps section name to section XML chopConfig of the following sections:
	// 1. remote servers
	// 2. common settings
	// 3. common files
	util.IncludeNonEmpty(commonConfigSections, createConfigSectionFilename(configRemoteServers), c.configGenerator.getRemoteServers(options.GetRemoteServersOptions()))
	util.IncludeNonEmpty(commonConfigSections, createConfigSectionFilename(configSettings), c.configGenerator.getSettingsGlobal())
	util.MergeStringMapsOverwrite(commonConfigSections, c.configGenerator.getSectionFromFiles(api.SectionCommon, true, nil))
	// Extra user-specified config files
	util.MergeStringMapsOverwrite(commonConfigSections, c.chopConfig.ClickHouse.Config.File.Runtime.CommonConfigFiles)

	return commonConfigSections
}

// createConfigFilesGroupUsers creates users config files
func (c *FilesGeneratorClickHouse) createConfigFilesGroupUsers() map[string]string {
	commonUsersConfigSections := make(map[string]string)
	// commonUsersConfigSections maps section name to section XML chopConfig of the following sections:
	// 1. users
	// 2. quotas
	// 3. profiles
	// 4. user files
	util.IncludeNonEmpty(commonUsersConfigSections, createConfigSectionFilename(configUsers), c.configGenerator.getUsers())
	util.IncludeNonEmpty(commonUsersConfigSections, createConfigSectionFilename(configQuotas), c.configGenerator.getQuotas())
	util.IncludeNonEmpty(commonUsersConfigSections, createConfigSectionFilename(configProfiles), c.configGenerator.getProfiles())
	util.MergeStringMapsOverwrite(commonUsersConfigSections, c.configGenerator.getSectionFromFiles(api.SectionUsers, false, nil))
	// Extra user-specified config files
	util.MergeStringMapsOverwrite(commonUsersConfigSections, c.chopConfig.ClickHouse.Config.File.Runtime.UsersConfigFiles)

	return commonUsersConfigSections
}

// createConfigFilesGroupHost creates host config files
func (c *FilesGeneratorClickHouse) createConfigFilesGroupHost(host *api.Host) map[string]string {
	// Prepare for this replica deployment chopConfig files map as filename->content
	hostConfigSections := make(map[string]string)
	util.IncludeNonEmpty(hostConfigSections, createConfigSectionFilename(configMacros), c.configGenerator.getHostMacros(host))
	util.IncludeNonEmpty(hostConfigSections, createConfigSectionFilename(configHostnamePorts), c.configGenerator.getHostHostnameAndPorts(host))
	util.IncludeNonEmpty(hostConfigSections, createConfigSectionFilename(configZookeeper), c.configGenerator.getHostZookeeper(host))
	util.IncludeNonEmpty(hostConfigSections, createConfigSectionFilename(configSettings), c.configGenerator.getSettings(host))
	util.MergeStringMapsOverwrite(hostConfigSections, c.configGenerator.getSectionFromFiles(api.SectionHost, true, host))
	// Extra user-specified config files
	util.MergeStringMapsOverwrite(hostConfigSections, c.chopConfig.ClickHouse.Config.File.Runtime.HostConfigFiles)

	return hostConfigSections
}

// createConfigSectionFilename creates filename of a configuration file.
// filename depends on a section which it will contain
func createConfigSectionFilename(section string) string {
	return "chop-generated-" + section + ".xml"
}
