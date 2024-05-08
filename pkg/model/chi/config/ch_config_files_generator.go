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

// ClickHouseConfigFilesGenerator specifies clickhouse configuration generator object
type ClickHouseConfigFilesGenerator struct {
	// ClickHouse config generator
	chConfigGenerator *ClickHouseConfigGenerator
	// clickhouse-operator configuration
	chopConfig *api.OperatorConfig
}

// NewClickHouseConfigFilesGenerator creates new clickhouse configuration generator object
func NewClickHouseConfigFilesGenerator(chi *api.ClickHouseInstallation) *ClickHouseConfigFilesGenerator {
	return &ClickHouseConfigFilesGenerator{
		chConfigGenerator: newClickHouseConfigGenerator(chi),
		chopConfig:        chop.Config(),
	}
}

// CreateConfigFilesGroupCommon creates common config files
func (c *ClickHouseConfigFilesGenerator) CreateConfigFilesGroupCommon(options *ClickHouseConfigFilesGeneratorOptions) map[string]string {
	if options == nil {
		options = defaultClickHouseConfigFilesGeneratorOptions()
	}
	commonConfigSections := make(map[string]string)
	// commonConfigSections maps section name to section XML chopConfig of the following sections:
	// 1. remote servers
	// 2. common settings
	// 3. common files
	util.IncludeNonEmpty(commonConfigSections, createConfigSectionFilename(configRemoteServers), c.chConfigGenerator.getRemoteServers(options.GetRemoteServersGeneratorOptions()))
	util.IncludeNonEmpty(commonConfigSections, createConfigSectionFilename(configSettings), c.chConfigGenerator.getSettingsGlobal())
	util.MergeStringMapsOverwrite(commonConfigSections, c.chConfigGenerator.getSectionFromFiles(api.SectionCommon, true, nil))
	// Extra user-specified config files
	util.MergeStringMapsOverwrite(commonConfigSections, c.chopConfig.ClickHouse.Config.File.Runtime.CommonConfigFiles)

	return commonConfigSections
}

// CreateConfigFilesGroupUsers creates users config files
func (c *ClickHouseConfigFilesGenerator) CreateConfigFilesGroupUsers() map[string]string {
	commonUsersConfigSections := make(map[string]string)
	// commonUsersConfigSections maps section name to section XML chopConfig of the following sections:
	// 1. users
	// 2. quotas
	// 3. profiles
	// 4. user files
	util.IncludeNonEmpty(commonUsersConfigSections, createConfigSectionFilename(configUsers), c.chConfigGenerator.getUsers())
	util.IncludeNonEmpty(commonUsersConfigSections, createConfigSectionFilename(configQuotas), c.chConfigGenerator.getQuotas())
	util.IncludeNonEmpty(commonUsersConfigSections, createConfigSectionFilename(configProfiles), c.chConfigGenerator.getProfiles())
	util.MergeStringMapsOverwrite(commonUsersConfigSections, c.chConfigGenerator.getSectionFromFiles(api.SectionUsers, false, nil))
	// Extra user-specified config files
	util.MergeStringMapsOverwrite(commonUsersConfigSections, c.chopConfig.ClickHouse.Config.File.Runtime.UsersConfigFiles)

	return commonUsersConfigSections
}

// CreateConfigFilesGroupHost creates host config files
func (c *ClickHouseConfigFilesGenerator) CreateConfigFilesGroupHost(host *api.ChiHost) map[string]string {
	// Prepare for this replica deployment chopConfig files map as filename->content
	hostConfigSections := make(map[string]string)
	util.IncludeNonEmpty(hostConfigSections, createConfigSectionFilename(configMacros), c.chConfigGenerator.getHostMacros(host))
	util.IncludeNonEmpty(hostConfigSections, createConfigSectionFilename(configHostnamePorts), c.chConfigGenerator.getHostHostnameAndPorts(host))
	util.IncludeNonEmpty(hostConfigSections, createConfigSectionFilename(configZookeeper), c.chConfigGenerator.getHostZookeeper(host))
	util.IncludeNonEmpty(hostConfigSections, createConfigSectionFilename(configSettings), c.chConfigGenerator.getSettings(host))
	util.MergeStringMapsOverwrite(hostConfigSections, c.chConfigGenerator.getSectionFromFiles(api.SectionHost, true, host))
	// Extra user-specified config files
	util.MergeStringMapsOverwrite(hostConfigSections, c.chopConfig.ClickHouse.Config.File.Runtime.HostConfigFiles)

	return hostConfigSections
}

// createConfigSectionFilename creates filename of a configuration file.
// filename depends on a section which it will contain
func createConfigSectionFilename(section string) string {
	return "chop-generated-" + section + ".xml"
}

// ClickHouseConfigFilesGeneratorOptions specifies options for clickhouse configuration generator
type ClickHouseConfigFilesGeneratorOptions struct {
	RemoteServersGeneratorOptions *RemoteServersGeneratorOptions
}

// NewClickHouseConfigFilesGeneratorOptions creates new options for clickhouse configuration generator
func NewClickHouseConfigFilesGeneratorOptions() *ClickHouseConfigFilesGeneratorOptions {
	return &ClickHouseConfigFilesGeneratorOptions{}
}

// GetRemoteServersGeneratorOptions gets remote-servers generator options
func (o *ClickHouseConfigFilesGeneratorOptions) GetRemoteServersGeneratorOptions() *RemoteServersGeneratorOptions {
	if o == nil {
		return nil
	}
	return o.RemoteServersGeneratorOptions
}

// SetRemoteServersGeneratorOptions sets remote-servers generator options
func (o *ClickHouseConfigFilesGeneratorOptions) SetRemoteServersGeneratorOptions(opts *RemoteServersGeneratorOptions) *ClickHouseConfigFilesGeneratorOptions {
	if o == nil {
		return nil
	}
	o.RemoteServersGeneratorOptions = opts

	return o
}

// defaultClickHouseConfigFilesGeneratorOptions creates new default options for clickhouse config generator
func defaultClickHouseConfigFilesGeneratorOptions() *ClickHouseConfigFilesGeneratorOptions {
	return NewClickHouseConfigFilesGeneratorOptions()
}
