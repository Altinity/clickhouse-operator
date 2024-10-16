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
	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// FilesGenerator specifies configuration generator object
type FilesGenerator struct {
	configGenerator *Generator
	// clickhouse-operator configuration
	chopConfig *chi.OperatorConfig
}

// NewFilesGenerator creates new configuration files generator object
func NewFilesGenerator(cr chi.ICustomResource, namer interfaces.INameManager, opts *GeneratorOptions) *FilesGenerator {
	return &FilesGenerator{
		configGenerator: newGenerator(cr, namer, opts),
		chopConfig:      chop.Config(),
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
		var options *FilesGeneratorOptions
		if len(params) > 0 {
			options = params[0].(*FilesGeneratorOptions)
			return c.createConfigFilesGroupHost(options)
		}
	}
	return nil
}

// createConfigFilesGroupCommon creates common config files
func (c *FilesGenerator) createConfigFilesGroupCommon(options *FilesGeneratorOptions) map[string]string {
	if options == nil {
		options = defaultFilesGeneratorOptions()
	}
	// Common ConfigSections maps section name to section XML
	configSections := make(map[string]string)

	c.createConfigFilesGroupCommonDomain(configSections, options)
	c.createConfigFilesGroupCommonGeneric(configSections, options)

	return configSections
}

func (c *FilesGenerator) createConfigFilesGroupCommonDomain(configSections map[string]string, options *FilesGeneratorOptions) {
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configRemoteServers), c.configGenerator.getRemoteServers(options.GetRemoteServersOptions()))
}

func (c *FilesGenerator) createConfigFilesGroupCommonGeneric(configSections map[string]string, options *FilesGeneratorOptions) {
	// common settings
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configSettings), c.configGenerator.getGlobalSettings())
	// common files
	util.MergeStringMapsOverwrite(configSections, c.configGenerator.getSectionFromFiles(chi.SectionCommon, true, nil))
	// Extra user-specified config files
	util.MergeStringMapsOverwrite(configSections, c.chopConfig.ClickHouse.Config.File.Runtime.CommonConfigFiles)
}

// createConfigFilesGroupUsers creates users config files
func (c *FilesGenerator) createConfigFilesGroupUsers() map[string]string {
	// CommonUsers ConfigSections maps section name to section XML
	configSections := make(map[string]string)

	c.createConfigFilesGroupUsersDomain(configSections)
	c.createConfigFilesGroupUsersGeneric(configSections)

	return configSections
}

func (c *FilesGenerator) createConfigFilesGroupUsersDomain(configSections map[string]string) {
	// users
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configUsers), c.configGenerator.getUsers())
	// quotas
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configQuotas), c.configGenerator.getQuotas())
	// profiles
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configProfiles), c.configGenerator.getProfiles())
}

func (c *FilesGenerator) createConfigFilesGroupUsersGeneric(configSections map[string]string) {
	// user files
	util.MergeStringMapsOverwrite(configSections, c.configGenerator.getSectionFromFiles(chi.SectionUsers, false, nil))
	// Extra user-specified config files
	util.MergeStringMapsOverwrite(configSections, c.chopConfig.ClickHouse.Config.File.Runtime.UsersConfigFiles)
}

// createConfigFilesGroupHost creates host config files
func (c *FilesGenerator) createConfigFilesGroupHost(options *FilesGeneratorOptions) map[string]string {
	// Prepare for this replica deployment chopConfig files map as filename->content
	configSections := make(map[string]string)

	c.createConfigFilesGroupHostDomain(configSections, options)
	c.createConfigFilesGroupHostGeneric(configSections, options)

	return configSections
}

func (c *FilesGenerator) createConfigFilesGroupHostDomain(configSections map[string]string, options *FilesGeneratorOptions) {
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configMacros), c.configGenerator.getHostMacros(options.GetHost()))
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configHostnamePorts), c.configGenerator.getHostHostnameAndPorts(options.GetHost()))
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configZookeeper), c.configGenerator.getHostZookeeper(options.GetHost()))
}

func (c *FilesGenerator) createConfigFilesGroupHostGeneric(configSections map[string]string, options *FilesGeneratorOptions) {
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configSettings), c.configGenerator.getHostSettings(options.GetHost()))
	util.MergeStringMapsOverwrite(configSections, c.configGenerator.getSectionFromFiles(chi.SectionHost, true, options.GetHost()))
	// Extra user-specified config files
	util.MergeStringMapsOverwrite(configSections, c.chopConfig.ClickHouse.Config.File.Runtime.HostConfigFiles)
}

// createConfigSectionFilename creates filename of a configuration file.
// filename depends on a section which it will contain
func createConfigSectionFilename(section string) string {
	return "chop-generated-" + section + ".xml"
}
