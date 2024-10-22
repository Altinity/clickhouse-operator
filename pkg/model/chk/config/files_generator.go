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
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// FilesGenerator specifies configuration generator object
type FilesGenerator struct {
	configGeneratorGeneric IConfigGeneratorGeneric
	// paths to additional config files
	pathsGetter chi.IOperatorConfigFilesPathsGetter
	// configFilesGeneratorDomain files generator
	configFilesGeneratorDomain IFilesGeneratorDomain
}

type IConfigGeneratorGeneric interface {
	GetGlobalSettings() string
	GetSectionFromFiles(section chi.SettingsSection, includeUnspecified bool, host *chi.Host) map[string]string
	GetHostSettings(host *chi.Host) string
}

type IFilesGeneratorDomain interface {
	CreateConfigFilesGroupCommon(configSections map[string]string, options *FilesGeneratorOptions)
	CreateConfigFilesGroupUsers(configSections map[string]string)
	CreateConfigFilesGroupHost(configSections map[string]string, options *FilesGeneratorOptions)
}

// NewFilesGenerator creates new configuration files generator object
func NewFilesGenerator(
	configGeneratorGeneric IConfigGeneratorGeneric,
	pathsGetter chi.IOperatorConfigFilesPathsGetter,
	configFilesGeneratorDomain IFilesGeneratorDomain,
) *FilesGenerator {
	return &FilesGenerator{
		configGeneratorGeneric:     configGeneratorGeneric,
		pathsGetter:                pathsGetter,
		configFilesGeneratorDomain: configFilesGeneratorDomain,
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
	c.configFilesGeneratorDomain.CreateConfigFilesGroupCommon(configSections, options)
}

func (c *FilesGenerator) createConfigFilesGroupCommonGeneric(configSections map[string]string, options *FilesGeneratorOptions) {
	// common settings
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configSettings), c.configGeneratorGeneric.GetGlobalSettings())
	// common files
	util.MergeStringMapsOverwrite(configSections, c.configGeneratorGeneric.GetSectionFromFiles(chi.SectionCommon, true, nil))
	// Extra user-specified config files
	util.MergeStringMapsOverwrite(configSections, c.pathsGetter.GetCommonConfigFiles())
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
	c.configFilesGeneratorDomain.CreateConfigFilesGroupUsers(configSections)
}

func (c *FilesGenerator) createConfigFilesGroupUsersGeneric(configSections map[string]string) {
	// user files
	util.MergeStringMapsOverwrite(configSections, c.configGeneratorGeneric.GetSectionFromFiles(chi.SectionUsers, false, nil))
	// Extra user-specified config files
	util.MergeStringMapsOverwrite(configSections, c.pathsGetter.GetUsersConfigFiles())
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
	c.configFilesGeneratorDomain.CreateConfigFilesGroupHost(configSections, options)
}

func (c *FilesGenerator) createConfigFilesGroupHostGeneric(configSections map[string]string, options *FilesGeneratorOptions) {
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configSettings), c.configGeneratorGeneric.GetHostSettings(options.GetHost()))
	util.MergeStringMapsOverwrite(configSections, c.configGeneratorGeneric.GetSectionFromFiles(chi.SectionHost, true, options.GetHost()))
	// Extra user-specified config files
	util.MergeStringMapsOverwrite(configSections, c.pathsGetter.GetHostConfigFiles())
}

// createConfigSectionFilename creates filename of a configuration file.
// filename depends on a section which it will contain
func createConfigSectionFilename(section string) string {
	return "chop-generated-" + section + ".xml"
}
