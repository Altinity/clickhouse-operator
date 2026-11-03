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

import "github.com/altinity/clickhouse-operator/pkg/util"

// FilesGenerator specifies configuration generator object
type FilesGeneratorDomain struct {
	configGenerator *Generator
}

// NewFilesGenerator creates new configuration files generator object
func NewFilesGeneratorDomain(configGenerator *Generator) *FilesGeneratorDomain {
	return &FilesGeneratorDomain{
		configGenerator: configGenerator,
	}
}

func (c *FilesGeneratorDomain) CreateConfigFilesGroupCommon(configSections map[string]string, options *FilesGeneratorOptions) {
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configRemoteServers), c.configGenerator.getRemoteServers(options.GetRemoteServersOptions()))
}

func (c *FilesGeneratorDomain) CreateConfigFilesGroupUsers(configSections map[string]string) {
	// users
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configUsers), c.configGenerator.getUsers())
	// quotas
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configQuotas), c.configGenerator.getQuotas())
	// profiles
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configProfiles), c.configGenerator.getProfiles())
}

func (c *FilesGeneratorDomain) CreateConfigFilesGroupHost(configSections map[string]string, options *FilesGeneratorOptions) {
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configMacros), c.configGenerator.getHostMacros(options.GetHost()))
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configHostnamePorts), c.configGenerator.getHostHostnameAndPorts(options.GetHost()))
	util.IncludeNonEmpty(configSections, createConfigSectionFilename(configZookeeper), c.configGenerator.getHostZookeeper(options.GetHost()))
}
