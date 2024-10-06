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
)

// Generator generates configuration files content for specified CR
// Configuration files content is an XML ATM, so config generator provides set of Get*() functions
// which produces XML which are parts of configuration and can/should be used as content of config files.
type Generator struct {
	cr    chi.ICustomResource
	namer interfaces.INameManager
	opts  *GeneratorOptions
}

// newGenerator returns new Generator struct
func newGenerator(cr chi.ICustomResource, namer interfaces.INameManager, opts *GeneratorOptions) *Generator {
	return &Generator{
		cr:    cr,
		namer: namer,
		opts:  opts,
	}
}

// getGlobalSettings creates data for global section of "settings.xml"
func (c *Generator) getGlobalSettings() string {
	// No host specified means request to generate common config
	return c.opts.Settings.ClickHouseConfig()
}

// getHostSettings creates data for host section of "settings.xml"
func (c *Generator) getHostSettings(host *chi.Host) string {
	// Generate config for the specified host
	return host.Settings.ClickHouseConfig()
}

// getSectionFromFiles creates data for custom common config files
func (c *Generator) getSectionFromFiles(section chi.SettingsSection, includeUnspecified bool, host *chi.Host) map[string]string {
	var files *chi.Settings
	if host == nil {
		// We are looking into Common files
		files = c.opts.Files
	} else {
		// We are looking into host's personal files
		files = host.Files
	}

	// Extract particular section from files

	return files.GetSection(section, includeUnspecified)
}

// getRaftConfig builds raft config for the chk
func (c *Generator) getRaftConfig() string {
	settings := chi.NewSettings()
	c.cr.WalkHosts(func(_host *chi.Host) error {
		settings.Set("keeper_server/raft_configuration/server/id", chi.MustNewSettingScalarFromAny(getServerId(_host)))
		settings.Set("keeper_server/raft_configuration/server/hostname", chi.NewSettingScalar(c.namer.Name(interfaces.NameInstanceHostname, _host)))
		settings.Set("keeper_server/raft_configuration/server/port", chi.MustNewSettingScalarFromAny(_host.RaftPort.Value()))
		return nil
	})
	return settings.ClickHouseConfig()
}

// getHostServerId builds server id config for the host
func (c *Generator) getHostServerId(host *chi.Host) string {
	settings := chi.NewSettings()
	settings.Set("keeper_server/server_id", chi.MustNewSettingScalarFromAny(getServerId(host)))
	return settings.ClickHouseConfig()
}

func getServerId(host *chi.Host) int {
	return host.GetRuntime().GetAddress().GetReplicaIndex()
}
