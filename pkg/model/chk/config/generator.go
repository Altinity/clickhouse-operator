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
	"bytes"
	"fmt"
	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
	"github.com/altinity/clickhouse-operator/pkg/xml"
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

// generateXMLConfig creates XML using map[string]string definitions
func (c *Generator) generateXMLConfig(settings *chi.Settings, prefix string) string {
	if settings.Len() == 0 {
		return ""
	}

	b := &bytes.Buffer{}
	// <yandex>
	// XML code
	// </yandex>
	util.Iline(b, 0, "<"+xmlTagYandex+">")
	xml.GenerateFromSettings(b, settings, prefix)
	util.Iline(b, 0, "</"+xmlTagYandex+">")

	return b.String()
}

// getGlobalSettings creates data for global section of "settings.xml"
func (c *Generator) getGlobalSettings() string {
	// No host specified means request to generate common config
	return c.generateXMLConfig(c.opts.Settings, "")
}

// getHostSettings creates data for host section of "settings.xml"
func (c *Generator) getHostSettings(host *chi.Host) string {
	// Generate config for the specified host
	return c.generateXMLConfig(host.Settings, "")
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

// getHostRaft builds host raft config
func (c *Generator) getHostRaft(host *chi.Host) string {
	settings := chi.NewSettings()

	serverId, _ := chi.NewSettingScalarFromAny(getServerId(host))
	settings.Set("keeper_server/server_id", serverId)

	host.GetCR().WalkHosts(func(_host *chi.Host) error {
		settings.Set("keeper_server/raft_configuration/server/id", chi.NewSettingScalar(fmt.Sprintf("%d", getServerId(_host))))
		settings.Set("keeper_server/raft_configuration/server/hostname", chi.NewSettingScalar(c.namer.Name(interfaces.NameInstanceHostname, _host)))
		settings.Set("keeper_server/raft_configuration/server/port", chi.NewSettingScalar(fmt.Sprintf("%d", _host.RaftPort.Value())))
		return nil
	})

	// <clickhouse>
	// 		CONFIG
	// </clickhouse>
	config := &bytes.Buffer{}
	util.Iline(config, 0, "<clickhouse>")
	xml.GenerateFromSettings(config, settings, "")
	util.Iline(config, 0, "</clickhouse>")

	return config.String()
}

func getServerId(host *chi.Host) int {
	return host.GetRuntime().GetAddress().GetReplicaIndex()
}
