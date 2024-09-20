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
	"strings"

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

// getSettingsGlobal creates data for global section of "settings.xml"
func (c *Generator) getSettingsGlobal() string {
	// No host specified means request to generate common config
	return c.generateXMLConfig(c.opts.Settings, "")
}

// getSettingsHost creates data for host section of "settings.xml"
func (c *Generator) getSettingsHost(host *chi.Host) string {
	// Generate config for the specified host
	return c.getHostConfig(host, host.Settings.MergeFrom(defaultSettings()))
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

// getHostConfig builds string host config based on provided `settings`
func (c *Generator) getHostConfig(host *chi.Host, settings *chi.Settings) string {
	if settings.Len() == 0 {
		return ""
	}

	// Init container will replace this macro with real value on bootstrap
	serverId, _ := chi.NewSettingScalarFromAny(getServerId(host))
	settings.Set("keeper_server/server_id", serverId)

	// Prepare empty placeholder for RAFT config and will be replaced later in this function:
	// <raft_configuration>
	//     <server></server>
	// </raft_configuration>
	settings.Set("keeper_server/raft_configuration/server", chi.NewSettingScalar(""))
	// 3-rd level (keeper_server/raft_configuration/server) by 4 spaces
	raftIndent := 12
	// Prepare RAFT config for injection into main config
	raft := &bytes.Buffer{}
	host.GetShard().WalkHosts(func(_host *chi.Host) error {
		util.Iline(raft, raftIndent, "<server>")
		util.Iline(raft, raftIndent, "    <id>%d</id>", getServerId(_host))
		util.Iline(raft, raftIndent, "    <hostname>%s</hostname>", c.namer.Name(interfaces.NameInstanceHostname, _host))
		util.Iline(raft, raftIndent, "    <port>%d</port>", _host.RaftPort.Value())
		util.Iline(raft, raftIndent, "</server>")
		return nil
	})

	// <clickhouse>
	// 		CONFIG
	// </clickhouse>
	config := &bytes.Buffer{}
	util.Iline(config, 0, "<clickhouse>")
	xml.GenerateFromSettings(config, settings, "")
	util.Iline(config, 0, "</clickhouse>")

	// tmp = strings.Replace(tmp, "<server_id></server_id>", "<server_id from_env=\"KEEPER_ID\" />", 1)

	// Inject RAFT config
	// Replace empty placeholder for RAFT config with actual RAFT config value
	// <raft_configuration>
	//     <server></server>
	// </raft_configuration>
	// TODO use raftIndent value here instead of spaces
	return strings.Replace(config.String(), "            <server></server>\n", raft.String(), 1)
}

func getServerId(host *chi.Host) int {
	return host.GetRuntime().GetAddress().GetReplicaIndex() + 1
}
