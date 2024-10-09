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
	"github.com/altinity/clickhouse-operator/pkg/model/common/config"
	"strings"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
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
func (c *Generator) getRaftConfig(selector *config.HostSelector) string {
	if selector == nil {
		selector = defaultIncludeAllSelector()
	}

	// Prepare empty placeholder for RAFT config which will be replaced later with real raft config
	// <raft_configuration>
	//     <server></server>
	// </raft_configuration>
	config := chi.NewSettings().Set("keeper_server/raft_configuration/server", chi.NewSettingScalar("")).ClickHouseConfig()

	// Prepare RAFT config
	// 3-rd level (keeper_server/raft_configuration/server) by 4 spaces
	raftIndent := 12
	// Prepare RAFT config for injection into main config
	raft := &bytes.Buffer{}
	c.cr.WalkHosts(func(host *chi.Host) error {
		msg := fmt.Sprintf("SKIP host from RAFT servers: %s", host.GetName())
		if selector.Include(host) {
			util.Iline(raft, raftIndent, "<server>")
			util.Iline(raft, raftIndent, "    <id>%d</id>", getServerId(host))
			util.Iline(raft, raftIndent, "    <hostname>%s</hostname>", c.namer.Name(interfaces.NameInstanceHostname, host))
			util.Iline(raft, raftIndent, "    <port>%d</port>", host.RaftPort.Value())
			util.Iline(raft, raftIndent, "</server>")
			msg = fmt.Sprintf("Add host to RAFT servers: %s", host.GetName())
		}
		log.V(1).M(host).Info(msg)
		return nil
	})

	// Inject RAFT config
	// Replace empty placeholder for RAFT config with actual RAFT config value
	// <raft_configuration>
	//     <server></server>
	// </raft_configuration>
	// TODO use raftIndent value here instead of spaces
	return strings.Replace(config, "            <server></server>\n", raft.String(), 1)
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
