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

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/common/config"
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

// NewGenerator returns new Generator struct
func NewGenerator(cr chi.ICustomResource, namer interfaces.INameManager, opts *GeneratorOptions) *Generator {
	return &Generator{
		cr:    cr,
		namer: namer,
		opts:  opts,
	}
}

// GetGlobalSettings creates data for global section of "settings.xml"
func (c *Generator) GetGlobalSettings() string {
	// No host specified means request to generate common config
	return c.opts.Settings.ClickHouseConfig()
}

// GetHostSettings creates data for host section of "settings.xml"
func (c *Generator) GetHostSettings(host *chi.Host) string {
	// Generate config for the specified host
	return host.Settings.ClickHouseConfig()
}

// GetSectionFromFiles creates data for custom common config files
func (c *Generator) GetSectionFromFiles(section chi.SettingsSection, includeUnspecified bool, host *chi.Host) map[string]string {
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
		selector = defaultSelectorIncludeAll()
	}

	// Prepare RAFT config
	// Indent is 12 = 3-rd level (clickhouse/keeper_server/raft_configuration) by 4 spaces
	i := 12
	raft := &bytes.Buffer{}
	c.cr.WalkHosts(func(host *chi.Host) error {
		msg := fmt.Sprintf("SKIP host from RAFT servers: %s", host.GetName())
		if selector.Include(host) {
			util.Iline(raft, i, "<server>")
			util.Iline(raft, i, "    <id>%d</id>", getServerId(host))
			util.Iline(raft, i, "    <hostname>%s</hostname>", c.namer.Name(interfaces.NameInstanceHostname, host))
			util.Iline(raft, i, "    <port>%d</port>", host.RaftPort.Value())
			util.Iline(raft, i, "</server>")
			msg = fmt.Sprintf("Add host to RAFT servers: %s", host.GetName())
		}
		log.V(1).M(host).Info(msg)
		return nil
	})

	return chi.NewSettings().Set("keeper_server/raft_configuration", chi.MustNewSettingScalarFromAny(raft).SetEmbed()).ClickHouseConfig()
}

// getHostServerId builds server id config for the host
func (c *Generator) getHostServerId(host *chi.Host) string {
	return chi.NewSettings().Set("keeper_server/server_id", chi.MustNewSettingScalarFromAny(getServerId(host))).ClickHouseConfig()
}

func getServerId(host *chi.Host) int {
	return host.GetRuntime().GetAddress().GetReplicaIndex()
}
