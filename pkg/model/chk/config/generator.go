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
			if host.IsSecure() {
				util.Iline(raft, i, "    <secure>1</secure>")
			}
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

// getHostListenersOverride builds a per-host XML overlay that opens Keeper's
// secure client-facing listener. Emits nothing when the host stays on the
// legacy default (insecure exposed, secure absent) so existing CHKs see
// byte-identical configmap content on upgrade.
//
// Semantics:
//   - host.IsSecure() == true: emit `<tcp_port_secure>` so Keeper opens the
//     TLS port. The user-supplied `<openSSL>` server XML (settings or files)
//     is responsible for the TLS handshake; the operator just opens the port.
//
// This file lands in the per-host config dir (conf.d, DirPathConfigHost). The
// secure port is per-host (PortDistributionClusterScopeIndex offsets
// host.ZKPortSecure per replica), so it must stay host-scoped here rather than
// in the shared common configmap. Adding `<tcp_port_secure>` is purely
// additive — no file in keeper_config.d defines it — so a per-host override
// merges cleanly.
//
// The plaintext-port REMOVAL is NOT emitted here: a `<tcp_port remove="1"/>`
// placed in conf.d cannot delete the static `<tcp_port>2181</tcp_port>` that
// the default overlay (keeper_config.d/01-keeper-01-default-config.xml) ships
// in keeper_config.d — Keeper merges keeper_config.d after conf.d, so the
// static port is re-added after the conf.d removal runs. The removal therefore
// lives in the common group alongside the definition it deletes; see
// getPlaintextListenerRemoval.
func (c *Generator) getHostListenersOverride(host *chi.Host) string {
	if host == nil {
		return ""
	}
	if !(host.IsSecure() && host.ZKPortSecure.HasValue()) {
		return ""
	}
	body := &bytes.Buffer{}
	// Indent 8 = children of <keeper_server> (depth 2: clickhouse/keeper_server/<child>).
	// Matches the indent convention used by getRaftConfig for analogous depth.
	util.Iline(body, 8, "<tcp_port_secure>%d</tcp_port_secure>", host.ZKPortSecure.Value())
	return chi.NewSettings().
		Set("keeper_server", chi.MustNewSettingScalarFromAny(body).SetEmbed()).
		ClickHouseConfig()
}

// getPlaintextListenerRemoval emits a common-group overlay that deletes the
// static `<tcp_port>2181</tcp_port>` shipped by
// keeper_config.d/01-keeper-01-default-config.xml, so a fully-secure Keeper
// binds no plaintext client listener.
//
// It MUST land in the common config dir (keeper_config.d, DirPathConfigCommon),
// the same directory as the static definition it removes: the `remove="1"`
// preprocessor directive only wins when it is merged after the definition, and
// Keeper merges keeper_config.d after the per-host conf.d. (The user-confirmed
// workaround — `keeper_server/tcp_port: _removed_` as a CR setting — works for
// exactly this reason: CR settings render into keeper_config.d.)
//
// The removal is CR-global (the common configmap is shared by every pod), so it
// is emitted only when EVERY host has its plaintext port closed (!IsInsecure()).
// For the legacy default (insecure exposed) it returns "" and no file is
// emitted, preserving byte-identical configmap content on upgrade.
func (c *Generator) getPlaintextListenerRemoval() string {
	anyHost := false
	allPlaintextClosed := true
	c.cr.WalkHosts(func(host *chi.Host) error {
		anyHost = true
		// A nil host carries no posture; treat it conservatively as "not closed"
		// so a phantom host never causes the plaintext port to be stripped.
		if host == nil || host.IsInsecure() {
			allPlaintextClosed = false
		}
		return nil
	})
	if !anyHost || !allPlaintextClosed {
		return ""
	}
	body := &bytes.Buffer{}
	// Indent 8 = children of <keeper_server> (depth 2: clickhouse/keeper_server/<child>).
	util.Iline(body, 8, `<tcp_port remove="1"/>`)
	return chi.NewSettings().
		Set("keeper_server", chi.MustNewSettingScalarFromAny(body).SetEmbed()).
		ClickHouseConfig()
}

func getServerId(host *chi.Host) int {
	return host.GetRuntime().GetAddress().GetReplicaIndex()
}
