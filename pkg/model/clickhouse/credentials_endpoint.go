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

package clickhouse

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

const (
	// http://user:password@host:8123
	chDsnUrlPattern = "%s://%s%s:%s"

	usernameReplacer = "***"
	passwordReplacer = "***"

	dsnUsernamePasswordPairPattern             = "%s:%s@"
	dsnUsernamePasswordPairUsernameOnlyPattern = "%s@"

	// tlsSettingsLegacy is the registry key used when no per-endpoint TLS knobs
	// are configured (the legacy path). Identical knobs across endpoints share
	// this key. Endpoints with explicit Verify/MinVersion/ServerName/rootCA get
	// a content-hash key (see tlsRegistryKey) so concurrent reconciles of
	// differently-configured clusters cannot race on a global registration.
	tlsSettingsLegacy = "tls-settings"
)

// EndpointCredentials specifies credentials to access specified endpoint
type EndpointCredentials struct {
	// External data
	scheme   string
	hostname string
	username string
	password string
	rootCA   string
	port     int

	// TLS hardening knobs — empty values preserve legacy behavior.
	tlsVerify     api.TLSVerify
	tlsMinVersion api.TLSMinVersion
	tlsServerName string

	// Internal generated data
	tlsConfigKey         string
	dsn                  string
	dsnHiddenCredentials string
	dsnLogQueries        string
}

// NewEndpointCredentials creates new EndpointCredentials object
func NewEndpointCredentials(scheme, hostname, username, password, rootCA string, port int) *EndpointCredentials {
	params := &EndpointCredentials{
		scheme:   scheme,
		hostname: hostname,
		username: username,
		password: password,
		rootCA:   rootCA,
		port:     port,
	}
	params.refreshDSN()
	return params
}

// SetTLSSecurity injects TLS hardening knobs. Empty values preserve legacy behavior.
// Recomputes the DSN(s) since the tls_config registry key is derived from these knobs.
func (c *EndpointCredentials) SetTLSSecurity(verify api.TLSVerify, minVersion api.TLSMinVersion, serverName string) *EndpointCredentials {
	if c == nil {
		return nil
	}
	c.tlsVerify = verify
	c.tlsMinVersion = minVersion
	c.tlsServerName = serverName
	c.refreshDSN()
	return c
}

// refreshDSN recomputes the registry key and DSN cache after any change to
// security-affecting fields.
func (c *EndpointCredentials) refreshDSN() {
	c.tlsConfigKey = c.computeTLSRegistryKey()
	c.dsn = c.makeDSN(false)
	c.dsnHiddenCredentials = c.makeDSN(true)
	c.dsnLogQueries = c.makeDSNLogQueries(false)
}

// computeTLSRegistryKey returns the go-clickhouse RegisterTLSConfig key for this
// endpoint. Endpoints with no explicit knobs share the legacy key (preserves
// today's single-registration behavior); endpoints with any knob set get a
// content-hash key so concurrent reconciles of differently-configured clusters
// can register independent tls.Config values without overwriting each other.
//
// The 8-byte (64-bit) sha256 prefix gives collision probability ≈ N²/2^65, which
// is negligible for any plausible operator (millions of distinct knob-combinations
// would still leave collision probability well below 10⁻⁶). A collision is only
// dangerous if two endpoints registered DIFFERENT configs under the SAME key —
// since the key derives from the config content, identical configs sharing a key
// is by design.
func (c *EndpointCredentials) computeTLSRegistryKey() string {
	if (c.tlsVerify == "") && (c.tlsMinVersion == "") && (c.tlsServerName == "") && (c.rootCA == "") {
		return tlsSettingsLegacy
	}
	h := sha256.Sum256([]byte(
		string(c.tlsVerify) + "|" + string(c.tlsMinVersion) + "|" + c.tlsServerName + "|" + c.rootCA,
	))
	return "tls-settings-" + hex.EncodeToString(h[:8])
}

// TLSVerify returns the resolved TLS verify policy.
func (c *EndpointCredentials) TLSVerify() api.TLSVerify {
	if c == nil {
		return ""
	}
	return c.tlsVerify
}

// TLSMinVersion returns the configured min TLS version.
func (c *EndpointCredentials) TLSMinVersion() api.TLSMinVersion {
	if c == nil {
		return ""
	}
	return c.tlsMinVersion
}

// TLSServerName returns the explicit ServerName for the TLS handshake, or empty.
func (c *EndpointCredentials) TLSServerName() string {
	if c == nil {
		return ""
	}
	return c.tlsServerName
}

// TLSConfigKey returns the go-clickhouse RegisterTLSConfig registry key for
// this endpoint.
func (c *EndpointCredentials) TLSConfigKey() string {
	if c == nil {
		return tlsSettingsLegacy
	}
	return c.tlsConfigKey
}

// Hostname returns the dial host (used as ServerName fallback in setupTLSAdvanced).
func (c *EndpointCredentials) Hostname() string {
	if c == nil {
		return ""
	}
	return c.hostname
}

// RootCA returns the raw rootCA payload (PEM or base64-wrapped).
func (c *EndpointCredentials) RootCA() string {
	if c == nil {
		return ""
	}
	return c.rootCA
}

// formatUsernamePassword formats username and password pair
func (c *EndpointCredentials) formatUsernamePassword(username, password string) string {
	// We may have neither username nor password
	if username == "" && password == "" {
		return ""
	}

	// Password may be omitted
	if password == "" {
		return fmt.Sprintf(dsnUsernamePasswordPairUsernameOnlyPattern, username)
	}

	// Expecting both username and password to be in place
	return fmt.Sprintf(dsnUsernamePasswordPairPattern, username, password)
}

// makeUsernamePassword makes "username:password" pair for connection
func (c *EndpointCredentials) makeUsernamePassword(hidden bool) string {
	// In case of hidden username+password pair we'd just return replacement
	if hidden {
		//return c.usernamePassword(usernameReplacer, passwordReplacer)
		pwd := c.password
		if c.password != "" {
			pwd = passwordReplacer
		}
		return c.formatUsernamePassword(c.username, pwd)
	}

	return c.formatUsernamePassword(c.username, c.password)
}

// makeDSN makes ClickHouse DSN
func (c *EndpointCredentials) makeDSN(hideCredentials bool) string {
	baseUrl := fmt.Sprintf(
		chDsnUrlPattern,
		c.scheme,
		c.makeUsernamePassword(hideCredentials),
		c.hostname,
		strconv.Itoa(c.port),
	)
	if c.scheme == api.ChSchemeHTTPS {
		baseUrl += "?tls_config=" + c.tlsConfigKey
	}
	return baseUrl
}

// makeDSN makes ClickHouse DSN
func (c *EndpointCredentials) makeDSNLogQueries(hideCredentials bool) string {
	baseUrl := fmt.Sprintf(
		chDsnUrlPattern,
		c.scheme,
		c.makeUsernamePassword(hideCredentials),
		c.hostname,
		strconv.Itoa(c.port),
	)
	baseUrl += "?log_queries=1"
	if c.scheme == api.ChSchemeHTTPS {
		baseUrl += "&tls_config=" + c.tlsConfigKey
	}
	return baseUrl
}

// GetDSN gets DSN
func (c *EndpointCredentials) GetDSN() string {
	return c.dsn
}

// GetDSNWithHiddenCredentials gets DSN with hidden sensitive info
func (c *EndpointCredentials) GetDSNWithHiddenCredentials() string {
	return c.dsnHiddenCredentials
}

// GetDSNLogQuery gets DSN with hidden sensitive info
func (c *EndpointCredentials) GetDSNLogQueries() string {
	return c.dsnLogQueries
}
