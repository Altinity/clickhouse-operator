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
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// TestEndpointTLSConfigKey_ClientCertificateIsAKnob verifies that a client certificate alone
// moves an endpoint off the legacy key: the legacy registration (InsecureSkipVerify, no certificate) must
// never serve an endpoint that is configured to present one.
func TestEndpointTLSConfigKey_ClientCertificateIsAKnob(t *testing.T) {
	c := NewEndpointCredentials("https", "host", "u", "p", "", 8443)
	c.SetClientCertificate("/etc/client/tls.crt", "/etc/client/tls.key")
	require.NotEqual(t, tlsSettingsLegacy, c.TLSConfigKey())
	require.Contains(t, c.GetDSN(), "tls_config="+c.TLSConfigKey())
}

// TestEndpointTLSConfigKey_ClientCertificatePathsChangeTheKey verifies that otherwise identical
// endpoints with different certificate paths do not share a registration, and identical paths do.
func TestEndpointTLSConfigKey_ClientCertificatePathsChangeTheKey(t *testing.T) {
	newEndpoint := func(certFile, keyFile string) *EndpointCredentials {
		c := NewEndpointCredentials("https", "host", "u", "p", "", 8443)
		c.SetTLSSecurity(api.TLSVerifyStrict, api.TLSMinVersion12, "")
		c.SetClientCertificate(certFile, keyFile)
		return c
	}
	none := newEndpoint("", "")
	a := newEndpoint("/etc/a/tls.crt", "/etc/a/tls.key")
	b := newEndpoint("/etc/b/tls.crt", "/etc/b/tls.key")

	require.NotEqual(t, none.TLSConfigKey(), a.TLSConfigKey())
	require.NotEqual(t, a.TLSConfigKey(), b.TLSConfigKey())
	require.Equal(t, a.TLSConfigKey(), newEndpoint("/etc/a/tls.crt", "/etc/a/tls.key").TLSConfigKey())

	// The separator inside a path: unquoted, both pairs would hash "/x|/y|/z".
	require.NotEqual(t,
		newEndpoint("/x|/y", "/z").TLSConfigKey(),
		newEndpoint("/x", "/y|/z").TLSConfigKey(),
		"a path containing the separator must not make two pairs share a key")
}

// TestEndpointTLSConfigKey_UnchangedWithoutClientCertificate is the golden guard for upgrades:
// with no client certificate configured, the registry key and the DSN are byte-for-byte what
// they were before client certificates existed.
func TestEndpointTLSConfigKey_UnchangedWithoutClientCertificate(t *testing.T) {
	// No knobs: the legacy key.
	legacy := NewEndpointCredentials("https", "host", "u", "p", "", 8443)
	require.Equal(t, tlsSettingsLegacy, legacy.TLSConfigKey())
	require.Equal(t, "https://u:p@host:8443?tls_config=tls-settings", legacy.GetDSN())

	// With knobs: the content hash over exactly the four pre-existing fields.
	c := NewEndpointCredentials("https", "host", "u", "p", "CA", 8443)
	c.SetTLSSecurity(api.TLSVerifyStrict, api.TLSMinVersion13, "sni.example.org")
	h := sha256.Sum256([]byte(string(api.TLSVerifyStrict) + "|" + string(api.TLSMinVersion13) + "|sni.example.org|CA"))
	want := "tls-settings-" + hex.EncodeToString(h[:8])
	require.Equal(t, want, c.TLSConfigKey())
	require.Equal(t, "https://u:p@host:8443?tls_config="+want, c.GetDSN())
	require.Equal(t, "https://u:***@host:8443?tls_config="+want, c.GetDSNWithHiddenCredentials())
	require.Equal(t, "https://u:p@host:8443?log_queries=1&tls_config="+want, c.GetDSNLogQueries())

	// Setting empty paths is the same as never setting them.
	c.SetClientCertificate("", "")
	require.Equal(t, want, c.TLSConfigKey())
}

// TestClusterConnectionParams_ClientCertificate verifies that the paths flow from chopconf to every
// endpoint and that a per-cluster TLS overlay leaves them alone (they are operator-scoped).
func TestClusterConnectionParams_ClientCertificate(t *testing.T) {
	config := &api.OperatorConfig{}
	config.ClickHouse.Access.Scheme = api.ChSchemeHTTPS
	config.ClickHouse.Access.Port = 8443
	config.ClickHouse.Access.ClientCertFile = "/etc/client/tls.crt"
	config.ClickHouse.Access.ClientKeyFile = "/etc/client/tls.key"

	params := NewClusterConnectionParamsFromCHOpConfig(config)
	require.Equal(t, "/etc/client/tls.crt", params.ClientCertFile)
	require.Equal(t, "/etc/client/tls.key", params.ClientKeyFile)

	params.OverlayTLS(api.TLSVerifyStrict, api.TLSMinVersion13, "sni.example.org", "CA")
	require.Equal(t, "/etc/client/tls.crt", params.ClientCertFile, "the overlay must not touch the client certificate")
	require.Equal(t, "/etc/client/tls.key", params.ClientKeyFile)

	endpoint := params.NewEndpointConnectionParams("host")
	require.Equal(t, "/etc/client/tls.crt", endpoint.ClientCertFile())
	require.Equal(t, "/etc/client/tls.key", endpoint.ClientKeyFile())

	// Unset in chopconf: endpoints carry nothing and keep the legacy key.
	plain := NewClusterConnectionParamsFromCHOpConfig(&api.OperatorConfig{}).NewEndpointConnectionParams("host")
	require.Empty(t, plain.ClientCertFile())
	require.Empty(t, plain.ClientKeyFile())
}
