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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"testing"
	"time"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/stretchr/testify/require"
)

// generateSelfSignedPEM produces a single self-signed cert as a PEM string.
// Used by tests below to feed parseRootCAs realistic input rather than the
// fragile hardcoded cert from test_058 (which expires).
func generateSelfSignedPEM(t *testing.T) string {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "unit-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageCertSign,
		IsCA:         true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	require.NoError(t, err)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return string(pemBytes)
}

// TestParseRootCAs_PEM verifies the happy path: plain PEM input.
func TestParseRootCAs_PEM(t *testing.T) {
	pemStr := generateSelfSignedPEM(t)
	pool, err := parseRootCAs(pemStr, log.New())
	require.NoError(t, err)
	require.NotNil(t, pool)
}

// TestParseRootCAs_Base64WrappedPEM verifies the base64-wrapped-PEM path:
// a caller wraps the PEM in base64 (a common envelope when stuffing certs
// into env vars or k8s secrets). Decode order is base64 → PEM → DER.
func TestParseRootCAs_Base64WrappedPEM(t *testing.T) {
	pemStr := generateSelfSignedPEM(t)
	wrapped := base64.StdEncoding.EncodeToString([]byte(pemStr))
	pool, err := parseRootCAs(wrapped, log.New())
	require.NoError(t, err)
	require.NotNil(t, pool)
}

// TestParseRootCAs_RawDER feeds DER bytes directly (no PEM wrapper). The
// decode path: base64 fails → original string treated as DER ASCII → PEM
// fails → ParseCertificate tries DER. With raw binary DER this works.
func TestParseRootCAs_RawDER(t *testing.T) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "raw-der-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageCertSign,
		IsCA:         true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	require.NoError(t, err)

	pool, err := parseRootCAs(string(der), log.New())
	require.NoError(t, err)
	require.NotNil(t, pool)
}

// TestParseRootCAs_Garbage verifies an unparseable input surfaces a clear
// error rather than panicking or returning nil silently. Required so the
// caller can refuse to register when verify=Strict; previously parseRootCAs
// returned nil and the caller silently fell back to the system trust store.
func TestParseRootCAs_Garbage(t *testing.T) {
	cases := []string{
		"not-a-cert",
		"-----BEGIN CERTIFICATE-----\ngarbage\n-----END CERTIFICATE-----",
		base64.StdEncoding.EncodeToString([]byte("still not a cert")),
		"",
	}
	for _, in := range cases {
		t.Run(in, func(t *testing.T) {
			pool, err := parseRootCAs(in, log.New())
			require.Error(t, err)
			require.Nil(t, pool)
			require.Contains(t, err.Error(), "rootCA parse failed")
		})
	}
}

// TestParseRootCAs_Base64NoPEM_KeepsDecodedBytes is a regression guard: an
// earlier version of parseRootCAs had a bug where the else-branch of
// pem.Decode reassigned certBytes back to the raw string, discarding the
// already-decoded base64 bytes. With the fix in place we should successfully
// parse base64(DER) — base64 → []byte (DER) → pem fails → ParseCertificate
// works on the DER bytes.
func TestParseRootCAs_Base64NoPEM_KeepsDecodedBytes(t *testing.T) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "base64-der"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageCertSign,
		IsCA:         true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	require.NoError(t, err)

	b64 := base64.StdEncoding.EncodeToString(der)
	pool, err := parseRootCAs(b64, log.New())
	require.NoError(t, err, "base64-wrapped DER must parse — regression guard")
	require.NotNil(t, pool)
}

// TestEndpointTLSConfigKey_LegacyOnNoKnobs verifies that an endpoint with
// no security knobs set still uses the legacy registry key. This preserves
// the legacy DSN format for back-compat scrapers and matches the comment
// at credentials_endpoint.go.
func TestEndpointTLSConfigKey_LegacyOnNoKnobs(t *testing.T) {
	c := NewEndpointCredentials("https", "host", "u", "p", "", 8443)
	require.Equal(t, tlsSettingsLegacy, c.TLSConfigKey())
	// DSN must contain the legacy key.
	require.Contains(t, c.GetDSN(), "tls_config="+tlsSettingsLegacy)
}

// TestEndpointTLSConfigKey_DistinctOnDifferentKnobs verifies that two
// endpoints with different security knobs receive different registry keys —
// this is the per-cluster TLS race fix from pass #8.
func TestEndpointTLSConfigKey_DistinctOnDifferentKnobs(t *testing.T) {
	a := NewEndpointCredentials("https", "host", "u", "p", "", 8443)
	a.SetTLSSecurity(api.TLSVerifyStrict, api.TLSMinVersion13, "")
	b := NewEndpointCredentials("https", "host", "u", "p", "", 8443)
	b.SetTLSSecurity(api.TLSVerifyNone, api.TLSMinVersion12, "")

	require.NotEqual(t, a.TLSConfigKey(), b.TLSConfigKey(),
		"different knobs MUST produce different registry keys (per-cluster TLS race fix)")
	require.NotEqual(t, tlsSettingsLegacy, a.TLSConfigKey(), "non-empty knobs must not use legacy key")
	require.NotEqual(t, tlsSettingsLegacy, b.TLSConfigKey(), "non-empty knobs must not use legacy key")
}

// TestEndpointTLSConfigKey_StableOnIdenticalKnobs verifies that two endpoints
// with the SAME security knobs share a registry key — necessary so the
// go-clickhouse driver's RegisterTLSConfig isn't churning per-call.
func TestEndpointTLSConfigKey_StableOnIdenticalKnobs(t *testing.T) {
	pemStr := generateSelfSignedPEM(t)
	a := NewEndpointCredentials("https", "hostA", "u", "p", pemStr, 8443)
	a.SetTLSSecurity(api.TLSVerifyStrict, api.TLSMinVersion13, "sni.example")
	b := NewEndpointCredentials("https", "hostB", "u2", "p2", pemStr, 8443)
	b.SetTLSSecurity(api.TLSVerifyStrict, api.TLSMinVersion13, "sni.example")

	require.Equal(t, a.TLSConfigKey(), b.TLSConfigKey(),
		"identical knobs MUST share a registry key (avoids redundant RegisterTLSConfig)")
}

// TestResolveInsecureSkipVerify locks in the back-compat polarity of the
// InsecureSkipVerify resolver. Rows order mirrors the documented semantics in
// setupTLSAdvanced — including the load-bearing "rootCA-only stays insecure"
// rule that preserves pre-0.27.1 behavior for CHIs supplying a CA payload to
// the auth path without expecting hostname/chain verification.
//
// Note: rootCA does NOT appear as a parameter because the resolver does not
// read it; the early-return in setupTLSAdvanced ensures this function is only
// called when AT LEAST ONE of {verify, minVersion, serverName, rootCA} is set.
// The "verify=Strict + rootCA empty" cell is a deliberate caller responsibility.
func TestResolveInsecureSkipVerify(t *testing.T) {
	cases := []struct {
		name       string
		verify     api.TLSVerify
		minVersion api.TLSMinVersion
		serverName string
		expect     bool // expected InsecureSkipVerify
	}{
		// Legacy back-compat: rootCA-only (verify="" everywhere else empty).
		// Caller invokes us with all-empty when rootCA is set alone, and we
		// MUST return true to preserve pre-0.27.1 behavior.
		{"rootCA-only legacy: all empty stays insecure", "", "", "", true},

		// Explicit Strict — always secure.
		{"Strict alone", api.TLSVerifyStrict, "", "", false},
		{"Strict + minVersion", api.TLSVerifyStrict, api.TLSMinVersion13, "", false},
		{"Strict + serverName", api.TLSVerifyStrict, "", "sni.example", false},
		{"Strict + minVersion + serverName", api.TLSVerifyStrict, api.TLSMinVersion13, "sni.example", false},

		// Explicit None — always insecure.
		{"None alone", api.TLSVerifyNone, "", "", true},
		{"None + minVersion (None wins)", api.TLSVerifyNone, api.TLSMinVersion13, "", true},
		{"None + serverName (None wins)", api.TLSVerifyNone, "", "sni.example", true},
		{"None + minVersion + serverName (None wins)", api.TLSVerifyNone, api.TLSMinVersion13, "sni.example", true},

		// Empty verify + other hardening knobs — opt-in to secure.
		{"empty verify + minVersion (opt-in secure)", "", api.TLSMinVersion13, "", false},
		{"empty verify + serverName (opt-in secure)", "", "", "sni.example", false},
		{"empty verify + minVersion + serverName (opt-in secure)", "", api.TLSMinVersion13, "sni.example", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := resolveInsecureSkipVerify(c.verify, c.minVersion, c.serverName)
			require.Equal(t, c.expect, got)
		})
	}
}

// tlsTestServer is a self-contained TLS server with a known CA and a known
// server cert. Returned `addr` is the host:port the test should dial; the
// server cert has DNS SAN "test-server.local" and IP SAN "127.0.0.1" only —
// SO hostname-mismatch tests use a name that matches neither.
type tlsTestServer struct {
	addr     string
	listener net.Listener
	caPool   *x509.CertPool
}

func (s *tlsTestServer) Close() { _ = s.listener.Close() }

// newTLSTestServer generates a self-signed CA and a server certificate signed
// by that CA, then starts a TLS listener that handshakes-and-disconnects on
// every connection. The handshake itself is what the tests assert against.
func newTLSTestServer(t *testing.T) *tlsTestServer {
	t.Helper()

	// CA
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)
	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)

	// Leaf
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test-server.local"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"test-server.local"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	require.NoError(t, err)

	serverCert := tls.Certificate{
		Certificate: [][]byte{leafDER},
		PrivateKey:  leafKey,
	}

	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		MinVersion:   tls.VersionTLS12,
	})
	require.NoError(t, err)

	// Accept-loop: handshake then close. Just enough for the client to
	// observe handshake success/failure.
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				if tlsConn, ok := c.(*tls.Conn); ok {
					_ = tlsConn.Handshake()
				}
				_ = c.Close()
			}(conn)
		}
	}()

	return &tlsTestServer{
		addr:     listener.Addr().String(),
		listener: listener,
		caPool:   caPool,
	}
}

// dialTLS opens a TCP+TLS connection to the server's listener address using
// the supplied tls.Config (a fresh clone is taken to avoid the test mutating
// shared state). Returns the handshake error or nil on success.
func dialTLS(t *testing.T, addr string, cfg *tls.Config) error {
	t.Helper()
	conn, err := tls.Dial("tcp", addr, cfg.Clone())
	if conn != nil {
		_ = conn.Close()
	}
	return err
}

// TestTLSHandshake walks the four spec-mandated outcomes against a
// self-managed TLS server with a known CA + leaf cert. The operator's FIPS
// scope specification (§2 line 38) requires this exact coverage. Tests build *tls.Config directly
// (mirroring what setupTLSAdvanced would produce) rather than going through
// go-clickhouse — the scope is whether our config-builder makes the right
// FIPS-compatible decisions.
func TestTLSHandshake(t *testing.T) {
	s := newTLSTestServer(t)
	defer s.Close()

	// Trust pool with an UNRELATED self-signed cert, used for "invalid CA".
	unrelatedPool := x509.NewCertPool()
	unrelatedPool.AppendCertsFromPEM([]byte(generateSelfSignedPEM(t)))

	// errCheck returns a non-empty failure message if err does not match the
	// expected typed-error shape. Returning string-vs-bool lets each row name
	// the expected x509 error type without coupling the assertion to Go's
	// (release-to-release-mutable) error message wording.
	cases := []struct {
		name     string
		cfg      *tls.Config
		wantErr  bool
		errCheck func(err error) string
	}{
		{
			name: "valid CA + matching DNS SAN → handshake succeeds",
			cfg: &tls.Config{
				RootCAs:    s.caPool,
				ServerName: "test-server.local",
				MinVersion: tls.VersionTLS12,
			},
			wantErr: false,
		},
		{
			name: "valid CA + matching IP SAN → handshake succeeds",
			cfg: &tls.Config{
				RootCAs:    s.caPool,
				ServerName: "127.0.0.1",
				MinVersion: tls.VersionTLS12,
			},
			wantErr: false,
		},
		{
			name: "invalid CA → handshake fails with unknown-authority error",
			cfg: &tls.Config{
				RootCAs:    unrelatedPool,
				ServerName: "test-server.local",
				MinVersion: tls.VersionTLS12,
			},
			wantErr: true,
			errCheck: func(err error) string {
				var unkAuth x509.UnknownAuthorityError
				if !errors.As(err, &unkAuth) {
					return "expected x509.UnknownAuthorityError"
				}
				return ""
			},
		},
		{
			name: "hostname mismatch → handshake fails with name-not-valid error",
			cfg: &tls.Config{
				RootCAs:    s.caPool,
				ServerName: "wrong.example.com",
				MinVersion: tls.VersionTLS12,
			},
			wantErr: true,
			errCheck: func(err error) string {
				var hostErr x509.HostnameError
				if !errors.As(err, &hostErr) {
					return "expected x509.HostnameError"
				}
				return ""
			},
		},
		{
			name: "explicit InsecureSkipVerify → handshake succeeds against unknown CA",
			cfg: &tls.Config{
				// No RootCAs → real verification would fail.
				InsecureSkipVerify: true,
				MinVersion:         tls.VersionTLS12,
			},
			wantErr: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := dialTLS(t, s.addr, c.cfg)
			if c.wantErr {
				require.Error(t, err, "expected handshake to fail")
				if c.errCheck != nil {
					if msg := c.errCheck(err); msg != "" {
						t.Fatalf("%s; got: %v", msg, err)
					}
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestEnforceVerifiedLegacyTLS covers the FIPS-mode override of the legacy
// tlsSettingsLegacy registration that setupTLSBasic() installs at init-time.
// Pre-FIPS deployments retain InsecureSkipVerify=true via setupTLSBasic;
// fipsGate() calls EnforceVerifiedLegacyTLS() to re-register the same key
// with InsecureSkipVerify=false so legacy DSNs (no explicit security knobs)
// pick up verified TLS instead. Per the operator's FIPS scope specification
// (§2 line 30 fix).
//
// Vendor go-clickhouse driver stores registrations in a global map (verified
// via vendor source); a second RegisterTLSConfig with the same key REPLACES
// the first. We exercise the polarity by building each config-shape and
// attempting handshakes against an unknown-CA TLS server.
func TestEnforceVerifiedLegacyTLS(t *testing.T) {
	s := newTLSTestServer(t)
	defer s.Close()

	t.Run("setupTLSBasic preserves legacy InsecureSkipVerify=true", func(t *testing.T) {
		// Equivalent to what setupTLSBasic registers globally.
		cfg := &tls.Config{InsecureSkipVerify: true}
		err := dialTLS(t, s.addr, cfg)
		require.NoError(t, err, "legacy config must accept ANY cert (back-compat for non-FIPS deployments)")
	})

	t.Run("EnforceVerifiedLegacyTLS overrides to verifying", func(t *testing.T) {
		// Equivalent to what EnforceVerifiedLegacyTLS re-registers.
		cfg := &tls.Config{InsecureSkipVerify: false}
		err := dialTLS(t, s.addr, cfg)
		require.Error(t, err, "verifying config must reject unknown CA")
		// Type-based assertion: Go's x509 error wording changes between
		// releases (e.g. Go 1.26 reshaped the unknown-authority message),
		// but the error TYPE is part of the stdlib API contract.
		var unkAuth x509.UnknownAuthorityError
		if !errors.As(err, &unkAuth) {
			t.Fatalf("expected x509.UnknownAuthorityError, got: %v", err)
		}
	})

	// Pin the explicit MinVersion floor per FIPS spec §2 L37 ("TLS minimum
	// version is explicit, at least TLS 1.2"). Future refactors that drop the
	// explicit version must fail this assertion before reaching production.
	t.Run("legacyVerifiedTLSConfig pins MinVersion to TLS 1.2", func(t *testing.T) {
		cfg := legacyVerifiedTLSConfig()
		require.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion,
			"legacy verified TLS config must set MinVersion=TLS12 explicitly")
		require.False(t, cfg.InsecureSkipVerify,
			"legacy verified TLS config must set InsecureSkipVerify=false")
	})
}
