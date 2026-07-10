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

package chop

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// notATransport is a RoundTripper that is NOT *http.Transport, used to exercise the
// "floor cannot be enforced" path (the case that must fail-closed under hardening).
type notATransport struct{}

func (notATransport) RoundTrip(*http.Request) (*http.Response, error) { return nil, nil }

// TestFloorTransportTLSMinVersion covers the pure floor-stamping helper that backs the
// K8s-API TLS minVersion enforcement (PR #2020). This is the transport-enforcement path
// that the PR's config-resolution tests did not reach.
func TestFloorTransportTLSMinVersion(t *testing.T) {
	t.Run("stamps MinVersion when TLS config is nil", func(t *testing.T) {
		tr := &http.Transport{}
		out, err := floorTransportTLSMinVersion(tr, tls.VersionTLS13)
		require.NoError(t, err)
		require.Same(t, tr, out)
		require.NotNil(t, tr.TLSClientConfig)
		require.Equal(t, uint16(tls.VersionTLS13), tr.TLSClientConfig.MinVersion)
	})

	t.Run("clones existing TLS config instead of mutating it in place", func(t *testing.T) {
		// client-go caches and shares *http.Transport across clientsets; the helper must not
		// mutate the shared *tls.Config in place.
		orig := &tls.Config{ServerName: "api.example"}
		tr := &http.Transport{TLSClientConfig: orig}
		_, err := floorTransportTLSMinVersion(tr, tls.VersionTLS13)
		require.NoError(t, err)
		require.Equal(t, uint16(0), orig.MinVersion, "original shared config must be untouched")
		require.NotSame(t, orig, tr.TLSClientConfig, "transport must hold a clone")
		require.Equal(t, uint16(tls.VersionTLS13), tr.TLSClientConfig.MinVersion)
		require.Equal(t, "api.example", tr.TLSClientConfig.ServerName, "clone preserves other fields")
	})

	t.Run("errors (floor unenforceable) when not *http.Transport", func(t *testing.T) {
		_, err := floorTransportTLSMinVersion(notATransport{}, tls.VersionTLS13)
		require.Error(t, err)
	})
}

// TestFloorTransportTLSMinVersion_EnforcesHandshake proves the floor actually governs the
// TLS handshake: a client floored to 1.3 must refuse a server that caps at 1.2, while a
// client floored to 1.2 connects. This is the end-to-end behavior the operator relies on
// under FIPS/Enforced and which minikube e2e cannot deterministically exercise.
func TestFloorTransportTLSMinVersion_EnforcesHandshake(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	srv.TLS = &tls.Config{MaxVersion: tls.VersionTLS12} // server refuses anything above 1.2
	srv.StartTLS()
	defer srv.Close()

	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	newTransport := func() *http.Transport {
		return &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}
	}

	t.Run("floor 1.3 rejects a TLS-1.2-max server", func(t *testing.T) {
		rt, err := floorTransportTLSMinVersion(newTransport(), tls.VersionTLS13)
		require.NoError(t, err)
		_, err = (&http.Client{Transport: rt}).Get(srv.URL)
		require.Error(t, err, "handshake must fail: server max 1.2 is below client floor 1.3")
	})

	t.Run("floor 1.2 accepts a TLS-1.2 server", func(t *testing.T) {
		rt, err := floorTransportTLSMinVersion(newTransport(), tls.VersionTLS12)
		require.NoError(t, err)
		resp, err := (&http.Client{Transport: rt}).Get(srv.URL)
		require.NoError(t, err)
		_ = resp.Body.Close()
	})
}
