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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	kube "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/flowcontrol"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
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

// testKubeconfig points at a port nothing listens on: building the clients sends no request.
const testKubeconfig = `apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: https://127.0.0.1:1
contexts:
- name: test
  context:
    cluster: test
current-context: test
`

// newClientset builds the clients from a config file with the given reconcile.runtime lines.
func newClientset(t *testing.T, runtime string) *kube.Clientset {
	t.Helper()
	dir := t.TempDir()
	kubeconfig := filepath.Join(dir, "kubeconfig")
	require.NoError(t, os.WriteFile(kubeconfig, []byte(testKubeconfig), 0o600))
	chopconf := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(chopconf, []byte("reconcile:\n  runtime:\n"+runtime), 0o600))
	kubeClient, _, _, _, _ := GetClientset(kubeconfig, "", chopconf)
	return kubeClient
}

// rateLimiters returns the rate limiters of the core/v1 and apps/v1 clients.
func rateLimiters(t *testing.T, runtime string) (core, apps flowcontrol.RateLimiter) {
	t.Helper()
	kubeClient := newClientset(t, runtime)
	return kubeClient.CoreV1().RESTClient().GetRateLimiter(), kubeClient.AppsV1().RESTClient().GetRateLimiter()
}

// burst counts the requests a fresh limiter lets through at once.
func burst(limiter flowcontrol.RateLimiter) int {
	n := 0
	for n < 1000 && limiter.TryAccept() {
		n++
	}
	return n
}

// The client takes QPS and burst from the config file; each env var, when set, wins over its own value.
func TestGetClientsetRateLimitsFromConfig(t *testing.T) {
	cases := []struct {
		name             string
		envQPS, envBurst string
		qps              float32
		burst            int
	}{
		{"config", "", "", 3, 7},
		{"env vars win", "2", "4", 2, 4},
		{"env burst wins", "", "4", 3, 4},
		{"env qps wins", "2", "", 2, 7},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(deployment.OPERATOR_K8S_CLIENT_QPS_LIMIT, c.envQPS)
			t.Setenv(deployment.OPERATOR_K8S_CLIENT_BURST_LIMIT, c.envBurst)
			core, _ := rateLimiters(t, "    k8sClientQPS: 3\n    k8sClientBurst: 7\n")
			require.Equal(t, c.qps, core.QPS())
			require.Equal(t, c.burst, burst(core))
		})
	}
}

// Unset, 0 or negative keeps the client-go defaults: every API group gets its own 5/10 limiter.
// Setting 5/10 explicitly would not: the clientset would share one limiter across groups.
func TestGetClientsetRateLimitsDefault(t *testing.T) {
	t.Setenv(deployment.OPERATOR_K8S_CLIENT_QPS_LIMIT, "")
	t.Setenv(deployment.OPERATOR_K8S_CLIENT_BURST_LIMIT, "")
	for _, runtime := range []string{
		"",
		"    k8sClientQPS: 0\n    k8sClientBurst: 0\n",
		"    k8sClientQPS: -1\n    k8sClientBurst: -1\n",
	} {
		core, apps := rateLimiters(t, runtime)
		require.NotSame(t, core, apps, runtime)
		require.Equal(t, float32(5), core.QPS(), runtime)
		require.Equal(t, 10, burst(core), runtime)
	}
}

// The warning fires when one CHI may reconcile more shards at once than the client's QPS.
func TestWarnIfK8sClientQPSTooLow(t *testing.T) {
	t.Setenv(deployment.OPERATOR_K8S_CLIENT_BURST_LIMIT, "")
	cases := []struct {
		name          string
		envQPS        string
		runtime       string
		shardsThreads int
		warn          bool
	}{
		{"code defaults", "", "", 1, false},
		{"shipped config.yaml", "", "", 5, false},
		{"shards raised, default limit", "", "", 200, true},
		{"shards raised, low limit", "", "    k8sClientQPS: 50\n    k8sClientBurst: 100\n", 200, true},
		{"shards raised, limit raised", "", "    k8sClientQPS: 200\n    k8sClientBurst: 400\n", 200, false},
		{"env qps 300 over config 3", "300", "    k8sClientQPS: 3\n    k8sClientBurst: 7\n", 200, false},
		{"env qps 2 over config 200", "2", "    k8sClientQPS: 200\n    k8sClientBurst: 400\n", 200, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(deployment.OPERATOR_K8S_CLIENT_QPS_LIMIT, c.envQPS)
			config := &api.OperatorConfig{}
			config.Reconcile.Runtime.ReconcileShardsThreadsNumber = c.shardsThreads
			require.Equal(t, c.warn, WarnIfK8sClientQPSTooLow(newClientset(t, c.runtime), config))
		})
	}
}
