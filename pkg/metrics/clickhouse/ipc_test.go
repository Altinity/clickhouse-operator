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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/stretchr/testify/require"
)

func TestIsLoopbackRemote(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"127.0.0.1:12345", true},
		{"[::1]:12345", true},
		{"localhost:80", true},
		{"10.0.0.5:443", false},
		{"192.168.1.10:1234", false},
		{"", false},
		{"not-a-valid-addr", false},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got := isLoopbackRemote(c.in)
			require.Equal(t, c.want, got)
		})
	}
}

// TestIPCAuthMiddlewareIdentityPassthrough verifies the middleware is a
// no-op when expected token is empty (Plain mode — middleware not installed).
// In particular: it must NOT check loopback or token, so a non-loopback
// caller with no header sails through.
func TestIPCAuthMiddlewareIdentityPassthrough(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	wrapped := ipcAuthMiddleware(next, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/chi", nil)
	req.RemoteAddr = "10.0.0.1:1234" // would be rejected if middleware were active
	wrapped.ServeHTTP(rec, req)
	require.True(t, called, "identity middleware must forward to downstream")
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestIPCAuthMiddlewareRejectsNonLoopback(t *testing.T) {
	wrapped := ipcAuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("downstream handler must NOT be called when remote is non-loopback")
	}), "secret-token")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/chi", nil)
	req.RemoteAddr = "10.0.0.5:1234" // non-loopback
	req.Header.Set(api.IPCHeaderToken, "secret-token")
	wrapped.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestIPCAuthMiddlewareRejectsBadToken(t *testing.T) {
	wrapped := ipcAuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("downstream handler must NOT be called on bad token")
	}), "secret-token")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/chi", nil)
	req.RemoteAddr = "127.0.0.1:42" // loopback OK
	req.Header.Set(api.IPCHeaderToken, "wrong-token")
	wrapped.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestIPCAuthMiddlewareRejectsMissingToken(t *testing.T) {
	wrapped := ipcAuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("downstream handler must NOT be called on missing header")
	}), "secret-token")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/chi", nil)
	req.RemoteAddr = "127.0.0.1:42"
	// no X-CHOP-Token header
	wrapped.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestIPCAuthMiddlewareAcceptsLoopbackWithValidToken(t *testing.T) {
	called := false
	wrapped := ipcAuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}), "secret-token")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/chi", nil)
	req.RemoteAddr = "127.0.0.1:42"
	req.Header.Set(api.IPCHeaderToken, "secret-token")
	wrapped.ServeHTTP(rec, req)
	require.True(t, called, "downstream handler must be called when both checks pass")
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestSecureBindAddressRewriting(t *testing.T) {
	plain := resolvedIPC{Mode: api.IPCModePlain, BindHost: "127.0.0.1"}
	secure := resolvedIPC{Mode: api.IPCModeSecure, BindHost: "127.0.0.1"}

	// Plain mode: address passed through unchanged.
	require.Equal(t, ":8888", plain.secureBindAddress(":8888"))
	require.Equal(t, "0.0.0.0:8888", plain.secureBindAddress("0.0.0.0:8888"))

	// Secure mode: host portion replaced with BindHost.
	require.Equal(t, "127.0.0.1:8888", secure.secureBindAddress(":8888"))
	require.Equal(t, "127.0.0.1:8888", secure.secureBindAddress("0.0.0.0:8888"))
}

// TestLoadTokenFromFile verifies happy-path file read with whitespace trim.
func TestLoadTokenFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(path, []byte("abc123\n"), 0o400))

	ipc := resolvedIPC{Mode: api.IPCModeSecure, TokenPath: path}
	tok, err := ipc.loadToken()
	require.NoError(t, err)
	require.Equal(t, "abc123", tok, "trailing whitespace must be trimmed")
}

func TestLoadTokenMissingFile(t *testing.T) {
	ipc := resolvedIPC{Mode: api.IPCModeSecure, TokenPath: "/tmp/does-not-exist-" + t.Name()}
	_, err := ipc.loadToken()
	require.Error(t, err)
	require.Contains(t, err.Error(), "IPC token unreadable")
}

func TestLoadTokenEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty-token")
	require.NoError(t, os.WriteFile(path, []byte("   \n"), 0o400))

	ipc := resolvedIPC{Mode: api.IPCModeSecure, TokenPath: path}
	_, err := ipc.loadToken()
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty")
}
