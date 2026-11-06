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

package profiling

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testEnv = "CHOP_TEST_PPROF_ADDRESS"

func TestResolveAddress(t *testing.T) {
	t.Run("disabled by default", func(t *testing.T) {
		t.Setenv(testEnv, "")
		require.Equal(t, "", ResolveAddress("", testEnv))
	})
	t.Run("env enables", func(t *testing.T) {
		t.Setenv(testEnv, "127.0.0.1:6060")
		require.Equal(t, "127.0.0.1:6060", ResolveAddress("", testEnv))
	})
	t.Run("flag overrides env", func(t *testing.T) {
		t.Setenv(testEnv, "127.0.0.1:6060")
		require.Equal(t, "127.0.0.1:7070", ResolveAddress("127.0.0.1:7070", testEnv))
	})
}

func TestIsLoopbackAddress(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1:6060": true,
		"[::1]:6060":     true,
		"localhost:6060": true,
		":6060":          false,
		"0.0.0.0:6060":   false,
		"10.1.2.3:6060":  false,
		"garbage":        false,
	} {
		require.Equal(t, want, IsLoopbackAddress(addr), addr)
	}
}

func TestNewMuxServesPprof(t *testing.T) {
	server := httptest.NewServer(NewMux())
	defer server.Close()

	for _, path := range []string{
		"/debug/pprof/",
		"/debug/pprof/cmdline",
		"/debug/pprof/symbol",
		"/debug/pprof/heap",
		"/debug/pprof/goroutine?debug=1",
	} {
		resp, err := http.Get(server.URL + path)
		require.NoError(t, err, path)
		_ = resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, path)
	}
}

func TestStartEmptyAddressIsNoop(t *testing.T) {
	// Must return without binding anything or panicking
	Start(context.Background(), "", "test")
}

func TestStartServesUntilContextDone(t *testing.T) {
	// Grab a free loopback port
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := l.Addr().String()
	require.NoError(t, l.Close())

	ctx, cancel := context.WithCancel(context.Background())
	Start(ctx, address, "test")

	url := "http://" + address + "/debug/pprof/"
	require.Eventually(t, func() bool {
		resp, err := http.Get(url)
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 50*time.Millisecond)

	cancel()
	require.Eventually(t, func() bool {
		_, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		return err != nil
	}, 5*time.Second, 50*time.Millisecond)
}
