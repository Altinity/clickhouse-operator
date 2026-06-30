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

package pprof

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewMuxServesMemoryAndGoroutineProfilesOnly(t *testing.T) {
	mux := NewMux()

	for _, path := range []string{
		"/debug/pprof/",
		"/debug/pprof/heap",
		"/debug/pprof/allocs",
		"/debug/pprof/goroutine?debug=1",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()

		mux.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code, path)
	}

	for _, path := range []string{
		"/debug/pprof/profile",
		"/debug/pprof/trace",
		"/debug/pprof/cmdline",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()

		mux.ServeHTTP(rec, req)

		require.Equal(t, http.StatusNotFound, rec.Code, path)
	}
}

func TestValidateLoopbackAddress(t *testing.T) {
	for _, addr := range []string{
		"127.0.0.1:6060",
		"localhost:6060",
		"[::1]:6060",
	} {
		require.NoError(t, validateLoopbackAddress(addr), addr)
	}

	for _, addr := range []string{
		":6060",
		"0.0.0.0:6060",
		"192.0.2.1:6060",
		"bad",
	} {
		require.Error(t, validateLoopbackAddress(addr), addr)
	}
}
