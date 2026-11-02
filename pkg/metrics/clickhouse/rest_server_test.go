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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altinity/clickhouse-operator/pkg/util/profiling"
)

func get(t *testing.T, handler http.Handler, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}

// TestMetricsMuxDoesNotServePprof guards the metrics-exporter metrics/CHI-list port against
// exposing /debug/pprof. net/http/pprof is linked into the binary (pkg/util/profiling) and its
// init() registers on http.DefaultServeMux, so the metrics listener must serve a private mux.
func TestMetricsMuxDoesNotServePprof(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux := newMetricsMux("/metrics", ok, "/chi", ok)

	// Precondition: pprof is linked and DefaultServeMux does serve it, so the check below is meaningful
	require.Equal(t, http.StatusOK, get(t, http.DefaultServeMux, "/debug/pprof/"))

	require.Equal(t, http.StatusOK, get(t, mux, "/metrics"))
	require.Equal(t, http.StatusOK, get(t, mux, "/chi"))
	for _, path := range []string{"/debug/pprof/", "/debug/pprof/heap", "/debug/pprof/profile", "/debug/pprof/cmdline"} {
		require.Equal(t, http.StatusNotFound, get(t, mux, path), path)
	}

	// And the dedicated pprof mux does serve it
	require.Equal(t, http.StatusOK, get(t, profiling.NewMux(), "/debug/pprof/"))
}
