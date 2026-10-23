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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
)

var probeSeq atomic.Int64

// probeExporter stands in for *Exporter: one self-described series that must
// survive whichever registry serves the scrape, without touching ClickHouse.
type probeExporter struct {
	name string
	desc *prometheus.Desc
}

func newProbeExporter() *probeExporter {
	name := fmt.Sprintf("clickhouse_metrics_exporter_probe_%d", probeSeq.Add(1))
	return &probeExporter{
		name: name,
		desc: prometheus.NewDesc(name, "probe metric", nil, nil),
	}
}

func (e *probeExporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- e.desc
}

func (e *probeExporter) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(e.desc, prometheus.GaugeValue, 1)
}

// scrapeFamilyNames serves one scrape and returns the metric family names in
// the payload, which is what a Prometheus server would ingest.
func scrapeFamilyNames(t *testing.T, handler http.Handler) map[string]bool {
	t.Helper()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	names := map[string]bool{}
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		rest, ok := strings.CutPrefix(line, "# TYPE ")
		if !ok {
			continue
		}
		names[strings.Fields(rest)[0]] = true
	}
	return names
}

func requireNoFamilyPrefix(t *testing.T, names map[string]bool, prefixes ...string) {
	t.Helper()

	for name := range names {
		for _, prefix := range prefixes {
			require.False(t, strings.HasPrefix(name, prefix),
				"%s is exposed with the default collectors disabled", name)
		}
	}
}

// TestMetricsHandlerCollectorsToggle is the exporter-side half of issue #2098.
// The default scrape keeps go_*, process_* and the promhttp_* handler metrics;
// the opt-out leaves the ClickHouse series and nothing else.
func TestMetricsHandlerCollectorsToggle(t *testing.T) {
	t.Run("default keeps runtime, process and promhttp collectors", func(t *testing.T) {
		t.Setenv(deployment.OPERATOR_METRICS_DISABLE_DEFAULT_COLLECTORS, "")

		probe := newProbeExporter()
		names := scrapeFamilyNames(t, newMetricsHandler(probe))

		require.True(t, names["go_goroutines"],
			"go_* is missing by default - the global registry is no longer served")
		require.True(t, names["process_start_time_seconds"],
			"process_* is missing by default - the global registry is no longer served")

		instrumented := false
		for name := range names {
			if strings.HasPrefix(name, "promhttp_") {
				instrumented = true
			}
		}
		require.True(t, instrumented,
			"promhttp_* is missing by default - the handler is no longer instrumented")
		require.True(t, names[probe.name], "the exporter's own metric is missing")
	})

	t.Run("opt-out drops them and keeps exporter metrics", func(t *testing.T) {
		t.Setenv(deployment.OPERATOR_METRICS_DISABLE_DEFAULT_COLLECTORS, "true")

		probe := newProbeExporter()
		names := scrapeFamilyNames(t, newMetricsHandler(probe))

		requireNoFamilyPrefix(t, names, "go_", "process_", "promhttp_")
		require.True(t, names[probe.name],
			"the exporter's own metric is missing - the opt-out must only drop default collectors")
	})
}
