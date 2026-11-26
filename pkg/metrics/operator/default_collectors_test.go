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

package operator

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

var probeSeq atomic.Int64

// probeCollector stands in for the operator's OTel exporter: one self-described
// series that must survive whichever registry serves the scrape.
type probeCollector struct {
	name string
	desc *prom.Desc
}

func newProbeCollector() *probeCollector {
	name := fmt.Sprintf("clickhouse_operator_collectors_probe_%d", probeSeq.Add(1))
	return &probeCollector{
		name: name,
		desc: prom.NewDesc(name, "probe metric", nil, nil),
	}
}

func (c *probeCollector) Describe(ch chan<- *prom.Desc) {
	ch <- c.desc
}

func (c *probeCollector) Collect(ch chan<- prom.Metric) {
	ch <- prom.MustNewConstMetric(c.desc, prom.GaugeValue, 1)
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

// TestMetricsCollectorsToggle is the regression guard for issue #2098: the
// operator endpoint must keep exposing go_*/process_* by default, and must drop
// them - without dropping the operator's own series - when the opt-out is set.
func TestMetricsCollectorsToggle(t *testing.T) {
	t.Run("default keeps runtime and process collectors", func(t *testing.T) {
		names := scrapeFamilyNames(t, newMetricsHandler(newMetricsRegistry(false).gatherer))

		require.True(t, names["go_goroutines"],
			"go_* is missing by default - the default registry is no longer served")
		require.True(t, names["process_start_time_seconds"],
			"process_* is missing by default - the default registry is no longer served")
	})

	t.Run("opt-out drops them and keeps operator metrics", func(t *testing.T) {
		probe := newProbeCollector()
		registry := newMetricsRegistry(true)
		registry.registerer.MustRegister(probe)

		names := scrapeFamilyNames(t, newMetricsHandler(registry.gatherer))

		requireNoFamilyPrefix(t, names, "go_", "process_", "promhttp_")
		require.True(t, names[probe.name],
			"the operator's own metric is missing - the opt-out must only drop default collectors")
	})
}
