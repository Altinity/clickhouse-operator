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
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/altinity/clickhouse-operator/pkg/apis/metrics"
)

// stubMetricsFilter excludes any metric whose name appears in the set.
// Decouples the writer test from the concrete filter implementation in the
// `filters` sub-package — the interface contract is what matters here.
type stubMetricsFilter struct {
	excluded map[string]bool
}

func (s *stubMetricsFilter) IsExcluded(name string) bool {
	return s.excluded[name]
}

func TestPrometheusWriterSkipsExcludedMetric(t *testing.T) {
	out := make(chan prometheus.Metric, 1)
	writer := &CHIPrometheusWriter{
		out:           out,
		metricsFilter: &stubMetricsFilter{excluded: map[string]bool{"metric.OSUserTimeCPU12": true}},
	}

	writer.writeSingleMetricToPrometheus(
		"metric.OSUserTimeCPU12",
		"",
		prometheus.GaugeValue,
		"1",
		nil,
	)

	if len(out) != 0 {
		t.Fatal("expected excluded metric not to be written")
	}
}

// TestWriteMetricsAddsKeyLabel verifies a key-value metric is written with its key as a label,
// so the per CPU core, block device and network interface series of ClickHouse 26.8+ stay distinct.
func TestWriteMetricsAddsKeyLabel(t *testing.T) {
	out := make(chan prometheus.Metric, 2)
	writer := &CHIPrometheusWriter{out: out, host: &metrics.WatchedHost{}}

	writer.WriteMetrics([][]string{
		{"metric.OSUserTimeCPU", "0.5", "", "gauge", "3"},
		{"metric.Query", "2", "", "gauge", ""},
	})

	if len(out) != 2 {
		t.Fatalf("expected 2 metrics, got %d", len(out))
	}
	withKey, withoutKey := (<-out).Desc().String(), (<-out).Desc().String()
	if !strings.Contains(withKey, ",key,") {
		t.Fatalf("expected the key label on a key-value metric, got %s", withKey)
	}
	if strings.Contains(withoutKey, ",key,") {
		t.Fatalf("expected no key label on a scalar metric, got %s", withoutKey)
	}
}

// TestAppendHostLabelStripsTrailingDot asserts that the trailing dot present on
// WatchedHost.Hostname (intentional FQDN form used on the DNS resolution path)
// does NOT leak into the Prometheus "hostname" label value — trailing dots
// break string-equality matchers in Grafana panels and alert rules.
func TestAppendHostLabelStripsTrailingDot(t *testing.T) {
	cases := []struct {
		in  string
		out string
	}{
		{"chi-x-cluster-0-0.ns.svc.cluster.local.", "chi-x-cluster-0-0.ns.svc.cluster.local"},
		{"chi-x-cluster-0-0.ns.svc.cluster.local", "chi-x-cluster-0-0.ns.svc.cluster.local"},
		{"", ""},
	}
	for _, tc := range cases {
		w := &CHIPrometheusWriter{host: &metrics.WatchedHost{Hostname: tc.in}}
		got := w.appendHostLabel(nil)["hostname"]
		if got != tc.out {
			t.Fatalf("appendHostLabel(%q) hostname = %q, want %q", tc.in, got, tc.out)
		}
		if strings.HasSuffix(got, ".") {
			t.Fatalf("hostname label %q has trailing dot", got)
		}
	}
}
