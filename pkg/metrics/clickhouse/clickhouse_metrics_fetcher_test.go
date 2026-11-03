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
)

// TestAppendMetricRowFiltersExcluded verifies the fetch-side memory pre-filter:
// excluded metric names are dropped during row scan so they never enter the
// in-memory Table (the OOM-reduction goal of the fetch-side filter). stubMetricsFilter
// is shared with prometheus_writer_test.go.
func TestAppendMetricRowFiltersExcluded(t *testing.T) {
	fetcher := NewMetricsFetcher(
		nil,
		"",
		&stubMetricsFilter{excluded: map[string]bool{
			"metric.OSUserTimeCPU0": true,
			"metric.OSUserTimeCPU1": true,
		}},
	)

	var data Table
	rows := [][]string{
		{"metric.OSUserTimeCPU0", "1", "", "gauge", ""}, // excluded
		{"metric.Query", "2", "", "gauge", ""},          // kept
		{"metric.OSUserTimeCPU1", "3", "", "gauge", ""}, // excluded
		{"metric.TCPConnection", "4", "", "gauge", ""},  // kept
	}
	for _, r := range rows {
		fetcher.appendMetricRow(&data, r[0], r[1], r[2], r[3], r[4])
	}

	want := Table{
		{"metric.Query", "2", "", "gauge", ""},
		{"metric.TCPConnection", "4", "", "gauge", ""},
	}
	if len(data) != len(want) {
		t.Fatalf("expected %d rows after filtering, got %d: %v", len(want), len(data), data)
	}
	for i := range want {
		for j := range want[i] {
			if data[i][j] != want[i][j] {
				t.Fatalf("row %d col %d: expected %q, got %q", i, j, want[i][j], data[i][j])
			}
		}
	}
}

// TestAppendMetricRowNilFilterKeepsAll verifies a nil filter is a no-op (keeps every
// row), guarding the nil-safe IsExcluded contract on the fetch path.
func TestAppendMetricRowNilFilterKeepsAll(t *testing.T) {
	fetcher := NewMetricsFetcher(nil, "", nil)

	var data Table
	fetcher.appendMetricRow(&data, "metric.OSUserTimeCPU0", "1", "", "gauge", "")
	fetcher.appendMetricRow(&data, "metric.Query", "2", "", "gauge", "")

	if len(data) != 2 {
		t.Fatalf("nil filter should keep all rows, got %d: %v", len(data), data)
	}
}

// TestBuildMetricsSQLAsyncMetricsSource verifies that the key_values column is queried only
// when the server has it, since older ClickHouse versions fail on an unknown column.
func TestBuildMetricsSQLAsyncMetricsSource(t *testing.T) {
	fetcher := NewMetricsFetcher(nil, "", nil)

	legacy := fetcher.buildMetricsSQL(false)
	if strings.Contains(legacy, "key_values") {
		t.Fatalf("query for a server without key_values must not reference it:\n%s", legacy)
	}
	keyValues := fetcher.buildMetricsSQL(true)
	for _, want := range []string{"WHERE empty(key_values)", "ARRAY JOIN key_values AS kv"} {
		if !strings.Contains(keyValues, want) {
			t.Fatalf("query for a server with key_values must contain %q:\n%s", want, keyValues)
		}
	}
}

// TestAppendMetricRowKeepsKey verifies the key of a key-value metric reaches the buffer.
func TestAppendMetricRowKeepsKey(t *testing.T) {
	fetcher := NewMetricsFetcher(nil, "", nil)

	var data Table
	fetcher.appendMetricRow(&data, "metric.OSUserTimeCPU", "0.5", "", "gauge", "3")

	if len(data) != 1 || data[0][4] != "3" {
		t.Fatalf("expected one row with key 3, got %v", data)
	}
}

// TestAppendMetricRowFiltersExcludedKey verifies that exclusions written for the pre-26.8 names
// of key-value metrics (OSUserTimeCPU3, CPUFrequencyMHz_0) also drop the keyed series.
func TestAppendMetricRowFiltersExcludedKey(t *testing.T) {
	fetcher := NewMetricsFetcher(
		nil,
		"",
		&stubMetricsFilter{excluded: map[string]bool{
			"metric.OSUserTimeCPU3":    true,
			"metric.CPUFrequencyMHz_0": true,
		}},
	)

	var data Table
	fetcher.appendMetricRow(&data, "metric.OSUserTimeCPU", "1", "", "gauge", "3")   // excluded
	fetcher.appendMetricRow(&data, "metric.OSUserTimeCPU", "2", "", "gauge", "4")   // kept
	fetcher.appendMetricRow(&data, "metric.CPUFrequencyMHz", "3", "", "gauge", "0") // excluded
	fetcher.appendMetricRow(&data, "metric.DiskTotal", "4", "", "gauge", "default") // kept

	if len(data) != 2 || data[0][4] != "4" || data[1][4] != "default" {
		t.Fatalf("expected the OSUserTimeCPU 4 and DiskTotal default rows, got %v", data)
	}
}
