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
	"time"
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
	fetcher.appendMetricRow(&data, "metric.SomeFamilyAddedLater", "0.5", "", "gauge", "3")

	if len(data) != 1 || data[0][4] != "3" {
		t.Fatalf("expected one row with key 3, got %v", data)
	}
}

// TestMetricsRowsUseLegacyNames verifies the elements of key-value metrics come out under the
// names they had before ClickHouse 26.8, and only families without such a name keep the key.
func TestMetricsRowsUseLegacyNames(t *testing.T) {
	rows := newMetricsRows(NewMetricsFetcher(nil, "", nil))
	rows.add("metric.DiskTotal", "1", "", "gauge", "default")
	rows.add("metric.EDACCorrectable", "2", "", "gauge", "0")
	rows.add("metric.SomeFamilyAddedLater", "3", "", "gauge", "x")
	rows.add("metric.Query", "4", "", "gauge", "")

	want := Table{
		{"metric.SomeFamilyAddedLater", "3", "", "gauge", "x"},
		{"metric.Query", "4", "", "gauge", ""},
		{"metric.DiskTotal_default", "1", "", "gauge", ""},
		{"metric.EDAC0_Correctable", "2", "", "gauge", ""},
	}
	assertTable(t, rows.table(), want)
}

// TestMetricsRowsFilterLegacyNames verifies that exclusions written for the pre 26.8 names,
// including those of the families that do not just append the key, drop the elements.
func TestMetricsRowsFilterLegacyNames(t *testing.T) {
	rows := newMetricsRows(NewMetricsFetcher(
		nil,
		"",
		&stubMetricsFilter{excluded: map[string]bool{
			"metric.OSUserTimeCPU3":    true,
			"metric.CPUFrequencyMHz_0": true,
			"metric.EDAC0_Correctable": true,
		}},
	))
	rows.add("metric.OSUserTimeCPU", "1", "", "gauge", "3")   // excluded
	rows.add("metric.OSUserTimeCPU", "2", "", "gauge", "4")   // kept
	rows.add("metric.CPUFrequencyMHz", "3", "", "gauge", "0") // excluded
	rows.add("metric.EDACCorrectable", "4", "", "gauge", "0") // excluded

	assertTable(t, rows.table(), Table{{"metric.OSUserTimeCPU4", "2", "", "gauge", ""}})
}

// TestMetricsRowsDedupeBothMode verifies that with asynchronous_metrics_key_values_mode = both,
// where the server publishes the legacy scalar next to the key-value row, the value is exported once.
func TestMetricsRowsDedupeBothMode(t *testing.T) {
	for name, order := range map[string][]string{"scalar first": {"scalar", "keyed"}, "keyed first": {"keyed", "scalar"}} {
		t.Run(name, func(t *testing.T) {
			rows := newMetricsRows(NewMetricsFetcher(nil, "", nil))
			for _, kind := range order {
				if kind == "scalar" {
					rows.add("metric.NetworkReceiveBytes_eth0", "7", "", "gauge", "")
				} else {
					rows.add("metric.NetworkReceiveBytes", "7", "", "gauge", "eth0")
				}
			}
			assertTable(t, rows.table(), Table{{"metric.NetworkReceiveBytes_eth0", "7", "", "gauge", ""}})
		})
	}
}

func assertTable(t *testing.T, got, want Table) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if strings.Join(got[i], "|") != strings.Join(want[i], "|") {
			t.Fatalf("row %d: expected %v, got %v", i, want[i], got[i])
		}
	}
}

// TestAsyncKeyValuesCache verifies a positive probe answer is kept, a negative one expires,
// and forgetting a host forces a new probe.
func TestAsyncKeyValuesCache(t *testing.T) {
	cache := newAsyncKeyValuesCache()
	if _, known := cache.get("h"); known {
		t.Fatal("expected an unknown host")
	}
	cache.set("h", true)
	if supported, known := cache.get("h"); !known || !supported {
		t.Fatal("expected a cached positive answer")
	}
	cache.forget("h")
	if _, known := cache.get("h"); known {
		t.Fatal("expected the host to be forgotten")
	}
	cache.set("h", false)
	if supported, known := cache.get("h"); !known || supported {
		t.Fatal("expected a cached negative answer")
	}
	cache.hosts["h"] = asyncKeyValuesEntry{probedAt: time.Now().Add(-2 * asyncKeyValuesNegativeTTL)}
	if _, known := cache.get("h"); known {
		t.Fatal("expected the negative answer to expire")
	}
}
