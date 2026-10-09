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

package metrics

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestAllCountersCarryTotalSuffix is the promtool-side half of GH issue #2093.
//
// promtool check metrics flags any Prometheus counter that is exposed without the
// conventional _total suffix. The operator ships its exporter with
// prometheus.WithoutCounterSuffixes(), so the exporter emits metric names
// verbatim — nothing appends _total for us. Every OTel Int64Counter registered by
// createMetrics therefore has to carry _total in its own name, or promtool flags it
// the moment the series first appears on /metrics (which is why un-initialized
// series such as pod_update/pod_delete escaped the original report — they were
// absent from the scrape, not compliant).
//
// This test registers the exact same instruments createMetrics registers, records
// one measurement per monotonic counter (so the delta temporality aggregation
// emits a stream for every instrument), collects them, and fails if any counter
// name does not end with _total.
func TestAllCountersCarryTotalSuffix(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	meter := provider.Meter("counter-suffix-test")

	m := createMetrics(meter)

	// One measurement per monotonic counter: without a recorded stream the
	// cumulative/delta aggregation holds no series and Collect returns nothing.
	recordZeroOnAllCounters(context.Background(), m)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	found := map[string]aggregation{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			_, dup := found[m.Name]
			require.False(t, dup, "metric %q registered twice", m.Name)
			found[m.Name] = aggregationOf(m)
		}
	}

	require.NotEmpty(t, found, "createMetrics registered no instruments")

	counters := 0
	for name, kind := range found {
		if kind != aggregationCounter {
			continue
		}
		counters++
		require.True(t, strings.HasSuffix(name, "_total"),
			"counter %q must carry the _total suffix (promtool check metrics)", name)
	}
	require.Positive(t, counters, "no counters were registered to check")
}

// The remaining assertions pin the two non-counter shapes so a future
// "just append _total everywhere" edit fails loudly instead of shipping
// mistyped histogram/gauge names.
func TestNonCounterShapesHaveNoTotalSuffix(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	meter := provider.Meter("shape-test")

	m := createMetrics(meter)
	recordZeroOnAllCounters(context.Background(), m)
	m.CHIReconcilesTimings.Record(context.Background(), 0.001)
	m.HostReconcilesTimings.Record(context.Background(), 0.001)
	m.CHI.Add(context.Background(), 0)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	seen := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			seen[m.Name] = true
			switch aggregationOf(m) {
			case aggregationCounter:
				// covered by TestAllCountersCarryTotalSuffix
			default:
				require.False(t, strings.HasSuffix(m.Name, "_total"),
					"non-counter %q must not carry _total", m.Name)
			}
		}
	}
	require.True(t, seen["clickhouse_operator_chi_reconciles_timings"],
		"histogram series vanished from the collection")
	require.True(t, seen["clickhouse_operator_chi"],
		"UpDownCounter series vanished from the collection")
}

// OTel exposes no instrument-kind helper on collected data; derive it from the
// aggregation the SDK recorded, which is exactly what the exporter maps onto
// the Prometheus TYPE line.
type aggregation int

const (
	aggregationOther aggregation = iota
	aggregationCounter
)

func aggregationOf(m metricdata.Metrics) aggregation {
	if sum, ok := m.Data.(metricdata.Sum[int64]); ok && sum.IsMonotonic {
		return aggregationCounter
	}
	return aggregationOther
}

// recordZeroOnAllCounters records one zero measurement on every monotonic
// counter createMetrics registers. ManualReader keeps a stream only for
// instruments with at least one recorded measurement, so without this the
// collection is empty and the test asserts on nothing. This mirrors what
// chiInitZeroValues does in production (Add(ctx, 0, ...)), without touching
// the production label-path globals.
func recordZeroOnAllCounters(ctx context.Context, m *Metrics) {
	m.CHIReconcilesStarted.Add(ctx, 0)
	m.CHIReconcilesCompleted.Add(ctx, 0)
	m.CHIReconcilesAborted.Add(ctx, 0)
	m.CHIAutoRecoveriesTriggered.Add(ctx, 0)
	m.CHIKeeperUpdatesSkipped.Add(ctx, 0)
	m.HostReconcilesStarted.Add(ctx, 0)
	m.HostReconcilesCompleted.Add(ctx, 0)
	m.HostReconcilesRestarts.Add(ctx, 0)
	m.HostReconcilesErrors.Add(ctx, 0)
	m.PodAddEvents.Add(ctx, 0)
	m.PodUpdateEvents.Add(ctx, 0)
	m.PodDeleteEvents.Add(ctx, 0)
}
