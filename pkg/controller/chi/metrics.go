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

package chi

import (
	"context"

	otelApi "go.opentelemetry.io/otel/metric"

	"github.com/altinity/clickhouse-operator/pkg/metrics"
)

type Metrics struct {
	CHIReconcilesStarted   otelApi.Int64Counter
	CHIReconcilesCompleted otelApi.Int64Counter
	CHIReconcilesTimings   otelApi.Float64Histogram

	HostReconcilesStarted   otelApi.Int64Counter
	HostReconcilesCompleted otelApi.Int64Counter
	HostReconcilesErrors    otelApi.Int64Counter
	HostReconcilesTimings   otelApi.Float64Histogram
}

var m *Metrics

func createMetrics() *Metrics {
	// The unit u should be defined using the appropriate [UCUM](https://ucum.org) case-sensitive code.
	CHIReconcilesStarted, _ := metrics.Meter().Int64Counter(
		"clickhouse_operator_chi_reconciles_started",
		otelApi.WithDescription("number or CHI reconciles started"),
		otelApi.WithUnit("items"),
	)
	CHIReconcilesCompleted, _ := metrics.Meter().Int64Counter(
		"clickhouse_operator_chi_reconciles_completed",
		otelApi.WithDescription("number or CHI reconciles completed"),
		otelApi.WithUnit("items"),
	)
	CHIReconcilesTimings, _ := metrics.Meter().Float64Histogram(
		"clickhouse_operator_chi_reconciles_timings",
		otelApi.WithDescription("timings CHI reconciles completed"),
		otelApi.WithUnit("s"),
	)
	HostReconcilesStarted, _ := metrics.Meter().Int64Counter(
		"clickhouse_operator_host_reconciles_started",
		otelApi.WithDescription("number or host reconciles started"),
		otelApi.WithUnit("items"),
	)
	HostReconcilesCompleted, _ := metrics.Meter().Int64Counter(
		"clickhouse_operator_host_reconciles_completed",
		otelApi.WithDescription("number or host reconciles completed"),
		otelApi.WithUnit("items"),
	)
	HostReconcilesErrors, _ := metrics.Meter().Int64Counter(
		"clickhouse_operator_host_reconciles_errors",
		otelApi.WithDescription("number or host reconciles errors"),
		otelApi.WithUnit("items"),
	)
	HostReconcilesTimings, _ := metrics.Meter().Float64Histogram(
		"clickhouse_operator_host_reconciles_timings",
		otelApi.WithDescription("timings host reconciles completed"),
		otelApi.WithUnit("s"),
	)

	return &Metrics{
		CHIReconcilesStarted:   CHIReconcilesStarted,
		CHIReconcilesCompleted: CHIReconcilesCompleted,
		CHIReconcilesTimings:   CHIReconcilesTimings,

		HostReconcilesStarted:   HostReconcilesStarted,
		HostReconcilesCompleted: HostReconcilesCompleted,
		HostReconcilesErrors:    HostReconcilesErrors,
		HostReconcilesTimings:   HostReconcilesTimings,
	}
}

func ensureMetrics() *Metrics {
	if m == nil {
		m = createMetrics()
	}
	return m
}

func metricsCHIReconcilesStarted(ctx context.Context) {
	ensureMetrics().CHIReconcilesStarted.Add(ctx, 1)
}
func metricsCHIReconcilesCompleted(ctx context.Context) {
	ensureMetrics().CHIReconcilesCompleted.Add(ctx, 1)
}
func metricsCHIReconcilesTimings(ctx context.Context, seconds float64) {
	ensureMetrics().CHIReconcilesTimings.Record(ctx, seconds)
}

func metricsHostReconcilesStarted(ctx context.Context) {
	ensureMetrics().HostReconcilesStarted.Add(ctx, 1)
}
func metricsHostReconcilesCompleted(ctx context.Context) {
	ensureMetrics().HostReconcilesCompleted.Add(ctx, 1)
}
func metricsHostReconcilesErrors(ctx context.Context) {
	ensureMetrics().HostReconcilesErrors.Add(ctx, 1)
}
func metricsHostReconcilesTimings(ctx context.Context, seconds float64) {
	ensureMetrics().HostReconcilesTimings.Record(ctx, seconds)
}
