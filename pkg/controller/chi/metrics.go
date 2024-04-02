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

// Metrics is a set of metrics that are tracked by the operator
type Metrics struct {
	// CHIReconcilesStarted is a number (counter) of started CHI reconciles
	CHIReconcilesStarted otelApi.Int64Counter
	// CHIReconcilesCompleted is a number (counter) of completed CHI reconciles.
	// In ideal world number of completed reconciles should be equal to CHIReconcilesStarted
	CHIReconcilesCompleted otelApi.Int64Counter
	// CHIReconcilesAborted is a number (counter) of explicitly aborted CHI reconciles.
	// This counter does not includes reconciles that we not completed due to external rasons, such as operator restart
	CHIReconcilesAborted otelApi.Int64Counter
	// CHIReconcilesTimings is a histogram of durations of successfully completed CHI reconciles
	CHIReconcilesTimings otelApi.Float64Histogram

	// HostReconcilesStarted is a number (counter) of started host reconciles
	HostReconcilesStarted otelApi.Int64Counter
	// HostReconcilesCompleted is a number (counter) of completed host reconciles.
	// In ideal world number of completed reconciles should be equal to HostReconcilesStarted
	HostReconcilesCompleted otelApi.Int64Counter
	// HostReconcilesRestarts is a number (counter) of host restarts during reconcile
	HostReconcilesRestarts otelApi.Int64Counter
	// HostReconcilesErrors is a number (counter) of failed (non-completed) host reconciles.
	HostReconcilesErrors otelApi.Int64Counter
	// HostReconcilesTimings is a histogram of durations of successfully completed host reconciles
	HostReconcilesTimings otelApi.Float64Histogram

	PodAddEvents    otelApi.Int64Counter
	PodUpdateEvents otelApi.Int64Counter
	PodDeleteEvents otelApi.Int64Counter
}

var m *Metrics

func createMetrics() *Metrics {
	// The unit u should be defined using the appropriate [UCUM](https://ucum.org) case-sensitive code.
	CHIReconcilesStarted, _ := metrics.Meter().Int64Counter(
		"clickhouse_operator_chi_reconciles_started",
		otelApi.WithDescription("number of CHI reconciles started"),
		otelApi.WithUnit("items"),
	)
	CHIReconcilesCompleted, _ := metrics.Meter().Int64Counter(
		"clickhouse_operator_chi_reconciles_completed",
		otelApi.WithDescription("number of CHI reconciles completed successfully"),
		otelApi.WithUnit("items"),
	)
	CHIReconcilesAborted, _ := metrics.Meter().Int64Counter(
		"clickhouse_operator_chi_reconciles_aborted",
		otelApi.WithDescription("number of CHI reconciles aborted"),
		otelApi.WithUnit("items"),
	)
	CHIReconcilesTimings, _ := metrics.Meter().Float64Histogram(
		"clickhouse_operator_chi_reconciles_timings",
		otelApi.WithDescription("timings of CHI reconciles completed successfully"),
		otelApi.WithUnit("s"),
	)

	HostReconcilesStarted, _ := metrics.Meter().Int64Counter(
		"clickhouse_operator_host_reconciles_started",
		otelApi.WithDescription("number of host reconciles started"),
		otelApi.WithUnit("items"),
	)
	HostReconcilesCompleted, _ := metrics.Meter().Int64Counter(
		"clickhouse_operator_host_reconciles_completed",
		otelApi.WithDescription("number of host reconciles completed successfully"),
		otelApi.WithUnit("items"),
	)
	HostReconcilesRestarts, _ := metrics.Meter().Int64Counter(
		"clickhouse_operator_host_reconciles_restarts",
		otelApi.WithDescription("number of host restarts during reconciles"),
		otelApi.WithUnit("items"),
	)
	HostReconcilesErrors, _ := metrics.Meter().Int64Counter(
		"clickhouse_operator_host_reconciles_errors",
		otelApi.WithDescription("number of host reconciles errors"),
		otelApi.WithUnit("items"),
	)
	HostReconcilesTimings, _ := metrics.Meter().Float64Histogram(
		"clickhouse_operator_host_reconciles_timings",
		otelApi.WithDescription("timings of host reconciles completed successfully"),
		otelApi.WithUnit("s"),
	)

	PodAddEvents, _ := metrics.Meter().Int64Counter(
		"clickhouse_operator_pod_add_events",
		otelApi.WithDescription("number PodAdd events"),
		otelApi.WithUnit("items"),
	)
	PodUpdateEvents, _ := metrics.Meter().Int64Counter(
		"clickhouse_operator_pod_update_events",
		otelApi.WithDescription("number PodUpdate events"),
		otelApi.WithUnit("items"),
	)
	PodDeleteEvents, _ := metrics.Meter().Int64Counter(
		"clickhouse_operator_pod_delete_events",
		otelApi.WithDescription("number PodDelete events"),
		otelApi.WithUnit("items"),
	)

	return &Metrics{
		CHIReconcilesStarted:   CHIReconcilesStarted,
		CHIReconcilesCompleted: CHIReconcilesCompleted,
		CHIReconcilesAborted:   CHIReconcilesAborted,
		CHIReconcilesTimings:   CHIReconcilesTimings,

		HostReconcilesStarted:   HostReconcilesStarted,
		HostReconcilesCompleted: HostReconcilesCompleted,
		HostReconcilesRestarts:  HostReconcilesRestarts,
		HostReconcilesErrors:    HostReconcilesErrors,
		HostReconcilesTimings:   HostReconcilesTimings,

		PodAddEvents:    PodAddEvents,
		PodUpdateEvents: PodUpdateEvents,
		PodDeleteEvents: PodDeleteEvents,
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
func metricsCHIReconcilesAborted(ctx context.Context) {
	ensureMetrics().CHIReconcilesAborted.Add(ctx, 1)
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
func metricsHostReconcilesRestart(ctx context.Context) {
	ensureMetrics().HostReconcilesRestarts.Add(ctx, 1)
}
func metricsHostReconcilesErrors(ctx context.Context) {
	ensureMetrics().HostReconcilesErrors.Add(ctx, 1)
}
func metricsHostReconcilesTimings(ctx context.Context, seconds float64) {
	ensureMetrics().HostReconcilesTimings.Record(ctx, seconds)
}

func metricsPodAdd(ctx context.Context) {
	ensureMetrics().PodAddEvents.Add(ctx, 1)
}
func metricsPodUpdate(ctx context.Context) {
	ensureMetrics().PodUpdateEvents.Add(ctx, 1)
}
func metricsPodDelete(ctx context.Context) {
	ensureMetrics().PodDeleteEvents.Add(ctx, 1)
}
