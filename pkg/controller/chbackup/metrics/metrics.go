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

// Package metrics exposes Prometheus metrics for operator-managed backups and restores.
// It registers on the SAME OpenTelemetry meter as the rest of the operator
// (pkg/metrics/operator), so the metrics surface on the operator's existing /metrics
// endpoint (:9999) with no extra wiring.
package metrics

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/altinity/clickhouse-operator/pkg/metrics/operator"
)

// meter returns the operator's OpenTelemetry meter (pkg/metrics/operator), whose Prometheus
// exporter serves the operator's /metrics endpoint (:9999) - that is where these metrics
// surface, alongside the CHI metrics. The operator does NOT register a global meter provider,
// so we must use operator.Meter() directly. It is nil until StartMetricsExporter runs (well
// before the first backup reconcile in a real operator); the global no-op meter is only a
// fallback so unit tests, which never start the exporter, do not panic.
func meter() metric.Meter {
	if m := operator.Meter(); m != nil {
		return m
	}
	return otel.Meter("clickhouse-operator-backup")
}

type metrics struct {
	backupsStarted      metric.Int64Counter
	backupsCompleted    metric.Int64Counter
	backupsFailed       metric.Int64Counter
	restoresStarted     metric.Int64Counter
	restoresCompleted   metric.Int64Counter
	restoresFailed      metric.Int64Counter
	verificationsFailed metric.Int64Counter
	backupDuration      metric.Float64Histogram
	lastSuccess         metric.Int64Gauge
}

var m *metrics

func ensure() *metrics {
	if m == nil {
		m = create()
	}
	return m
}

func create() *metrics {
	x := &metrics{}
	x.backupsStarted, _ = meter().Int64Counter(
		"clickhouse_operator_backups_started", metric.WithDescription("number of backups started"), metric.WithUnit("items"))
	x.backupsCompleted, _ = meter().Int64Counter(
		"clickhouse_operator_backups_completed", metric.WithDescription("number of backups completed successfully"), metric.WithUnit("items"))
	x.backupsFailed, _ = meter().Int64Counter(
		"clickhouse_operator_backups_failed", metric.WithDescription("number of backups failed"), metric.WithUnit("items"))
	x.restoresStarted, _ = meter().Int64Counter(
		"clickhouse_operator_restores_started", metric.WithDescription("number of restores started"), metric.WithUnit("items"))
	x.restoresCompleted, _ = meter().Int64Counter(
		"clickhouse_operator_restores_completed", metric.WithDescription("number of restores completed successfully"), metric.WithUnit("items"))
	x.restoresFailed, _ = meter().Int64Counter(
		"clickhouse_operator_restores_failed", metric.WithDescription("number of restores failed"), metric.WithUnit("items"))
	x.verificationsFailed, _ = meter().Int64Counter(
		"clickhouse_operator_backup_verifications_failed", metric.WithDescription("number of backup verifications failed"), metric.WithUnit("items"))
	x.backupDuration, _ = meter().Float64Histogram(
		"clickhouse_operator_backup_duration_seconds", metric.WithDescription("duration of completed backups"), metric.WithUnit("s"))
	x.lastSuccess, _ = meter().Int64Gauge(
		"clickhouse_operator_backup_last_success_timestamp", metric.WithDescription("unix timestamp of the last successful backup"), metric.WithUnit("s"))
	return x
}

func attrs(namespace, chi string) metric.MeasurementOption {
	return metric.WithAttributes(
		attribute.String("namespace", namespace),
		attribute.String("clickhouse_installation", chi),
	)
}

// BackupStarted increments the backups-started counter.
func BackupStarted(ctx context.Context, namespace, chi string) {
	ensure().backupsStarted.Add(ctx, 1, attrs(namespace, chi))
}

// BackupCompleted records a successful backup, its duration, and the last-success timestamp.
func BackupCompleted(ctx context.Context, namespace, chi string, durationSeconds float64) {
	e := ensure()
	e.backupsCompleted.Add(ctx, 1, attrs(namespace, chi))
	if durationSeconds > 0 {
		e.backupDuration.Record(ctx, durationSeconds, attrs(namespace, chi))
	}
	e.lastSuccess.Record(ctx, time.Now().Unix(), attrs(namespace, chi))
}

// BackupFailed increments the backups-failed counter.
func BackupFailed(ctx context.Context, namespace, chi string) {
	ensure().backupsFailed.Add(ctx, 1, attrs(namespace, chi))
}

// RestoreStarted increments the restores-started counter.
func RestoreStarted(ctx context.Context, namespace, chi string) {
	ensure().restoresStarted.Add(ctx, 1, attrs(namespace, chi))
}

// RestoreCompleted increments the restores-completed counter.
func RestoreCompleted(ctx context.Context, namespace, chi string) {
	ensure().restoresCompleted.Add(ctx, 1, attrs(namespace, chi))
}

// RestoreFailed increments the restores-failed counter.
func RestoreFailed(ctx context.Context, namespace, chi string) {
	ensure().restoresFailed.Add(ctx, 1, attrs(namespace, chi))
}

// VerificationFailed increments the backup-verifications-failed counter.
func VerificationFailed(ctx context.Context, namespace, chi string) {
	ensure().verificationsFailed.Add(ctx, 1, attrs(namespace, chi))
}
