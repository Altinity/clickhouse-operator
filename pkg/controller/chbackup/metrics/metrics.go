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

// Package metrics exposes metrics of operator-managed backups and restores. They are created on
// the operator's own meter (pkg/metrics/operator), so they are served by the operator's metrics
// endpoint alongside the installation metrics.
package metrics

import (
	"context"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/altinity/clickhouse-operator/pkg/metrics/operator"
)

// Names of the last-outcome gauges.
const (
	lastSuccessName = "clickhouse_operator_backup_last_success_timestamp"
	lastFailureName = "clickhouse_operator_backup_last_failure_timestamp"
)

type instruments struct {
	backupsStarted      metric.Int64Counter
	backupsCompleted    metric.Int64Counter
	backupsFailed       metric.Int64Counter
	restoresStarted     metric.Int64Counter
	restoresCompleted   metric.Int64Counter
	restoresFailed      metric.Int64Counter
	verificationsFailed metric.Int64Counter
	backupDuration      metric.Float64Histogram
	lastSuccess         metric.Int64ObservableGauge
	lastFailure         metric.Int64ObservableGauge
}

// latestKey identifies a series of a last-outcome gauge.
type latestKey struct {
	name      string
	namespace string
	chi       string
	cluster   string
}

var (
	// meterSource returns the meter to create instruments on, nil until the operator's metrics
	// exporter has started.
	meterSource = operator.Meter

	mu      sync.Mutex
	created *instruments

	// latest holds the time each last-outcome series was last set to, so that it only ever moves
	// forward: the controllers set it again from every status they reconcile, older ones included. The
	// gauges export what it holds, so that a series dropped from it is no longer exported.
	latestMu sync.Mutex
	latest   = map[latestKey]int64{}
)

// ensure returns the instruments, creating them once on the operator's meter. Before the exporter
// has started there is no meter; a measurement then is dropped rather than recorded on a no-op
// meter, which would otherwise be cached and keep every later measurement from being exported.
func ensure() *instruments {
	mu.Lock()
	defer mu.Unlock()

	if created != nil {
		return created
	}
	meter := meterSource()
	if meter == nil {
		return nil
	}
	created = create(meter)
	return created
}

func create(meter metric.Meter) *instruments {
	i := &instruments{}
	i.backupsStarted, _ = meter.Int64Counter(
		"clickhouse_operator_backups_started",
		metric.WithDescription("number of backups started"),
		metric.WithUnit("items"),
	)
	i.backupsCompleted, _ = meter.Int64Counter(
		"clickhouse_operator_backups_completed",
		metric.WithDescription("number of backups completed successfully"),
		metric.WithUnit("items"),
	)
	i.backupsFailed, _ = meter.Int64Counter(
		"clickhouse_operator_backups_failed",
		metric.WithDescription("number of backups failed"),
		metric.WithUnit("items"),
	)
	i.restoresStarted, _ = meter.Int64Counter(
		"clickhouse_operator_restores_started",
		metric.WithDescription("number of restores started"),
		metric.WithUnit("items"),
	)
	i.restoresCompleted, _ = meter.Int64Counter(
		"clickhouse_operator_restores_completed",
		metric.WithDescription("number of restores completed successfully"),
		metric.WithUnit("items"),
	)
	i.restoresFailed, _ = meter.Int64Counter(
		"clickhouse_operator_restores_failed",
		metric.WithDescription("number of restores failed"),
		metric.WithUnit("items"),
	)
	i.verificationsFailed, _ = meter.Int64Counter(
		"clickhouse_operator_backup_verifications_failed",
		metric.WithDescription("number of backup verifications failed"),
		metric.WithUnit("items"),
	)
	i.backupDuration, _ = meter.Float64Histogram(
		"clickhouse_operator_backup_duration_seconds",
		metric.WithDescription("duration of backups completed successfully"),
		metric.WithUnit("s"),
	)
	i.lastSuccess, _ = meter.Int64ObservableGauge(
		lastSuccessName,
		metric.WithDescription("unix timestamp of the last backup completed successfully"),
		metric.WithUnit("s"),
		metric.WithInt64Callback(observeLatest(lastSuccessName)),
	)
	i.lastFailure, _ = meter.Int64ObservableGauge(
		lastFailureName,
		metric.WithDescription("unix timestamp of the last backup failed"),
		metric.WithUnit("s"),
		metric.WithInt64Callback(observeLatest(lastFailureName)),
	)
	return i
}

// observeLatest returns the callback that exports every series of the named last-outcome gauge.
func observeLatest(name string) metric.Int64Callback {
	return func(_ context.Context, observer metric.Int64Observer) error {
		latestMu.Lock()
		defer latestMu.Unlock()

		for key, at := range latest {
			if key.name == name {
				observer.Observe(at, attributes(key.namespace, key.chi, key.cluster))
			}
		}
		return nil
	}
}

// recordLatest sets a last-outcome gauge of a cluster of an installation to at, unless it is set to a
// later time already. The time is kept even before the operator's meter exists: the gauge exports it
// once the meter does.
func recordLatest(name, namespace, chi, cluster string, at time.Time) {
	// The gauges, and with them the callbacks that export the times, exist once the meter does
	ensure()

	latestMu.Lock()
	defer latestMu.Unlock()

	key := latestKey{name: name, namespace: namespace, chi: chi, cluster: cluster}
	if previous, found := latest[key]; found && (previous > at.Unix()) {
		return
	}
	latest[key] = at.Unix()
}

// BackupSucceededAt sets the time of the last successful backup of a cluster of an installation,
// unless a later one is set. The controllers call it with what the status of every backup and
// schedule they reconcile records, so the gauge is back after an operator restart.
func BackupSucceededAt(_ context.Context, namespace, chi, cluster string, at time.Time) {
	recordLatest(lastSuccessName, namespace, chi, cluster, at)
}

// BackupFailedAt sets the time of the last failed backup of a cluster of an installation, unless a
// later one is set, as BackupSucceededAt does for successful ones.
func BackupFailedAt(_ context.Context, namespace, chi, cluster string, at time.Time) {
	recordLatest(lastFailureName, namespace, chi, cluster, at)
}

// RetainClusters drops the last-outcome series of every cluster of an installation that is not listed.
// A cluster that left the installation, or turned its backup off, makes no backup any more: its series,
// left in place, would fire every alert on the age of the last backup for good.
func RetainClusters(namespace, chi string, clusters []string) {
	latestMu.Lock()
	defer latestMu.Unlock()

	for key := range latest {
		if (key.namespace == namespace) && (key.chi == chi) && !slices.Contains(clusters, key.cluster) {
			delete(latest, key)
		}
	}
}

// attributes identify the cluster of an installation a measurement is about. Every Job drives one
// cluster, which an alert on the last backup of an installation has to tell apart.
func attributes(namespace, chi, cluster string) metric.MeasurementOption {
	return metric.WithAttributes(
		attribute.String(operator.LabelNamespace, namespace),
		attribute.String(operator.LabelCHI, chi),
		attribute.String(operator.LabelCluster, cluster),
	)
}

// BackupStarted counts a backup started on a cluster.
func BackupStarted(ctx context.Context, namespace, chi, cluster string) {
	if i := ensure(); i != nil {
		i.backupsStarted.Add(ctx, 1, attributes(namespace, chi, cluster))
	}
}

// BackupCompleted counts a backup completed on a cluster and records its duration and completion time.
func BackupCompleted(ctx context.Context, namespace, chi, cluster string, duration time.Duration, completed time.Time) {
	if i := ensure(); i != nil {
		i.backupsCompleted.Add(ctx, 1, attributes(namespace, chi, cluster))
		i.backupDuration.Record(ctx, duration.Seconds(), attributes(namespace, chi, cluster))
	}
	recordLatest(lastSuccessName, namespace, chi, cluster, completed)
}

// BackupFailed counts a backup failed on a cluster and records when it failed.
func BackupFailed(ctx context.Context, namespace, chi, cluster string, failed time.Time) {
	if i := ensure(); i != nil {
		i.backupsFailed.Add(ctx, 1, attributes(namespace, chi, cluster))
	}
	recordLatest(lastFailureName, namespace, chi, cluster, failed)
}

// VerificationFailed counts a backup verification failed on a cluster.
func VerificationFailed(ctx context.Context, namespace, chi, cluster string) {
	if i := ensure(); i != nil {
		i.verificationsFailed.Add(ctx, 1, attributes(namespace, chi, cluster))
	}
}

// RestoreStarted counts a restore started on a cluster.
func RestoreStarted(ctx context.Context, namespace, chi, cluster string) {
	if i := ensure(); i != nil {
		i.restoresStarted.Add(ctx, 1, attributes(namespace, chi, cluster))
	}
}

// RestoreCompleted counts a restore completed on a cluster.
func RestoreCompleted(ctx context.Context, namespace, chi, cluster string) {
	if i := ensure(); i != nil {
		i.restoresCompleted.Add(ctx, 1, attributes(namespace, chi, cluster))
	}
}

// RestoreFailed counts a restore failed on a cluster.
func RestoreFailed(ctx context.Context, namespace, chi, cluster string) {
	if i := ensure(); i != nil {
		i.restoresFailed.Add(ctx, 1, attributes(namespace, chi, cluster))
	}
}
