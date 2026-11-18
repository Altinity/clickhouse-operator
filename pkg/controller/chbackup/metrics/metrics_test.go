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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkMetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/altinity/clickhouse-operator/pkg/metrics/operator"
)

// useMeter swaps the operator meter for one the test reads back, starting from no instruments.
// It mutates package state, which is safe only because no test here runs in parallel.
func useMeter(t *testing.T, meter func() metric.Meter) {
	t.Helper()
	saved := meterSource
	meterSource = meter
	created = nil
	latest = map[latestKey]int64{}
	t.Cleanup(func() {
		meterSource = saved
		created = nil
		latest = map[latestKey]int64{}
	})
}

func newReader(t *testing.T) *sdkMetric.ManualReader {
	t.Helper()
	reader := sdkMetric.NewManualReader()
	meter := sdkMetric.NewMeterProvider(sdkMetric.WithReader(reader)).Meter("test")
	useMeter(t, func() metric.Meter { return meter })
	return reader
}

func collect(t *testing.T, reader *sdkMetric.ManualReader) map[string]metricdata.Aggregation {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	collected := map[string]metricdata.Aggregation{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			collected[m.Name] = m.Data
		}
	}
	return collected
}

func counterValue(t *testing.T, collected map[string]metricdata.Aggregation, name string) int64 {
	t.Helper()
	sum, ok := collected[name].(metricdata.Sum[int64])
	require.True(t, ok, "%s must be collected as a counter", name)
	require.Len(t, sum.DataPoints, 1)
	return sum.DataPoints[0].Value
}

// The exporter starts asynchronously, so a measurement may come before there is a meter. It is
// dropped; what must not happen is a no-op instrument being cached for good, silencing every
// later measurement.
func TestMeasurementsBeforeTheMeterAreDroppedNotCached(t *testing.T) {
	var meter metric.Meter
	var reader *sdkMetric.ManualReader
	useMeter(t, func() metric.Meter { return meter })

	BackupStarted(context.Background(), "ns", "demo", "main")
	require.Nil(t, created)

	reader = sdkMetric.NewManualReader()
	meter = sdkMetric.NewMeterProvider(sdkMetric.WithReader(reader)).Meter("test")
	BackupStarted(context.Background(), "ns", "demo", "main")

	require.Equal(t, int64(1), counterValue(t, collect(t, reader), "clickhouse_operator_backups_started"))
}

func TestInstrumentsAreCreatedOnce(t *testing.T) {
	calls := 0
	meter := sdkMetric.NewMeterProvider(sdkMetric.WithReader(sdkMetric.NewManualReader())).Meter("test")
	useMeter(t, func() metric.Meter {
		calls++
		return meter
	})

	var wg sync.WaitGroup
	results := make([]*instruments, 32)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = ensure()
		}(i)
	}
	wg.Wait()

	for _, result := range results {
		require.Same(t, results[0], result)
	}
	require.Equal(t, 1, calls, "instruments are created on the first meter and kept")
}

// Every measurement tells the cluster of the installation it is about: a Job drives one cluster, and
// an alert on the last backup of an installation has to tell its clusters apart.
func TestMeasurementsCarryTheInstallationLabels(t *testing.T) {
	reader := newReader(t)
	completed := time.Unix(1700000000, 0)

	BackupCompleted(context.Background(), "ns", "demo", "main", 90*time.Second, completed)
	BackupFailed(context.Background(), "ns", "demo", "main", completed.Add(time.Hour))
	VerificationFailed(context.Background(), "ns", "demo", "main")
	RestoreStarted(context.Background(), "ns", "demo", "main")
	RestoreCompleted(context.Background(), "ns", "demo", "main")
	RestoreFailed(context.Background(), "ns", "demo", "main")

	collected := collect(t, reader)
	for _, name := range []string{
		"clickhouse_operator_backups_completed",
		"clickhouse_operator_backups_failed",
		"clickhouse_operator_backup_verifications_failed",
		"clickhouse_operator_restores_started",
		"clickhouse_operator_restores_completed",
		"clickhouse_operator_restores_failed",
	} {
		require.Equal(t, int64(1), counterValue(t, collected, name), name)
	}

	sum := collected["clickhouse_operator_backups_completed"].(metricdata.Sum[int64])
	attributes := sum.DataPoints[0].Attributes
	chi, _ := attributes.Value(attribute.Key(operator.LabelCHI))
	namespace, _ := attributes.Value(attribute.Key(operator.LabelNamespace))
	cluster, _ := attributes.Value(attribute.Key(operator.LabelCluster))
	require.Equal(t, "demo", chi.AsString())
	require.Equal(t, "ns", namespace.AsString())
	require.Equal(t, "main", cluster.AsString())
	require.Equal(t, 3, attributes.Len())

	gauge, ok := collected["clickhouse_operator_backup_last_success_timestamp"].(metricdata.Gauge[int64])
	require.True(t, ok)
	require.Equal(t, completed.Unix(), gauge.DataPoints[0].Value)
	gauge, ok = collected["clickhouse_operator_backup_last_failure_timestamp"].(metricdata.Gauge[int64])
	require.True(t, ok)
	require.Equal(t, completed.Add(time.Hour).Unix(), gauge.DataPoints[0].Value)

	histogram, ok := collected["clickhouse_operator_backup_duration_seconds"].(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Equal(t, float64(90), histogram.DataPoints[0].Sum)
}

func gaugeValues(t *testing.T, reader *sdkMetric.ManualReader, name string) map[string]int64 {
	t.Helper()
	values := map[string]int64{}
	gauge, ok := collect(t, reader)[name].(metricdata.Gauge[int64])
	if !ok {
		return values
	}
	for _, point := range gauge.DataPoints {
		chi, _ := point.Attributes.Value(attribute.Key(operator.LabelCHI))
		cluster, _ := point.Attributes.Value(attribute.Key(operator.LabelCluster))
		values[chi.AsString()+"/"+cluster.AsString()] = point.Value
	}
	return values
}

// The last-outcome gauges are set again from every status the controllers reconcile, in no
// particular order, which is how they come back after an operator restart. They only ever move
// forward, so an old backup reconciled after a newer one leaves the newer time in place, and each
// cluster of an installation has its own: a newer backup of one cluster says nothing of another.
func TestLastOutcomeGaugesKeepTheLatestTime(t *testing.T) {
	reader := newReader(t)
	older, newer := time.Unix(1700000000, 0), time.Unix(1700086400, 0)

	BackupSucceededAt(context.Background(), "ns", "demo", "hot", newer)
	BackupSucceededAt(context.Background(), "ns", "demo", "hot", older)
	BackupSucceededAt(context.Background(), "ns", "demo", "cold", older)
	BackupSucceededAt(context.Background(), "ns", "other", "hot", older)
	BackupFailedAt(context.Background(), "ns", "demo", "hot", newer)
	BackupFailed(context.Background(), "ns", "demo", "hot", older)

	require.Equal(t, map[string]int64{"demo/hot": newer.Unix(), "demo/cold": older.Unix(), "other/hot": older.Unix()},
		gaugeValues(t, reader, "clickhouse_operator_backup_last_success_timestamp"))
	require.Equal(t, map[string]int64{"demo/hot": newer.Unix()},
		gaugeValues(t, reader, "clickhouse_operator_backup_last_failure_timestamp"))
}

// A cluster that left the installation, or its backup, makes no backup any more: its last-outcome
// series go, so that an alert on the age of the last backup does not fire on it for good, while the
// series of the clusters that stay, and of other installations, keep their times.
func TestRetainClustersDropsTheSeriesOfClustersThatLeft(t *testing.T) {
	reader := newReader(t)
	older, newer := time.Unix(1700000000, 0), time.Unix(1700086400, 0)
	BackupSucceededAt(context.Background(), "ns", "demo", "hot", newer)
	BackupSucceededAt(context.Background(), "ns", "demo", "cold", older)
	BackupFailedAt(context.Background(), "ns", "demo", "cold", older)
	BackupSucceededAt(context.Background(), "ns", "other", "cold", older)
	BackupSucceededAt(context.Background(), "elsewhere", "demo", "cold", older)

	RetainClusters("ns", "demo", []string{"hot"})

	require.Equal(t, map[string]int64{"ns/demo/hot": newer.Unix(), "ns/other/cold": older.Unix(), "elsewhere/demo/cold": older.Unix()},
		series(t, reader, lastSuccessName))
	require.Empty(t, series(t, reader, lastFailureName))

	BackupSucceededAt(context.Background(), "ns", "demo", "hot", older)
	require.Equal(t, newer.Unix(), series(t, reader, lastSuccessName)["ns/demo/hot"], "a retained series still only moves forward")
}

// The controllers set the last-outcome gauges from the statuses they reconcile as soon as the operator
// starts, possibly before its metrics exporter has: what they set is exported once it has.
func TestLastOutcomesBeforeTheMeterAreExportedOnceItExists(t *testing.T) {
	var meter metric.Meter
	useMeter(t, func() metric.Meter { return meter })
	at := time.Unix(1700000000, 0)
	BackupSucceededAt(context.Background(), "ns", "demo", "main", at)
	require.Nil(t, created)

	reader := sdkMetric.NewManualReader()
	meter = sdkMetric.NewMeterProvider(sdkMetric.WithReader(reader)).Meter("test")
	BackupFailedAt(context.Background(), "ns", "demo", "main", at.Add(time.Hour))

	require.Equal(t, map[string]int64{"ns/demo/main": at.Unix()}, series(t, reader, lastSuccessName))
	require.Equal(t, map[string]int64{"ns/demo/main": at.Add(time.Hour).Unix()}, series(t, reader, lastFailureName))
}

// series returns the values a gauge is exported with, by namespace, installation and cluster.
func series(t *testing.T, reader *sdkMetric.ManualReader, name string) map[string]int64 {
	t.Helper()
	values := map[string]int64{}
	gauge, _ := collect(t, reader)[name].(metricdata.Gauge[int64])
	for _, point := range gauge.DataPoints {
		namespace, _ := point.Attributes.Value(attribute.Key(operator.LabelNamespace))
		chi, _ := point.Attributes.Value(attribute.Key(operator.LabelCHI))
		cluster, _ := point.Attributes.Value(attribute.Key(operator.LabelCluster))
		values[namespace.AsString()+"/"+chi.AsString()+"/"+cluster.AsString()] = point.Value
	}
	return values
}
