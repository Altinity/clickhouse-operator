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
	"context"
	"database/sql"
	"fmt"

	"github.com/MakeNowJust/heredoc"

	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

const (
	querySystemReplicasSQL = `
		SELECT
			database,
			table,
			'1' AS session_expired
		FROM system.replicas
		WHERE is_session_expired
	`

	queryMetricsSQLTemplate = `
    	SELECT
        	concat('metric.', metric) AS metric,
        	toString(value)           AS value,
        	''                        AS description,
        	'gauge'                   AS type
    	FROM %s
	    UNION ALL
    	SELECT
        	concat('metric.', metric) AS metric,
        	toString(value)           AS value,
        	''                        AS description,
        	'gauge'                   AS type
    	FROM system.asynchronous_metrics
	    UNION ALL
	    SELECT
	        concat('event.', event)   AS metric,
	        toString(value)           AS value,
	        ''                        AS description,
	        'counter'                 AS type
	    FROM system.events
	    UNION ALL
	    SELECT
	        'metric.MemoryDictionaryBytesAllocated'  AS metric,
	        toString(sum(bytes_allocated))           AS value,
	        'Memory size allocated for dictionaries' AS description,
	        'gauge'                                  AS type
	    FROM system.dictionaries
	    UNION ALL
	    SELECT
            'metric.LongestRunningQuery' AS metric,
            toString(max(elapsed))       AS value,
            'Longest running query time' AS description,
            'gauge'                      AS type
	    FROM system.processes
		UNION ALL
		SELECT
            'metric.ChangedSettingsHash'                  AS metric,
            toString(groupBitXor(cityHash64(name,value))) AS value,
            'Control sum for changed settings'            AS description,
            'gauge'                                       AS type
		FROM system.settings WHERE changed
		UNION ALL
		SELECT
		    concat('metric.SystemErrors_',name) AS metric,
		    toString(sum(value))                AS value,
		    'Error counter from system.errors'  AS description,
			'counter'                           AS type
		FROM system.errors
        GROUP BY name
	`
	querySystemPartsSQL = `
		SELECT
			database,
			table,
			toString(active)                       AS active,
			toString(uniq(partition))              AS partitions,
			toString(count())                      AS parts,
			toString(sum(bytes))                   AS bytes,
			toString(sum(data_uncompressed_bytes)) AS uncompressed_bytes,
			toString(sum(rows))                    AS rows,
	        toString(sum(bytes_on_disk))           AS metric_DiskDataBytes,
	        toString(sum(primary_key_bytes_in_memory_allocated)) AS metric_MemoryPrimaryKeyBytesAllocated
		FROM system.parts
		GROUP BY active, database, table
	`

	queryMutationsSQL = `
		SELECT
			database,
			table,
			count()          AS mutations,
			sum(parts_to_do) AS parts_to_do
		FROM system.mutations
		WHERE is_done = 0
		GROUP BY database, table
	`

	querySystemDisksSQL = `
	    SELECT
	        name,
            toString(free_space)  AS free_space,
			toString(total_space) AS total_space
        FROM system.disks
        WHERE type IN ('local','Local')
	`

	queryDetachedPartsSQL = `
		SELECT
			count() AS detached_parts,
			database,
			table,
			disk,
			if(coalesce(reason,'unknown')='','detached_by_user',coalesce(reason,'unknown')) AS detach_reason
		FROM system.detached_parts
		GROUP BY
			database,
			table,
			disk,
			reason
    `
)

// MetricsFetcher specifies clickhouse fetcher object
type MetricsFetcher struct {
	connectionParams *clickhouse.EndpointConnectionParams
	tablesRegexp     string
	// Used to filter system-metric names while fetching metrics. Nil means keep all.
	metricsFilter MetricsFilter
}

// NewMetricsFetcher creates new clickhouse fetcher object
func NewMetricsFetcher(
	endpointConnectionParams *clickhouse.EndpointConnectionParams,
	tablesRegexp string,
	metricsFilter MetricsFilter,
) *MetricsFetcher {
	return &MetricsFetcher{
		connectionParams: endpointConnectionParams,
		tablesRegexp:     tablesRegexp,
		metricsFilter:    metricsFilter,
	}
}

// connection is a connection getter
func (f *MetricsFetcher) connection() *clickhouse.Connection {
	return clickhouse.GetPooledDBConnection(f.connectionParams)
}

// buildMetricsTableSource returns the FROM clause for the metrics query —
// the WHICH-tables half of the SQL build (cf. buildMetricsSQL which assembles
// the full query). If tablesRegexp is set it uses merge() to query tables
// matching the regexp; otherwise it falls back to the bundled default.
func (f *MetricsFetcher) buildMetricsTableSource() string {
	if f.tablesRegexp == "" {
		return "merge('system','^(metrics|custom_metrics)$')"
	}
	return fmt.Sprintf("merge('system','%s')", f.tablesRegexp)
}

// buildMetricsSQL renders the metrics-fetch SQL by substituting the table-source
// clause into queryMetricsSQLTemplate. Symmetric with buildMetricsTableSource:
// one helper picks WHICH tables to read, this one assembles the full query.
func (f *MetricsFetcher) buildMetricsSQL() string {
	return fmt.Sprintf(queryMetricsSQLTemplate, f.buildMetricsTableSource())
}

// getClickHouseQueryMetrics requests metrics data from ClickHouse.
// Excluded names are dropped during row scan so they never enter the in-memory buffer.
// SQL-side filtering was tried and abandoned: wrapping the UNION-ALL in
// `FROM (...) WHERE NOT (...)` caused zero rows on restart-then-scrape windows.
func (f *MetricsFetcher) getClickHouseQueryMetrics(ctx context.Context) (Table, error) {
	return f.clickHouseQueryScanRows(
		ctx,
		f.buildMetricsSQL(),
		func(rows *sql.Rows, data *Table) error {
			var metric, value, description, _type string
			if err := rows.Scan(&metric, &value, &description, &_type); err == nil {
				f.appendMetricRow(data, metric, value, description, _type)
			}
			return nil
		},
	)
}

// appendMetricRow adds a scanned system-metrics row to the buffer, dropping excluded
// names first. This is a memory pre-filter on the highest-cardinality fetch path (the
// system.metrics/asynchronous_metrics UNION, whose per-CPU OS series scale with core
// count) so excluded rows never enter the in-memory Table. Names synthesized by the
// other query paths (parts, mutations, disks, replicas) never reach this scan, so the
// writer-side filter stays authoritative for those; where both paths see a name the
// filter is identical, so re-checking writer-side is idempotent.
func (f *MetricsFetcher) appendMetricRow(data *Table, metric, value, description, _type string) {
	if IsExcluded(f.metricsFilter, metric) {
		return
	}
	*data = append(*data, []string{metric, value, description, _type})
}

// getClickHouseSystemParts requests data sizes from ClickHouse
func (f *MetricsFetcher) getClickHouseSystemParts(ctx context.Context) (Table, error) {
	return f.clickHouseQueryScanRows(
		ctx,
		querySystemPartsSQL,
		func(rows *sql.Rows, data *Table) error {
			var database, table, active, partitions, parts, bytes, uncompressed, _rows,
				metricDiskDataBytes, metricMemoryPrimaryKeyBytesAllocated string
			if err := rows.Scan(
				&database, &table, &active, &partitions, &parts, &bytes, &uncompressed, &_rows,
				&metricDiskDataBytes, &metricMemoryPrimaryKeyBytesAllocated,
			); err == nil {
				*data = append(*data, []string{
					database, table, active, partitions, parts, bytes, uncompressed, _rows,
					metricDiskDataBytes, metricMemoryPrimaryKeyBytesAllocated,
				})
			}
			return nil
		},
	)
}

// getClickHouseQuerySystemReplicas requests replica information from ClickHouse
func (f *MetricsFetcher) getClickHouseQuerySystemReplicas(ctx context.Context) (Table, error) {
	return f.clickHouseQueryScanRows(
		ctx,
		querySystemReplicasSQL,
		func(rows *sql.Rows, data *Table) error {
			var database, table, isSessionExpired string
			if err := rows.Scan(&database, &table, &isSessionExpired); err == nil {
				*data = append(*data, []string{database, table, isSessionExpired})
			}
			return nil
		},
	)
}

// getClickHouseQueryMutations requests mutations information from ClickHouse
func (f *MetricsFetcher) getClickHouseQueryMutations(ctx context.Context) (Table, error) {
	return f.clickHouseQueryScanRows(
		ctx,
		queryMutationsSQL,
		func(rows *sql.Rows, data *Table) error {
			var database, table, mutations, partsToDo string
			if err := rows.Scan(&database, &table, &mutations, &partsToDo); err == nil {
				*data = append(*data, []string{database, table, mutations, partsToDo})
			}
			return nil
		},
	)
}

// getClickHouseQuerySystemDisks requests used disks information from ClickHouse
func (f *MetricsFetcher) getClickHouseQuerySystemDisks(ctx context.Context) (Table, error) {
	return f.clickHouseQueryScanRows(
		ctx,
		querySystemDisksSQL,
		func(rows *sql.Rows, data *Table) error {
			var disk, freeBytes, totalBytes string
			if err := rows.Scan(&disk, &freeBytes, &totalBytes); err == nil {
				*data = append(*data, []string{disk, freeBytes, totalBytes})
			}
			return nil
		},
	)
}

// getClickHouseQueryDetachedParts requests detached parts reasons from ClickHouse
func (f *MetricsFetcher) getClickHouseQueryDetachedParts(ctx context.Context) (Table, error) {
	return f.clickHouseQueryScanRows(
		ctx,
		queryDetachedPartsSQL,
		func(rows *sql.Rows, data *Table) error {
			var detachedParts, database, table, disk, reason string
			if err := rows.Scan(&detachedParts, &database, &table, &disk, &reason); err == nil {
				*data = append(*data, []string{detachedParts, database, table, disk, reason})
			}
			return nil
		},
	)
}

// clickHouseQueryScanRows scan all rows by external scan function
func (f *MetricsFetcher) clickHouseQueryScanRows(
	ctx context.Context,
	sql string,
	scanner ScanFunction,
) (Table, error) {
	if util.IsContextDone(ctx) {
		return nil, ctx.Err()
	}
	query, err := f.connection().QueryContext(ctx, heredoc.Doc(sql))
	if err != nil {
		return nil, err
	}
	defer query.Close()
	data := newTable()
	for query.Rows.Next() {
		if util.IsContextDone(ctx) {
			return nil, ctx.Err()
		}
		_ = scanner(query.Rows, &data)
	}
	return data, nil
}
