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
	sqlmodule "database/sql"
	"time"

	"github.com/MakeNowJust/heredoc"

	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"
)

const (
	querySystemReplicasSQL = `
		SELECT
			database,
			table,
			toString(is_session_expired) AS is_session_expired
		FROM system.replicas
	`

	queryMetricsSQL = `
    	SELECT
        	concat('metric.', metric) AS metric,
        	toString(value)           AS value, 
        	''                        AS description, 
        	'gauge'                   AS type
    	FROM system.asynchronous_metrics
    	UNION ALL 
    	SELECT 
	        concat('metric.', metric) AS metric, 
	        toString(value)           AS value, 
    	    ''                        AS description,       
	        'gauge'                   AS type   
	    FROM system.metrics
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
            'metric.ChangedSettingsHash'       AS metric,
            toString(groupBitXor(cityHash64(name,value))) AS value,
            'Control sum for changed settings' AS description,
            'gauge'                            AS type
		FROM system.settings WHERE changed
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
            toString(free_space) AS free_space,
			toString(total_space) AS total_space			
        FROM system.disks
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

// ClickHouseFetcher specifies clickhouse fetcher object
type ClickHouseFetcher struct {
	connectionParams *clickhouse.ConnectionParams
}

// NewClickHouseFetcher creates new clickhouse fetcher object
func NewClickHouseFetcher(scheme, hostname, username, password string, port int) *ClickHouseFetcher {
	return &ClickHouseFetcher{
		connectionParams: clickhouse.NewConnectionParams(scheme, hostname, username, password, port),
	}
}

// SetQueryTimeout sets query timeout
func (f *ClickHouseFetcher) SetQueryTimeout(timeout time.Duration) *ClickHouseFetcher {
	f.connectionParams.SetQueryTimeout(timeout)
	return f
}

func (f *ClickHouseFetcher) getConnection() *clickhouse.Connection {
	return clickhouse.GetPooledDBConnection(f.connectionParams)
}

// getClickHouseQueryMetrics requests metrics data from ClickHouse
func (f *ClickHouseFetcher) getClickHouseQueryMetrics() ([][]string, error) {
	return f.clickHouseQueryScanRows(
		queryMetricsSQL,
		func(rows *sqlmodule.Rows, data *[][]string) error {
			var metric, value, description, _type string
			if err := rows.Scan(&metric, &value, &description, &_type); err == nil {
				*data = append(*data, []string{metric, value, description, _type})
			}
			return nil
		},
	)
}

// getClickHouseSystemParts requests data sizes from ClickHouse
func (f *ClickHouseFetcher) getClickHouseSystemParts() ([][]string, error) {
	return f.clickHouseQueryScanRows(
		querySystemPartsSQL,
		func(rows *sqlmodule.Rows, data *[][]string) error {
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
func (f *ClickHouseFetcher) getClickHouseQuerySystemReplicas() ([][]string, error) {
	return f.clickHouseQueryScanRows(
		querySystemReplicasSQL,
		func(rows *sqlmodule.Rows, data *[][]string) error {
			var database, table, isSessionExpired string
			if err := rows.Scan(&database, &table, &isSessionExpired); err == nil {
				*data = append(*data, []string{database, table, isSessionExpired})
			}
			return nil
		},
	)
}

// getClickHouseQueryMutations requests mutations information from ClickHouse
func (f *ClickHouseFetcher) getClickHouseQueryMutations() ([][]string, error) {
	return f.clickHouseQueryScanRows(
		queryMutationsSQL,
		func(rows *sqlmodule.Rows, data *[][]string) error {
			var database, table, mutations, partsToDo string
			if err := rows.Scan(&database, &table, &mutations, &partsToDo); err == nil {
				*data = append(*data, []string{database, table, mutations, partsToDo})
			}
			return nil
		},
	)
}

// getClickHouseQuerySystemDisks requests used disks information from ClickHouse
func (f *ClickHouseFetcher) getClickHouseQuerySystemDisks() ([][]string, error) {
	return f.clickHouseQueryScanRows(
		querySystemDisksSQL,
		func(rows *sqlmodule.Rows, data *[][]string) error {
			var disk, freeBytes, totalBytes string
			if err := rows.Scan(&disk, &freeBytes, &totalBytes); err == nil {
				*data = append(*data, []string{disk, freeBytes, totalBytes})
			}
			return nil
		},
	)
}

// getClickHouseQueryDetachedParts requests detached parts reasons from ClickHouse
func (f *ClickHouseFetcher) getClickHouseQueryDetachedParts() ([][]string, error) {
	return f.clickHouseQueryScanRows(
		queryDetachedPartsSQL,
		func(rows *sqlmodule.Rows, data *[][]string) error {
			var detachedParts, database, table, disk, reason string
			if err := rows.Scan(&detachedParts, &database, &table, &disk, &reason); err == nil {
				*data = append(*data, []string{detachedParts, database, table, disk, reason})
			}
			return nil
		},
	)
}

// clickHouseQueryScanRows scan all rows by external scan function
func (f *ClickHouseFetcher) clickHouseQueryScanRows(
	sql string,
	scan func(
		rows *sqlmodule.Rows,
		data *[][]string,
	) error,
) ([][]string, error) {
	query, err := f.getConnection().Query(heredoc.Doc(sql))
	if err != nil {
		return nil, err
	}
	defer query.Close()
	data := make([][]string, 0)
	for query.Rows.Next() {
		_ = scan(query.Rows, &data)
	}
	return data, nil
}
