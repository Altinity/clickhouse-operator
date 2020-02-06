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
    	    description               AS description,       
	        'gauge'                   AS type   
	    FROM system.metrics
	    UNION ALL 
	    SELECT
	        concat('event.', event)   AS metric,
	        toString(value)           AS value,
	        description               AS description,
	        'counter'                 AS type
	    FROM system.events
	    UNION ALL
	    SELECT 
	        'metric.DiskDataBytes'                      AS metric,
	        toString(sum(bytes_on_disk))                AS value,
	        'Total data size for all ClickHouse tables' AS description,
		    'gauge'                                     AS type
	    FROM system.parts
	    UNION ALL
	    SELECT 
	        'metric.MemoryPrimaryKeyBytesAllocated'              AS metric,
	        toString(sum(primary_key_bytes_in_memory_allocated)) AS value,
	        'Memory size allocated for primary keys'             AS description,
	        'gauge'                                              AS type
	    FROM system.parts
	    UNION ALL
	    SELECT 
	        'metric.MemoryDictionaryBytesAllocated'  AS metric,
	        toString(sum(bytes_allocated))           AS value,
	        'Memory size allocated for dictionaries' AS description,
	        'gauge'                                  AS type
	    FROM system.dictionaries
	    UNION ALL
	    SELECT 
	        'metric.DiskFreeBytes'                     AS metric,
    	    toString(filesystemFree())                 AS value,
	        'Free disk space available at file system' AS description,
	        'gauge'                                    AS type
		`

	queryTableSizesSQL = `
		SELECT
			database,
			table, 
			toString(uniq(partition))              AS partitions, 
			toString(count())                      AS parts, 
			toString(sum(bytes))                   AS bytes, 
			toString(sum(data_uncompressed_bytes)) AS uncompressed_bytes, 
			toString(sum(rows))                    AS rows 
		FROM system.parts
		WHERE active = 1
		GROUP BY database, table
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
)

type ClickHouseFetcher struct {
	Hostname string
	Username string
	Password string
	Port     int
}

func NewClickHouseFetcher(hostname, username, password string, port int) *ClickHouseFetcher {
	return &ClickHouseFetcher{
		Hostname: hostname,
		Username: username,
		Password: password,
		Port:     port,
	}
}

func (f *ClickHouseFetcher) newConn() *clickhouse.Conn {
	return clickhouse.New(f.Hostname, f.Username, f.Password, f.Port)
}

// clickHouseQueryMetrics requests metrics data from ClickHouse
func (f *ClickHouseFetcher) clickHouseQueryMetrics() ([][]string, error) {
	return f.clickHouseQueryScanRows(
		heredoc.Doc(queryMetricsSQL),
		func(rows *sqlmodule.Rows, data *[][]string) error {
			var metric, value, description, _type string
			if err := rows.Scan(&metric, &value, &description, &_type); err == nil {
				*data = append(*data, []string{metric, value, description, _type})
			}
			return nil
		},
	)
}

// clickHouseQueryTableSizes requests data sizes from ClickHouse
func (f *ClickHouseFetcher) clickHouseQueryTableSizes() ([][]string, error) {
	return f.clickHouseQueryScanRows(
		heredoc.Doc(queryTableSizesSQL),
		func(rows *sqlmodule.Rows, data *[][]string) error {
			var database, table, partitions, parts, bytes, uncompressed, _rows string
			if err := rows.Scan(&database, &table, &partitions, &parts, &bytes, &uncompressed, &_rows); err == nil {
				*data = append(*data, []string{database, table, partitions, parts, bytes, uncompressed, _rows})
			}
			return nil
		},
	)
}

// clickHouseQuerySystemReplicas requests replica information from ClickHouse
func (f *ClickHouseFetcher) clickHouseQuerySystemReplicas() ([][]string, error) {
	return f.clickHouseQueryScanRows(
		heredoc.Doc(querySystemReplicasSQL),
		func(rows *sqlmodule.Rows, data *[][]string) error {
			var database, table, isSessionExpired string
			if err := rows.Scan(&database, &table, &isSessionExpired); err == nil {
				*data = append(*data, []string{database, table, isSessionExpired})
			}
			return nil
		},
	)
}

// clickHouseQuerySystemMutations requests mutations information from ClickHouse
func (f *ClickHouseFetcher) clickHouseQueryMutations() ([][]string, error) {
	return f.clickHouseQueryScanRows(
		heredoc.Doc(queryMutationsSQL),
		func(rows *sqlmodule.Rows, data *[][]string) error {
			var database, table, mutations, parts_to_do string
			if err := rows.Scan(&database, &table, &mutations, &parts_to_do); err == nil {
				*data = append(*data, []string{database, table, mutations, parts_to_do})
			}
			return nil
		},
	)
}

// clickHouseQueryScanRows scan all rows by external scan function
func (f *ClickHouseFetcher) clickHouseQueryScanRows(sql string, scan func(rows *sqlmodule.Rows, data *[][]string) error) ([][]string, error) {
	data := make([][]string, 0)
	conn := f.newConn()
	if rows, err := conn.Query(heredoc.Doc(sql)); err != nil {
		return nil, err
	} else {
		for rows.Next() {
			_ = scan(rows, &data)
		}
	}
	return data, nil
}
