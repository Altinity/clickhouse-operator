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
	"github.com/MakeNowJust/heredoc"

	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"
)

const (
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
	FROM system.events`

	queryTableSizesSQL = `
	SELECT
		database,
		table, 
		toString(uniq(partition)) AS partitions, 
		toString(count())         AS parts, 
		toString(sum(bytes))      AS bytes, 
		toString(sum(data_uncompressed_bytes)) AS uncompressed_bytes, 
		toString(sum(rows))       AS rows 
	FROM system.parts
	WHERE active = 1
	GROUP BY database, table`
)

type Fetcher struct {
	Hostname string
	Username string
	Password string
	Port     int
}

func NewFetcher(hostname, username, password string, port int) *Fetcher {
	return &Fetcher{
		Hostname: hostname,
		Username: username,
		Password: password,
		Port:     port,
	}
}

func (f *Fetcher) newConn() *clickhouse.Conn {
	return clickhouse.New(f.Hostname, f.Username, f.Password, f.Port)
}

// clickHouseQueryMetrics requests metrics data from the ClickHouse database using REST interface
// data is a concealed output
func (f *Fetcher) clickHouseQueryMetrics(data *[][]string) error {
	conn := f.newConn()
	if rows, err := conn.Query(heredoc.Doc(queryMetricsSQL)); err != nil {
		return err
	} else {
		for rows.Next() {
			var metric, value, description, _type string
			if err := rows.Scan(&metric, &value, &description, &_type); err == nil {
				*data = append(*data, []string{metric, value, description, _type})
			} else {
				// Skip erroneous line
			}
		}
	}
	return nil
}

// clickHouseQueryTableSizes requests data sizes from the ClickHouse database using REST interface
// data is a concealed output
func (f *Fetcher) clickHouseQueryTableSizes(data *[][]string) error {
	conn := f.newConn()
	if rows, err := conn.Query(heredoc.Doc(queryTableSizesSQL)); err != nil {
		return err
	} else {
		for rows.Next() {
			var database, table, partitions, parts, bytes, uncompressed, _rows string
			if err := rows.Scan(&database, &table, &partitions, &parts, &bytes, &uncompressed, &_rows); err == nil {
				*data = append(*data, []string{database, table, partitions, parts, bytes, uncompressed, _rows})
			} else {
				// Skip erroneous line
			}
		}
	}
	return nil
}
