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
	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse"
)

const (
	queryMetricsSQL = `
	SELECT concat('metric.', metric) metric, toString(value), '' AS description, 'gauge' as type FROM system.asynchronous_metrics
	UNION ALL 
	SELECT concat('metric.', metric) metric, toString(value), description, 'gauge' as type FROM system.metrics
	UNION ALL 
	SELECT concat('event.', event) as metric, toString(value), description, 'counter' as type FROM system.events`
	queryTableSizesSQL = `select database, table, 
	uniq(partition) as partitions, count() as parts, sum(bytes) as bytes, sum(data_uncompressed_bytes) uncompressed_bytes, sum(rows) as rows 
	from system.parts where active = 1 group by database, table`
)

// clickHouseQueryMetrics requests metrics data from the ClickHouse database using REST interface
// data is a concealed output
func clickHouseQueryMetrics(data *[][]string, hostname string) error {
	return clickhouse.Query(data, queryMetricsSQL, hostname)
}

// clickHouseQueryTableSizes requests data sizes from the ClickHouse database using REST interface
// data is a concealed output
func clickHouseQueryTableSizes(data *[][]string, hostname string) error {
	return clickhouse.Query(data, queryTableSizesSQL, hostname)
}
