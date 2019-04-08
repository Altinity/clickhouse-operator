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

package models

import (
	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse"
	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

func ClusterGatherCreateDatabases(cluster *v1.ChiCluster) ([]string, error) {
	return gatherUnique(CreateClusterPodFQDNs(cluster), "SELECT concat('CREATE DATABASE IF NOT EXISTS ', name) FROM system.databases WHERE name != 'system' ORDER BY name")
}

func ClusterGatherCreateTables(cluster *v1.ChiCluster) ([]string, error) {
	return gatherUnique(CreateClusterPodFQDNs(cluster), "SELECT create_table_query FROM system.tables WHERE database != 'system'")
}

func ClusterApplySQLs(cluster *v1.ChiCluster, sqls []string) error {
	return applySQLs(CreateClusterPodFQDNs(cluster), sqls)
}

func gatherUnique(hosts []string, sql string) ([]string, error) {
	// Make set-alike data-structure with map - fetch items from all hosts which maps item to bool
	allItemsMap := make(map[string]bool)
	for _, host := range hosts {
		// Items gathered from this host - slice of rows (slices)
		hostItemsRows := make([][]string, 0)
		if clickhouse.Query(&hostItemsRows, sql, host) == nil {
			for _, row := range hostItemsRows {
				item := row[0]
				allItemsMap[item] = true
			}
		}
	}

	// Extract items from map into slice - slice is more user-friendly
	items := make([]string, 0, len(allItemsMap))
	for _, item := range items {
		items = append(items, item)
	}

	return items, nil
}

func applySQLs(hosts []string, sqls []string) error {
	data := make([][]string, 0)
	for _, host := range hosts {
		for _, sql := range sqls {
			clickhouse.Query(&data, sql, host)
		}
	}

	return nil
}
