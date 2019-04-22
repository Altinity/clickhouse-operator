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
	"fmt"
	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse"
	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/golang/glog"
	"time"
)

const (
	ignoredDBs = "'system'"
)

func ClusterGetCreateDatabases(chi *chi.ClickHouseInstallation, cluster *chi.ChiCluster) ([]string, []string, error) {
	result := make([][]string, 0)
	glog.V(1).Info(CreateChiServiceFQDN(chi))
	_ = clickhouse.Query(&result,
		fmt.Sprintf(
			`SELECT distinct name, concat('CREATE DATABASE IF NOT EXISTS ', name)
				FROM cluster('%s', system, databases) 
				WHERE name not in (%s)
				ORDER BY name
				SETTINGS skip_unavailable_shards = 1`,
			cluster.Name, ignoredDBs),
		CreateChiServiceFQDN(chi),
	)
	names, creates := unzip(result)
	return names, creates, nil
}

func ClusterGetCreateTables(chi *chi.ClickHouseInstallation, cluster *chi.ChiCluster) ([]string, []string, error) {
	result := make([][]string, 0)
	_ = clickhouse.Query(&result,
		fmt.Sprintf(
			`SELECT distinct name, 
			        replaceRegexpOne(create_table_query, 'CREATE (TABLE|VIEW|MATERIALIZED VIEW)', 'CREATE \\1 IF NOT EXISTS') 
				FROM cluster('%s', system, tables)
				WHERE database not in (%s) 
				AND name not like '.inner.%%'
				ORDER BY multiIf(engine not in ('Distributed', 'View', 'MaterializedView'), 1, engine = 'MaterializedView', 2, engine = 'Distributed', 3, 4), name
				SETTINGS skip_unavailable_shards = 1`,
			cluster.Name, ignoredDBs),
		CreateChiServiceFQDN(chi),
	)
	names, creates := unzip(result)
	return names, creates, nil
}

func unzip(slice [][]string) ([]string, []string) {
	col1, col2 := make([]string, len(slice)), make([]string, len(slice))
	for i := 0; i < len(slice); i++ {
		col1 = append(col1, slice[i][0])
		if len(slice[i]) > 1 {
			col2 = append(col2, slice[i][1])
		}
	}
	return col1, col2
}

func ClusterDropDnsCache(cluster *chi.ChiCluster) error {
	time.Sleep(45 * time.Second)
	sqls := []string{
		`SYSTEM DROP DNS CACHE`,
	}
	return ClusterApplySQLs(cluster, sqls)
}

func ClusterApplySQLs(cluster *chi.ChiCluster, sqls []string) error {
	return applySQLs(CreateClusterPodFQDNs(cluster), sqls)
}

func applySQLs(hosts []string, sqls []string) error {
	for _, host := range hosts {
		for _, sql := range sqls {
			if len(sql) > 0 {
				data := make([][]string, 0)
				_ = clickhouse.Exec(&data, sql, host)
				glog.V(1).Infof("applySQL(%s):%v\n", sql, data)
			}
		}
	}

	return nil
}
