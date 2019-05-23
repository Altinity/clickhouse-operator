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
	// Comma-separated ''-enclosed list of database names to be ignored
	ignoredDBs = "'system'"

	// Max number of retries for SQL queries
	maxRetries = 10
)

// ClusterGetCreateDatabases returns set of 'CREATE DATABASE ...' SQLs
func ClusterGetCreateDatabases(chi *chi.ClickHouseInstallation, cluster *chi.ChiCluster) ([]string, []string, error) {
	sql := `
		SELECT
			distinct name AS name,
			concat('CREATE DATABASE IF NOT EXISTS ', name) AS create_db_query
		FROM cluster('%s', system, databases) 
		WHERE name not in (%s)
		ORDER BY name
		SETTINGS skip_unavailable_shards = 1`
	sql = fmt.Sprintf(sql, cluster.Name, ignoredDBs)

	dbNames := make([]string, 0)
	createStatements := make([]string, 0)
	glog.V(1).Info(CreateChiServiceFQDN(chi))
	conn := clickhouse.New(CreateChiServiceFQDN(chi), "", "", 8123)
	if rows, err := conn.Query(sql); err != nil {
		return nil, nil, err
	} else {
		for rows.Next() {
			var name, create string
			if err := rows.Scan(&name, &create); err == nil {
				dbNames = append(dbNames, name)
				createStatements = append(createStatements, create)
			} else {
				// Skip erroneous line
			}
		}
	}
	return dbNames, createStatements, nil
}

// ClusterGetCreateTables returns set of 'CREATE TABLE ...' SQLs
func ClusterGetCreateTables(chi *chi.ClickHouseInstallation, cluster *chi.ChiCluster) ([]string, []string, error) {
	sql := `
		SELECT
			distinct name, 
			replaceRegexpOne(create_table_query, 'CREATE (TABLE|VIEW|MATERIALIZED VIEW)', 'CREATE \\1 IF NOT EXISTS') 
		FROM cluster('%s', system, tables)
		WHERE database not in (%s)
			AND name not like '.inner.%%'
		ORDER BY multiIf(engine not in ('Distributed', 'View', 'MaterializedView'), 1, engine = 'MaterializedView', 2, engine = 'Distributed', 3, 4), name
		SETTINGS skip_unavailable_shards = 1`
	sql = fmt.Sprintf(sql, cluster.Name, ignoredDBs)

	tableNames := make([]string, 0)
	createStatements := make([]string, 0)
	glog.V(1).Info(CreateChiServiceFQDN(chi))
	conn := clickhouse.New(CreateChiServiceFQDN(chi), "", "", 8123)
	if rows, err := conn.Query(sql); err != nil {
		return nil, nil, err
	} else {
		for rows.Next() {
			var name, create string
			if err := rows.Scan(&name, &create); err == nil {
				tableNames = append(tableNames, name)
				createStatements = append(createStatements, create)
			} else {
				// Skip erroneous line
			}
		}
	}
	return tableNames, createStatements, nil
}

// ReplicaGetDropTables returns set of 'DROP TABLE ...' SQLs
func ReplicaGetDropTables(replica *chi.ChiReplica) ([]string, []string, error) {
	// There isn't a separate query for deleting views. To delete a view, use DROP TABLE
	// See https://clickhouse.yandex/docs/en/query_language/create/

	sql := `
		SELECT
			distinct name, 
			concat('DROP TABLE IF EXISTS ', database, '.', name)
		FROM system.tables
		WHERE database not in (%s) 
			AND engine like 'Replicated%%'`
	sql = fmt.Sprintf(sql, ignoredDBs)

	tableNames := make([]string, 0)
	dropStatements := make([]string, 0)
	glog.V(1).Info(CreatePodFQDN(replica))
	conn := clickhouse.New(CreatePodFQDN(replica), "", "", 8123)
	if rows, err := conn.Query(sql); err != nil {
		return nil, nil, err
	} else {
		for rows.Next() {
			var name, create string
			if err := rows.Scan(&name, &create); err == nil {
				tableNames = append(tableNames, name)
				dropStatements = append(dropStatements, create)
			} else {
				// Skip erroneous line
			}
		}
	}
	return tableNames, dropStatements, nil
}

// ChiDropDnsCache runs 'DROP DNS CACHE' over the whole CHI
func ChiDropDnsCache(chi *chi.ClickHouseInstallation) error {
	sqls := []string{
		`SYSTEM DROP DNS CACHE`,
	}
	return ChiApplySQLs(chi, sqls)
}

// ClusterApplySQLs runs set of SQL queries over the cluster
func ClusterApplySQLs(cluster *chi.ChiCluster, sqls []string, retry bool) error {
	return applySQLs(CreatePodFQDNsOfCluster(cluster), sqls, retry)
}

// ChiApplySQLs runs set of SQL queries over the whole CHI
func ChiApplySQLs(chi *chi.ClickHouseInstallation, sqls []string) error {
	return applySQLs(CreatePodFQDNsOfChi(chi), sqls, true)
}

// ReplicaApplySQLs runs set of SQL queries over the replica
func ReplicaApplySQLs(replica *chi.ChiReplica, sqls []string, retry bool) error {
	hosts := []string{CreatePodFQDN(replica)}
	return applySQLs(hosts, sqls, true)
}

// applySQLs runs set of SQL queries on set on hosts
func applySQLs(hosts []string, sqls []string, retry bool) error {
	var err error = nil
	// For each host in the list run all SQL queries
	for _, host := range hosts {
		conn := clickhouse.New(host, "", "", 8123)
		for _, sql := range sqls {
			if len(sql) == 0 {
				// Skip malformed SQL query, move to the next SQL query
				continue
			}
			// Now retry this SQL query on particular host
			for retryCount := 0; retryCount < maxRetries; retryCount++ {
				glog.V(1).Infof("applySQL(%s)\n", sql)
				err = conn.Exec(sql)
				if (err == nil) || !retry {
					// Either all is good or we are not interested in retries anyway
					// Move on to the next SQL query on this host
					break
				}
				glog.V(1).Infof("attempt %d failed, sleep and retry\n", retryCount)
				seconds := (retryCount + 1) * 5
				time.Sleep(time.Duration(seconds) * time.Second)
			}
		}
	}

	return err
}
