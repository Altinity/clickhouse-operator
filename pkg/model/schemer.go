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

package model

import (
	sqlmodule "database/sql"

	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"
	"github.com/altinity/clickhouse-operator/pkg/util"

	"fmt"
	"github.com/MakeNowJust/heredoc"
	log "github.com/golang/glog"
	// log "k8s.io/klog"
	"strings"
)

const (
	// Comma-separated ''-enclosed list of database names to be ignored
	ignoredDBs = "'system'"

	// Max number of tries for SQL queries
	defaultMaxTries = 10
)

type Schemer struct {
	Username string
	Password string
	Port     int
}

func NewSchemer(username, password string, port int) *Schemer {
	return &Schemer{
		Username: username,
		Password: password,
		Port:     port,
	}
}

func (s *Schemer) getCHConnection(hostname string) *clickhouse.CHConnection {
	return clickhouse.GetPooledDBConnection(clickhouse.NewCHConnectionParams(hostname, s.Username, s.Password, s.Port))
}

func (s *Schemer) getObjectListFromClickHouse(serviceUrl string, sql string) ([]string, []string, error) {
	// Results
	names := make([]string, 0)
	sqlStatements := make([]string, 0)

	log.V(1).Info(serviceUrl)
	conn := s.getCHConnection(serviceUrl)
	var rows *sqlmodule.Rows = nil
	var err error
	rows, err = conn.Query(sql)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	// Some data fetched
	for rows.Next() {
		var name, sqlStatement string
		if err := rows.Scan(&name, &sqlStatement); err == nil {
			names = append(names, name)
			sqlStatements = append(sqlStatements, sqlStatement)
		} else {
			// Skip erroneous line
		}
	}
	return names, sqlStatements, nil
}

// clusterGetCreateDistributedObjects returns a list of objects that needs to be created on a shard in a cluster
// That includes all Distributed tables, corresponding local tables, and databases, if necessary
func (s *Schemer) clusterGetCreateDistributedObjects(cluster *chop.ChiCluster) ([]string, []string, error) {
	// system_tables := fmt.Sprintf("cluster('%s', system, tables)", cluster.Name)
	hosts := CreatePodFQDNsOfCluster(cluster)
	system_tables := fmt.Sprintf("remote('%s', system, tables)", strings.Join(hosts, ","))

	sql := heredoc.Doc(strings.ReplaceAll(`
		SELECT DISTINCT 
			database AS name, 
			concat('CREATE DATABASE IF NOT EXISTS ', name) AS create_db_query
		FROM 
		(
			SELECT DISTINCT database
			FROM system.tables
			WHERE engine = 'Distributed'
			SETTINGS skip_unavailable_shards = 1
			UNION ALL
			SELECT DISTINCT extract(engine_full, 'Distributed\\([^,]+, *\'?([^,\']+)\'?, *[^,]+') AS shard_database
			FROM system.tables 
			WHERE engine = 'Distributed'
			SETTINGS skip_unavailable_shards = 1
		) 
		UNION ALL
		SELECT DISTINCT 
			name, 
			replaceRegexpOne(create_table_query, 'CREATE (TABLE|VIEW|MATERIALIZED VIEW)', 'CREATE \\1 IF NOT EXISTS')
		FROM 
		(
			SELECT 
				database, 
				name,
				2 AS order
			FROM system.tables
			WHERE engine = 'Distributed'
			SETTINGS skip_unavailable_shards = 1
			UNION ALL
			SELECT 
				extract(engine_full, 'Distributed\\([^,]+, *\'?([^,\']+)\'?, *[^,]+') AS shard_database, 
				extract(engine_full, 'Distributed\\([^,]+, [^,]+, *\'?([^,\\\')]+)') AS shard_table,
				1 AS order
			FROM system.tables
			WHERE engine = 'Distributed'
			SETTINGS skip_unavailable_shards = 1
		) 
		LEFT JOIN (select distinct database, name, create_table_query from system.tables SETTINGS skip_unavailable_shards = 1) USING (database, name)
		ORDER BY order
		`,
		"system.tables",
		system_tables,
	))

	names, sqlStatements, _ := s.getObjectListFromClickHouse(CreateChiServiceFQDN(cluster.GetCHI()), sql)
	return names, sqlStatements, nil
}

// getCreateReplicatedObjects returns a list of objects that needs to be created on a host in a cluster
func (s *Schemer) getCreateReplicatedObjects(host *chop.ChiHost) ([]string, []string, error) {

	var shard *chop.ChiShard = nil
	for shardIndex := range host.GetCluster().Layout.Shards {
		shard = &host.GetCluster().Layout.Shards[shardIndex]
		for replicaIndex := range shard.Hosts {
			replica := shard.Hosts[replicaIndex]
			if replica == host {
				break
			}
		}
	}
	if shard == nil {
		log.V(1).Info("Can not find shard for replica")
		return nil, nil, nil
	}
	replicas := CreatePodFQDNsOfShard(shard)
	system_tables := fmt.Sprintf("remote('%s', system, tables)", strings.Join(replicas, ","))
	sql := heredoc.Doc(strings.ReplaceAll(`
		SELECT DISTINCT 
			database AS name, 
			concat('CREATE DATABASE IF NOT EXISTS ', name) AS create_db_query
		FROM system.tables
		WHERE engine_full LIKE 'Replicated%'
		SETTINGS skip_unavailable_shards = 1
		UNION ALL
		SELECT DISTINCT 
			name, 
			replaceRegexpOne(create_table_query, 'CREATE (TABLE|VIEW|MATERIALIZED VIEW)', 'CREATE \\1 IF NOT EXISTS')
		FROM system.tables
		WHERE engine_full LIKE 'Replicated%'
		SETTINGS skip_unavailable_shards = 1
		`,
		"system.tables",
		system_tables,
	))

	names, sqlStatements, _ := s.getObjectListFromClickHouse(CreateChiServiceFQDN(host.GetCHI()), sql)
	return names, sqlStatements, nil
}

// clusterGetCreateDatabases returns list of DB names and list of 'CREATE DATABASE ...' SQLs for these DBs
func (s *Schemer) clusterGetCreateDatabases(cluster *chop.ChiCluster) ([]string, []string, error) {
	sql := heredoc.Docf(`
		SELECT
			distinct name AS name,
			concat('CREATE DATABASE IF NOT EXISTS ', name) AS create_db_query
		FROM cluster('%s', system, databases) 
		WHERE name not in (%s)
		ORDER BY name
		SETTINGS skip_unavailable_shards = 1
		`,
		cluster.Name,
		ignoredDBs,
	)

	names, sqlStatements, _ := s.getObjectListFromClickHouse(CreateChiServiceFQDN(cluster.GetCHI()), sql)
	return names, sqlStatements, nil
}

// clusterGetCreateTables returns list of table names and list of 'CREATE TABLE ...' SQLs for these tables
func (s *Schemer) clusterGetCreateTables(cluster *chop.ChiCluster) ([]string, []string, error) {
	sql := heredoc.Docf(`
		SELECT
			distinct name, 
			replaceRegexpOne(create_table_query, 'CREATE (TABLE|VIEW|MATERIALIZED VIEW)', 'CREATE \\1 IF NOT EXISTS') 
		FROM cluster('%s', system, tables)
		WHERE database not in (%s)
			AND name not like '.inner.%%'
		ORDER BY multiIf(engine not in ('Distributed', 'View', 'MaterializedView'), 1, engine = 'MaterializedView', 2, engine = 'Distributed', 3, 4), name
		SETTINGS skip_unavailable_shards = 1
		`,
		cluster.Name,
		ignoredDBs,
	)

	names, sqlStatements, _ := s.getObjectListFromClickHouse(CreateChiServiceFQDN(cluster.GetCHI()), sql)
	return names, sqlStatements, nil
}

// hostGetDropTables returns set of 'DROP TABLE ...' SQLs
func (s *Schemer) hostGetDropTables(host *chop.ChiHost) ([]string, []string, error) {
	// There isn't a separate query for deleting views. To delete a view, use DROP TABLE
	// See https://clickhouse.yandex/docs/en/query_language/create/
	sql := heredoc.Docf(`
		SELECT
			distinct name, 
			concat('DROP TABLE IF EXISTS ', database, '.', name)
		FROM system.tables
		WHERE database not in (%s) 
			AND engine like 'Replicated%%'
		`,
		ignoredDBs,
	)

	names, sqlStatements, _ := s.getObjectListFromClickHouse(CreatePodFQDN(host), sql)
	return names, sqlStatements, nil
}

// HostDeleteTables
func (s *Schemer) HostDeleteTables(host *chop.ChiHost) error {
	tableNames, dropTableSQLs, _ := s.hostGetDropTables(host)
	log.V(1).Infof("Drop tables: %v as %v", tableNames, dropTableSQLs)
	return s.hostApplySQLs(host, dropTableSQLs, false)
}

// HostCreateTables
func (s *Schemer) HostCreateTables(host *chop.ChiHost) error {

	names, createSQLs, _ := s.getCreateReplicatedObjects(host)
	log.V(1).Infof("Creating replicated objects: %v", names)
	_ = s.hostApplySQLs(host, createSQLs, true)

	names, createSQLs, _ = s.clusterGetCreateDistributedObjects(host.GetCluster())
	log.V(1).Infof("Creating distributed objects: %v", names)
	return s.hostApplySQLs(host, createSQLs, true)
}

// ShardCreateTables
func (s *Schemer) ShardCreateTables(shard *chop.ChiShard) error {

	names, createSQLs, _ := s.clusterGetCreateDistributedObjects(shard.GetCluster())
	log.V(1).Infof("Creating distributed objects: %v", names)
	return s.shardApplySQLs(shard, createSQLs, true)
}

// CHIDropDnsCache runs 'DROP DNS CACHE' over the whole CHI
func (s *Schemer) CHIDropDnsCache(chi *chop.ClickHouseInstallation) error {
	sqls := []string{
		`SYSTEM DROP DNS CACHE`,
	}
	return s.chiApplySQLs(chi, sqls, false)
}

// chiApplySQLs runs set of SQL queries over the whole CHI
func (s *Schemer) chiApplySQLs(chi *chop.ClickHouseInstallation, sqls []string, retry bool) error {
	return s.applySQLs(CreatePodFQDNsOfChi(chi), sqls, retry)
}

// clusterApplySQLs runs set of SQL queries over the cluster
func (s *Schemer) clusterApplySQLs(cluster *chop.ChiCluster, sqls []string, retry bool) error {
	return s.applySQLs(CreatePodFQDNsOfCluster(cluster), sqls, retry)
}

// hostApplySQLs runs set of SQL queries over the replica
func (s *Schemer) hostApplySQLs(host *chop.ChiHost, sqls []string, retry bool) error {
	hosts := []string{CreatePodFQDN(host)}
	return s.applySQLs(hosts, sqls, retry)
}

// shardApplySQLs runs set of SQL queries over the shard replicas
func (s *Schemer) shardApplySQLs(shard *chop.ChiShard, sqls []string, retry bool) error {
	return s.applySQLs(CreatePodFQDNsOfShard(shard), sqls, retry)
}

// applySQLs runs set of SQL queries on set on hosts
func (s *Schemer) applySQLs(hosts []string, sqls []string, retry bool) error {
	var err error = nil
	// For each host in the list run all SQL queries
	for _, host := range hosts {
		conn := s.getCHConnection(host)
		for _, sql := range sqls {
			if len(sql) == 0 {
				// Skip malformed SQL query, move to the next SQL query
				continue
			}
			maxTries := 1
			if retry {
				maxTries = defaultMaxTries
			}
			err = util.Retry(maxTries, sql, func() error {
				sqlerr := conn.Exec(sql)
				if sqlerr != nil &&
					strings.Contains(sqlerr.Error(), "Code: 253,") &&
					strings.Contains(sql, "CREATE TABLE") {
					log.V(1).Info("Replica is already in ZooKeeper. Trying ATTACH TABLE instead")
					sqlAttach := strings.ReplaceAll(sql, "CREATE TABLE", "ATTACH TABLE")
					sqlerr = conn.Exec(sqlAttach)
				}
				return sqlerr
			})
			if err != nil {
				// Do not run any more SQL queries on this host in case of failure
				// Move to next host
				break
			}
		}
	}

	return err
}
