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

func (s *Schemer) getObjectListFromClickHouse(services []string, sql string) ([]string, []string, error) {
	// Results
	names := make([]string, 0)
	sqlStatements := make([]string, 0)

	var rows *sqlmodule.Rows = nil
	var err error
	for _, service := range services {
		log.V(2).Infof("Trying %s", service)
		conn := s.getCHConnection(service)

		rows, err = conn.Query(sql)
		if err == nil {
			defer rows.Close()
			break
		}
	}
	if err != nil {
		return nil, nil, err
	}

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
	// cluster_tables := fmt.Sprintf("cluster('%s', system, tables)", cluster.Name)
	hosts := CreatePodFQDNsOfCluster(cluster)
	cluster_tables := fmt.Sprintf("remote('%s', system, tables)", strings.Join(hosts, ","))

	sqlDBs := heredoc.Doc(strings.ReplaceAll(`
		SELECT DISTINCT 
			database AS name, 
			concat('CREATE DATABASE IF NOT EXISTS "', name, '"') AS create_query
		FROM 
		(
			SELECT DISTINCT arrayJoin([database, extract(engine_full, 'Distributed\\([^,]+, *\'?([^,\']+)\'?, *[^,]+')]) database
			FROM cluster('all-sharded', system.tables) tables
			WHERE engine = 'Distributed'
			SETTINGS skip_unavailable_shards = 1
		)`,
		"cluster('all-sharded', system.tables)",
		cluster_tables,
	))
	sqlTables := heredoc.Doc(strings.ReplaceAll(`
		SELECT DISTINCT 
			concat(database,'.', name) as name, 
			replaceRegexpOne(create_table_query, 'CREATE (TABLE|VIEW|MATERIALIZED VIEW)', 'CREATE \\1 IF NOT EXISTS')
		FROM 
		(
			SELECT 
			    database, name,
				create_table_query,
				2 AS order
			FROM cluster('all-sharded', system.tables) tables
			WHERE engine = 'Distributed'
			SETTINGS skip_unavailable_shards = 1
			UNION ALL
			SELECT 
				extract(engine_full, 'Distributed\\([^,]+, *\'?([^,\']+)\'?, *[^,]+') AS database, 
				extract(engine_full, 'Distributed\\([^,]+, [^,]+, *\'?([^,\\\')]+)') AS name,
				t.create_table_query,
				1 AS order
			FROM cluster('all-sharded', system.tables) tables
			LEFT JOIN system.tables t USING (database, name)
			WHERE engine = 'Distributed' AND t.create_table_query != ''
			SETTINGS skip_unavailable_shards = 1
		) tables
		ORDER BY order
		`,
		"cluster('all-sharded', system.tables)",
		cluster_tables,
	))

	names1, sqlStatements1, _ := s.getObjectListFromClickHouse(CreatePodFQDNsOfChi(cluster.GetCHI()), sqlDBs)
	names2, sqlStatements2, _ := s.getObjectListFromClickHouse(CreatePodFQDNsOfChi(cluster.GetCHI()), sqlTables)
	return append(names1, names2...), append(sqlStatements1, sqlStatements2...) , nil
}

// getCreateReplicaObjects returns a list of objects that needs to be created on a host in a cluster
func (s *Schemer) getCreateReplicaObjects(host *chop.ChiHost) ([]string, []string, error) {

	var shard *chop.ChiShard = nil
    var replicaIndex int
	for shardIndex := range host.GetCluster().Layout.Shards {
		shard = &host.GetCluster().Layout.Shards[shardIndex]
		for replicaIndex = range shard.Hosts {
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
	nReplicas := len(replicas)
	if nReplicas <=1 {
		log.V(1).Info("Single replica in a shard. Nothing to create from")
		return nil, nil, nil
	}
	// remove new replica from the list. See https://stackoverflow.com/questions/37334119/how-to-delete-an-element-from-a-slice-in-golang
	replicas[replicaIndex] = replicas[nReplicas-1]
	replicas = replicas[:nReplicas-1]
	log.V(1).Infof("Extracting table definitions from %v", replicas)
	
	system_tables := fmt.Sprintf("remote('%s', system, tables)", strings.Join(replicas, ","))
	
	sqlDBs := heredoc.Doc(strings.ReplaceAll(`
		SELECT DISTINCT 
			database AS name, 
			concat('CREATE DATABASE IF NOT EXISTS "', name, '"') AS create_db_query
		FROM system.tables
		WHERE database != 'system'
		SETTINGS skip_unavailable_shards = 1`, 
		"system.tables", system_tables,
	))
	sqlTables := heredoc.Doc(strings.ReplaceAll(`
		SELECT DISTINCT 
			name, 
			replaceRegexpOne(create_table_query, 'CREATE (TABLE|VIEW|MATERIALIZED VIEW)', 'CREATE \\1 IF NOT EXISTS')
		FROM system.tables
		WHERE database != 'system' and create_table_query != '' and name not like '.inner.%'
		SETTINGS skip_unavailable_shards = 1`,
		"system.tables",
		system_tables,
	))

	names1, sqlStatements1, _ := s.getObjectListFromClickHouse(CreatePodFQDNsOfChi(host.GetCHI()), sqlDBs)
	names2, sqlStatements2, _ := s.getObjectListFromClickHouse(CreatePodFQDNsOfChi(host.GetCHI()), sqlTables)
	return append(names1, names2...), append(sqlStatements1, sqlStatements2...) , nil
}

// hostGetDropTables returns set of 'DROP TABLE ...' SQLs
func (s *Schemer) hostGetDropTables(host *chop.ChiHost) ([]string, []string, error) {
	// There isn't a separate query for deleting views. To delete a view, use DROP TABLE
	// See https://clickhouse.yandex/docs/en/query_language/create/
	sql := heredoc.Docf(`
		SELECT
			distinct name, 
			concat('DROP TABLE IF EXISTS "', database, '"."', name, '"')
		FROM system.tables
		WHERE database not in (%s) 
			AND engine like 'Replicated%%'
		`,
		ignoredDBs,
	)

	names, sqlStatements, _ := s.getObjectListFromClickHouse([]string{CreatePodFQDN(host)}, sql)
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
	log.V(1).Infof("Migrating schema objects to host %s", host.Address.HostName)

	names, createSQLs, _ := s.getCreateReplicaObjects(host)
	log.V(1).Infof("Creating replica objects %v at %s", names, host.Address.HostName)
	_ = s.hostApplySQLs(host, createSQLs, true)

	names, createSQLs, _ = s.clusterGetCreateDistributedObjects(host.GetCluster())
	log.V(1).Infof("Creating distributed objects %v at %s", names, host.Address.HostName)
	return s.hostApplySQLs(host, createSQLs, true)
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
// Retry logic traverses the list of SQLs multiple times until all SQLs succeed
func (s *Schemer) applySQLs(hosts []string, sqls []string, retry bool) error {
	var err error = nil
	// For each host in the list run all SQL queries
	for _, host := range hosts {
		conn := s.getCHConnection(host)
		maxTries := 1
		if retry {
			maxTries = defaultMaxTries
		}
		err = util.Retry(maxTries, "Applying sqls", func() error {
			var runErr error = nil
			for i, sql := range sqls {
				if len(sql) == 0 {
					// Skip malformed or already executed SQL query, move to the next one
					continue
				}
				err = conn.Exec(sql)
				if err != nil && strings.Contains(err.Error(), "Code: 253,") && strings.Contains(sql, "CREATE TABLE") {
					log.V(1).Info("Replica is already in ZooKeeper. Trying ATTACH TABLE instead")
					sqlAttach := strings.ReplaceAll(sql, "CREATE TABLE", "ATTACH TABLE")
					err = conn.Exec(sqlAttach)
				}
				if err == nil {
					sqls[i] = "" // Query is executed, removing from the list
				} else {
					runErr = err
				} 
			}
			return runErr
		})
	}

	return err
}
