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
	"fmt"
	"strings"

	"github.com/MakeNowJust/heredoc"
	log "github.com/golang/glog"
	// log "k8s.io/klog"

	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

const (
	// Comma-separated ''-enclosed list of database names to be ignored
	// ignoredDBs = "'system'"

	// Max number of tries for SQL queries
	defaultMaxTries = 10
)

// Schemer
type Schemer struct {
	Username string
	Password string
	Port     int
}

// NewSchemer
func NewSchemer(username, password string, port int) *Schemer {
	return &Schemer{
		Username: username,
		Password: password,
		Port:     port,
	}
}

// getCHConnection
func (s *Schemer) getCHConnection(hostname string) *clickhouse.CHConnection {
	return clickhouse.GetPooledDBConnection(clickhouse.NewCHConnectionParams(hostname, s.Username, s.Password, s.Port))
}

// getObjectListFromClickHouse
func (s *Schemer) getObjectListFromClickHouse(endpoints []string, sql string) ([]string, []string, error) {
	if len(endpoints) == 0 {
		// Nowhere to fetch data from
		return nil, nil, nil
	}

	// Results
	var names []string
	var statements []string
	var err error

	// Fetch data from any of specified services
	var query *clickhouse.Query = nil
	for _, endpoint := range endpoints {
		log.V(1).Infof("Run query on: %s of %v", endpoint, endpoints)

		query, err = s.getCHConnection(endpoint).Query(sql)
		if err == nil {
			// One of specified services returned result, no need to iterate more
			break
		} else {
			log.V(1).Infof("Run query on: %s of %v FAILED skip to next. err: %v", endpoint, endpoints, err)
		}
	}
	if err != nil {
		log.V(1).Infof("Run query FAILED on all %v", endpoints)
		return nil, nil, err
	}

	// Some data available, let's fetch it
	defer query.Close()

	for query.Rows.Next() {
		var name, statement string
		if err := query.Rows.Scan(&name, &statement); err == nil {
			names = append(names, name)
			statements = append(statements, statement)
		} else {
			log.V(1).Infof("UNABLE to scan row err: %v", err)
		}
	}

	return names, statements, nil
}

// getCreateDistributedObjects returns a list of objects that needs to be created on a shard in a cluster
// That includes all Distributed tables, corresponding local tables, and databases, if necessary
func (s *Schemer) getCreateDistributedObjects(host *chop.ChiHost) ([]string, []string, error) {
	hosts := CreatePodFQDNsOfCluster(host.GetCluster())
	nHosts := len(hosts)
	if nHosts <= 1 {
		log.V(1).Info("Single host in a cluster. Nothing to create a schema from.")
		return nil, nil, nil
	}

	var hostIndex int
	for i, h := range hosts {
		if h == CreatePodFQDN(host) {
			hostIndex = i
			break
		}
	}

	// remove new host from the list. See https://stackoverflow.com/questions/37334119/how-to-delete-an-element-from-a-slice-in-golang
	hosts[hostIndex] = hosts[nHosts-1]
	hosts = hosts[:nHosts-1]
	log.V(1).Infof("Extracting distributed table definitions from hosts: %v", hosts)

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
			LEFT JOIN (SELECT distinct database, name, create_table_query 
			             FROM cluster('all-sharded', system.tables) SETTINGS skip_unavailable_shards = 1)  t USING (database, name)
			WHERE engine = 'Distributed' AND t.create_table_query != ''
			SETTINGS skip_unavailable_shards = 1
		) tables
		ORDER BY order
		`,
		"cluster('all-sharded', system.tables)",
		cluster_tables,
	))

	log.V(1).Infof("fetch dbs list")
	log.V(1).Infof("dbs sql\n%v", sqlDBs)
	names1, sqlStatements1, _ := s.getObjectListFromClickHouse(CreatePodFQDNsOfCHI(host.GetCHI()), sqlDBs)
	log.V(1).Infof("names1:")
	for _, v := range names1 {
		log.V(1).Infof("names1: %s", v)
	}
	log.V(1).Infof("sql1:")
	for _, v := range sqlStatements1 {
		log.V(1).Infof("sql1: %s", v)
	}

	log.V(1).Infof("fetch table list")
	log.V(1).Infof("tbl sql\n%v", sqlTables)
	names2, sqlStatements2, _ := s.getObjectListFromClickHouse(CreatePodFQDNsOfCHI(host.GetCHI()), sqlTables)
	log.V(1).Infof("names2:")
	for _, v := range names2 {
		log.V(1).Infof("names2: %s", v)
	}
	log.V(1).Infof("sql2:")
	for _, v := range sqlStatements2 {
		log.V(1).Infof("sql2: %s", v)
	}

	return append(names1, names2...), append(sqlStatements1, sqlStatements2...), nil
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
	if nReplicas <= 1 {
		log.V(1).Info("Single replica in a shard. Nothing to create a schema from.")
		return nil, nil, nil
	}
	// remove new replica from the list. See https://stackoverflow.com/questions/37334119/how-to-delete-an-element-from-a-slice-in-golang
	replicas[replicaIndex] = replicas[nReplicas-1]
	replicas = replicas[:nReplicas-1]
	log.V(1).Infof("Extracting replicated table definitions from %v", replicas)

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

	names1, sqlStatements1, _ := s.getObjectListFromClickHouse(CreatePodFQDNsOfCHI(host.GetCHI()), sqlDBs)
	names2, sqlStatements2, _ := s.getObjectListFromClickHouse(CreatePodFQDNsOfCHI(host.GetCHI()), sqlTables)
	return append(names1, names2...), append(sqlStatements1, sqlStatements2...), nil
}

// hostGetDropTables returns set of 'DROP TABLE ...' SQLs
func (s *Schemer) hostGetDropTables(host *chop.ChiHost) ([]string, []string, error) {
	// There isn't a separate query for deleting views. To delete a view, use DROP TABLE
	// See https://clickhouse.yandex/docs/en/query_language/create/
	sql := heredoc.Doc(`
		SELECT
			distinct name, 
			concat('DROP TABLE IF EXISTS "', database, '"."', name, '"') AS drop_db_query
		FROM system.tables
		WHERE engine like 'Replicated%'`,
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
	log.V(1).Infof("Creating replica objects at %s: %v", host.Address.HostName, names)
	log.V(1).Infof("\n%v", createSQLs)
	err1 := s.hostApplySQLs(host, createSQLs, true)

	names, createSQLs, _ = s.getCreateDistributedObjects(host)
	log.V(1).Infof("Creating distributed objects at %s: %v", host.Address.HostName, names)
	log.V(1).Infof("\n%v", createSQLs)
	err2 := s.hostApplySQLs(host, createSQLs, true)

	if err2 != nil {
		return err2
	}
	if err1 != nil {
		return err1
	}

	return nil
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
	return s.applySQLs(CreatePodFQDNsOfCHI(chi), sqls, retry)
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
