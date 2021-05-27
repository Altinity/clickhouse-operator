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
	"context"
	"github.com/MakeNowJust/heredoc"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Schemer
type Schemer struct {
	clickhouse.ClusterConnectionParams
	Cluster *clickhouse.Cluster
}

// NewSchemer
func NewSchemer(username, password string, port int) *Schemer {
	params := clickhouse.ClusterConnectionParams{
		Username: username,
		Password: password,
		Port:     port,
	}
	return &Schemer{
		ClusterConnectionParams: params,
		Cluster: clickhouse.NewCluster().SetConnectionParams(params),
	}
}

// queryUnzipColumns
func (s *Schemer) queryUnzipColumns(ctx context.Context, hosts []string, sql string, columns ...*[]string) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	if len(hosts) == 0 {
		// Nowhere to fetch data from
		return nil
	}

	// Fetch data from any of specified hosts
	query, err := s.Cluster.SetHosts(hosts).QueryAny(ctx, sql)
	if err != nil {
		return nil
	}
	if query == nil {
		return nil
	}

	// Some data available, let's fetch it
	defer query.Close()
	return query.UnzipColumnsAsStrings(columns...)
}

// queryUnzip2Columns
func (s *Schemer) queryUnzip2Columns(ctx context.Context, endpoints []string, sql string) ([]string, []string, error) {
	var column1 []string
	var column2 []string
	if err := s.queryUnzipColumns(ctx, endpoints, sql, &column1, &column2); err != nil {
		return nil, nil, err
	}
	return column1, column2, nil
}

// getCreateDistributedObjectsContext returns a list of objects that needs to be created on a shard in a cluster
// That includes all distributed tables, corresponding local tables and databases, if necessary
func (s *Schemer) getCreateDistributedObjectsContext(ctx context.Context, host *chop.ChiHost) ([]string, []string, error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil, nil, nil
	}

	hosts := CreatePodFQDNsOfCluster(host.GetCluster())
	if len(hosts) <= 1 {
		log.V(1).M(host).F().Info("Single host in a cluster. Nothing to create a schema from.")
		return nil, nil, nil
	}

	log.V(1).M(host).F().Info("Extracting distributed table definitions from hosts %v", hosts)

	sqlDBs := heredoc.Doc(`
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
	)
	sqlTables := heredoc.Doc(`
		SELECT DISTINCT 
			concat(database,'.', name) as name, 
			replaceRegexpOne(create_table_query, 'CREATE (TABLE|VIEW|MATERIALIZED VIEW|DICTIONARY)', 'CREATE \\1 IF NOT EXISTS')
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
	)

	log.V(1).M(host).F().Info("fetch dbs list")
	log.V(1).M(host).F().Info("dbs sql\n%v", sqlDBs)
	names1, sqlStatements1, _ := s.queryUnzip2Columns(ctx, CreatePodFQDNsOfCHI(host.GetCHI()), sqlDBs)
	log.V(1).M(host).F().Info("names1:")
	for _, v := range names1 {
		log.V(1).M(host).F().Info("names1: %s", v)
	}
	log.V(1).M(host).F().Info("sql1:")
	for _, v := range sqlStatements1 {
		log.V(1).M(host).F().Info("sql1: %s", v)
	}

	log.V(1).M(host).F().Info("fetch table list")
	log.V(1).M(host).F().Info("tbl sql\n%v", sqlTables)
	names2, sqlStatements2, _ := s.queryUnzip2Columns(ctx, CreatePodFQDNsOfCHI(host.GetCHI()), sqlTables)
	log.V(1).M(host).F().Info("names2:")
	for _, v := range names2 {
		log.V(1).M(host).F().Info("names2: %s", v)
	}
	log.V(1).M(host).F().Info("sql2:")
	for _, v := range sqlStatements2 {
		log.V(1).M(host).F().Info("sql2: %s", v)
	}

	return append(names1, names2...), append(sqlStatements1, sqlStatements2...), nil
}

// getCreateReplicaObjectsContext returns a list of objects that needs to be created on a host in a cluster
func (s *Schemer) getCreateReplicaObjectsContext(ctx context.Context, host *chop.ChiHost) ([]string, []string, error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil, nil, nil
	}

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
		log.V(1).M(host).F().Info("Can not find shard for replica")
		return nil, nil, nil
	}
	replicas := CreatePodFQDNsOfShard(shard)
	if len(replicas) <= 1 {
		log.V(1).M(host).F().Info("Single replica in a shard. Nothing to create a schema from.")
		return nil, nil, nil
	}
	log.V(1).M(host).F().Info("Extracting replicated table definitions from %v", replicas)

	sqlDBs := heredoc.Doc(`
		SELECT DISTINCT 
			database AS name, 
			concat('CREATE DATABASE IF NOT EXISTS "', name, '"') AS create_db_query
		FROM cluster('all-sharded', system.tables) tables
		WHERE database != 'system'
		SETTINGS skip_unavailable_shards = 1`,
	)
	sqlTables := heredoc.Doc(`
		SELECT DISTINCT 
			name, 
			replaceRegexpOne(create_table_query, 'CREATE (TABLE|VIEW|MATERIALIZED VIEW|DICTIONARY)', 'CREATE \\1 IF NOT EXISTS')
		FROM cluster('all-sharded', system.tables) tables
		WHERE database != 'system' and create_table_query != '' and name not like '.inner.%'
		SETTINGS skip_unavailable_shards = 1`,
	)

	names1, sqlStatements1, _ := s.queryUnzip2Columns(ctx, CreatePodFQDNsOfCHI(host.GetCHI()), sqlDBs)
	names2, sqlStatements2, _ := s.queryUnzip2Columns(ctx, CreatePodFQDNsOfCHI(host.GetCHI()), sqlTables)
	return append(names1, names2...), append(sqlStatements1, sqlStatements2...), nil
}

// hostGetDropTables returns set of 'DROP TABLE ...' SQLs
func (s *Schemer) hostGetDropTablesContext(ctx context.Context, host *chop.ChiHost) ([]string, []string, error) {
	// There isn't a separate query for deleting views. To delete a view, use DROP TABLE
	// See https://clickhouse.yandex/docs/en/query_language/create/
	sql := heredoc.Doc(`
		SELECT
			distinct name, 
			concat('DROP TABLE IF EXISTS "', database, '"."', name, '"') AS drop_db_query
		FROM system.tables
		WHERE engine like 'Replicated%'`,
	)

	names, sqlStatements, _ := s.queryUnzip2Columns(ctx, []string{CreatePodFQDN(host)}, sql)
	return names, sqlStatements, nil
}

// HostDeleteTables
func (s *Schemer) HostDeleteTables(ctx context.Context, host *chop.ChiHost) error {
	tableNames, dropTableSQLs, _ := s.hostGetDropTablesContext(ctx, host)
	log.V(1).M(host).F().Info("Drop tables: %v as %v", tableNames, dropTableSQLs)
	return s.hostApplySQLsContext(ctx, host, dropTableSQLs, false)
}

// HostCreateTables
func (s *Schemer) HostCreateTables(ctx context.Context, host *chop.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	log.V(1).M(host).F().Info("Migrating schema objects to host %s", host.Address.HostName)

	var err1, err2 error

	if names, createSQLs, err := s.getCreateReplicaObjectsContext(ctx, host); err == nil {
		if len(createSQLs) > 0 {
			log.V(1).M(host).F().Info("Creating replica objects at %s: %v", host.Address.HostName, names)
			log.V(1).M(host).F().Info("\n%v", createSQLs)
			err1 = s.hostApplySQLsContext(ctx, host, createSQLs, true)
		}
	}

	if names, createSQLs, err := s.getCreateDistributedObjectsContext(ctx, host); err == nil {
		if len(createSQLs) > 0 {
			log.V(1).M(host).F().Info("Creating distributed objects at %s: %v", host.Address.HostName, names)
			log.V(1).M(host).F().Info("\n%v", createSQLs)
			err2 = s.hostApplySQLsContext(ctx, host, createSQLs, true)
		}
	}

	if err2 != nil {
		return err2
	}
	if err1 != nil {
		return err1
	}

	return nil
}

// IsHostInCluster checks whether host is a member of at least one ClickHouse cluster
func (s *Schemer) IsHostInCluster(ctx context.Context, host *chop.ChiHost) bool {
	sqls := []string{
		heredoc.Docf(
			`SELECT throwIf(count()=0) FROM system.clusters WHERE cluster='%s' AND is_local`,
			allShardsOneReplicaClusterName,
		),
	}
	//TODO: Change to select count() query to avoid exception in operator and ClickHouse logs
	return s.hostApplySQLsContext(ctx, host, sqls, false) == nil
}

// CHIDropDnsCache runs 'DROP DNS CACHE' over the whole CHI
func (s *Schemer) CHIDropDnsCache(ctx context.Context, chi *chop.ClickHouseInstallation) error {
	sqls := []string{
		`SYSTEM DROP DNS CACHE`,
	}
	return s.chiApplySQLsContext(ctx, chi, sqls, false)
}

// chiApplySQLs runs set of SQL queries over the whole CHI
func (s *Schemer) chiApplySQLsContext(ctx context.Context, chi *chop.ClickHouseInstallation, sqls []string, retry bool) error {
	hosts := CreatePodFQDNsOfCHI(chi)
	return s.Cluster.SetHosts(hosts).QueryAll(ctx, sqls, retry)
}

// clusterApplySQLs runs set of SQL queries over the cluster
func (s *Schemer) clusterApplySQLsContext(ctx context.Context, cluster *chop.ChiCluster, sqls []string, retry bool) error {
	hosts := CreatePodFQDNsOfCluster(cluster)
	return s.Cluster.SetHosts(hosts).QueryAll(ctx, sqls, retry)
}

// hostApplySQLs runs set of SQL queries over the replica
func (s *Schemer) hostApplySQLsContext(ctx context.Context, host *chop.ChiHost, sqls []string, retry bool) error {
	hosts := []string{CreatePodFQDN(host)}
	return s.Cluster.SetHosts(hosts).QueryAll(ctx, sqls, retry)
}

// shardApplySQLs runs set of SQL queries over the shard replicas
func (s *Schemer) shardApplySQLsContext(ctx context.Context, shard *chop.ChiShard, sqls []string, retry bool) error {
	hosts := CreatePodFQDNsOfShard(shard)
	return s.Cluster.SetHosts(hosts).QueryAll(ctx, sqls, retry)
}
