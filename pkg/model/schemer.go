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
	"fmt"
	"time"

	"github.com/MakeNowJust/heredoc"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Schemer specifies schema manager
type Schemer struct {
	*Cluster
}

// NewSchemer creates new Schemer object
func NewSchemer(username, password string, port int) *Schemer {
	credentials := &clickhouse.ClusterEndpointCredentials{
		Username: username,
		Password: password,
		Port:     port,
	}
	return &Schemer{
		NewCluster().SetEndpointCredentials(credentials),
	}
}

// getCreateDistributedObjects returns a list of objects that needs to be created on a shard in a cluster
// That includes all distributed tables, corresponding local tables and databases, if necessary
func (s *Schemer) getCreateDistributedObjects(ctx context.Context, host *chop.ChiHost) ([]string, []string, error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil, nil, nil
	}

	hosts := CreateFQDNs(host, chop.ChiCluster{}, false)
	if len(hosts) <= 1 {
		log.V(1).M(host).F().Info("Single host in a cluster. Nothing to create a schema from.")
		return nil, nil, nil
	}

	log.V(1).M(host).F().Info("Extracting distributed table definitions from hosts %v", hosts)

	sqlDBs := heredoc.Docf(`
		SELECT 
			DISTINCT database AS name, 
			concat('CREATE DATABASE IF NOT EXISTS "', name, '"') AS create_db_query
		FROM 
		(
			SELECT DISTINCT arrayJoin([database, extract(engine_full, 'Distributed\\([^,]+, *\'?([^,\']+)\'?, *[^,]+')]) database
			FROM clusterAllReplicas('%s', system.tables) tables
			WHERE engine = 'Distributed'
			SETTINGS skip_unavailable_shards = 1
		)
		`,
		host.Address.ClusterName,
	)
	sqlTables := heredoc.Docf(`
		SELECT DISTINCT 
			concat(database, '.', name) as name, 
			replaceRegexpOne(create_table_query, 'CREATE (TABLE|VIEW|MATERIALIZED VIEW|DICTIONARY)', 'CREATE \\1 IF NOT EXISTS')
		FROM 
		(
			SELECT 
				database,
				name,
				create_table_query,
				2 AS order
			FROM clusterAllReplicas('%s', system.tables) tables
			WHERE engine = 'Distributed'
			SETTINGS skip_unavailable_shards = 1
			UNION ALL
			SELECT 
				extract(engine_full, 'Distributed\\([^,]+, *\'?([^,\']+)\'?, *[^,]+') AS database, 
				extract(engine_full, 'Distributed\\([^,]+, [^,]+, *\'?([^,\\\')]+)') AS name,
				t.create_table_query,
				1 AS order
			FROM clusterAllReplicas('%s', system.tables) tables
			LEFT JOIN 
			(
				SELECT 
					DISTINCT database, 
					name, 
					create_table_query 
				FROM clusterAllReplicas('%s', system.tables)
				SETTINGS skip_unavailable_shards = 1
			) t 
			USING (database, name)
			WHERE engine = 'Distributed' AND t.create_table_query != ''
			SETTINGS skip_unavailable_shards = 1
		) tables
		ORDER BY order
		`,
		host.Address.ClusterName,
		host.Address.ClusterName,
		host.Address.ClusterName,
	)

	log.V(1).M(host).F().Info("fetch dbs list")
	log.V(1).M(host).F().Info("dbs sql\n%v", sqlDBs)
	names1, sqlStatements1, _ := s.QueryUnzip2Columns(ctx, CreateFQDNs(host, chop.ClickHouseInstallation{}, false), sqlDBs)
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
	names2, sqlStatements2, _ := s.QueryUnzip2Columns(ctx, CreateFQDNs(host, chop.ClickHouseInstallation{}, false), sqlTables)
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

// getCreateReplicaObjects returns a list of objects that needs to be created on a host in a cluster
func (s *Schemer) getCreateReplicaObjects(ctx context.Context, host *chop.ChiHost) ([]string, []string, error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil, nil, nil
	}

	replicas := CreateFQDNs(host, chop.ChiShard{}, false)
	if len(replicas) <= 1 {
		log.V(1).M(host).F().Info("Single replica in a shard. Nothing to create a schema from.")
		return nil, nil, nil
	}
	log.V(1).M(host).F().Info("Extracting replicated table definitions from %v", replicas)

	sqlDBs := heredoc.Docf(`
		SELECT 
			DISTINCT database AS name, 
			concat('CREATE DATABASE IF NOT EXISTS "', name, '"') AS create_db_query
		FROM clusterAllReplicas('%s', system.tables) tables
		WHERE database != 'system'
		SETTINGS skip_unavailable_shards = 1
		`,
		host.Address.ClusterName,
	)
	sqlTables := heredoc.Docf(`
		SELECT 
			DISTINCT name, 
			replaceRegexpOne(create_table_query, 'CREATE (TABLE|VIEW|MATERIALIZED VIEW|DICTIONARY)', 'CREATE \\1 IF NOT EXISTS')
		FROM clusterAllReplicas('%s', system.tables) tables
		WHERE database != 'system' AND create_table_query != '' AND name NOT LIKE '.inner.%%'
		SETTINGS skip_unavailable_shards = 1
		`,
		host.Address.ClusterName,
	)

	names1, sqlStatements1, _ := s.QueryUnzip2Columns(ctx, CreateFQDNs(host, chop.ClickHouseInstallation{}, false), sqlDBs)
	names2, sqlStatements2, _ := s.QueryUnzip2Columns(ctx, CreateFQDNs(host, chop.ClickHouseInstallation{}, false), sqlTables)
	return append(names1, names2...), append(sqlStatements1, sqlStatements2...), nil
}

// hostGetDropTables returns set of 'DROP TABLE ...' SQLs
func (s *Schemer) hostGetDropTables(ctx context.Context, host *chop.ChiHost) ([]string, []string, error) {
	// There isn't a separate query for deleting views. To delete a view, use DROP TABLE
	// See https://clickhouse.yandex/docs/en/query_language/create/
	sql := heredoc.Doc(`
		SELECT
			DISTINCT name, 
			concat('DROP TABLE IF EXISTS "', database, '"."', name, '"') AS drop_table_query
		FROM system.tables
		WHERE engine LIKE 'Replicated%'`,
	)

	names, sqlStatements, _ := s.QueryUnzip2Columns(ctx, CreateFQDNs(host, chop.ChiHost{}, false), sql)
	return names, sqlStatements, nil
}

// hostGetSyncTables returns set of 'SYSTEM SYNC REPLICA database.table ...' SQLs
func (s *Schemer) hostGetSyncTables(ctx context.Context, host *chop.ChiHost) ([]string, []string, error) {
	sql := heredoc.Doc(`
		SELECT
			DISTINCT name, 
			concat('SYSTEM SYNC REPLICA "', database, '"."', name, '"') AS sync_table_query
		FROM system.tables
		WHERE engine LIKE 'Replicated%'`,
	)

	names, sqlStatements, _ := s.QueryUnzip2Columns(ctx, CreateFQDNs(host, chop.ChiHost{}, false), sql)
	return names, sqlStatements, nil
}

// HostSyncTables calls SYSTEM SYNC REPLICA for replicated tables
func (s *Schemer) HostSyncTables(ctx context.Context, host *chop.ChiHost) error {
	tableNames, syncTableSQLs, _ := s.hostGetSyncTables(ctx, host)
	log.V(1).M(host).F().Info("Sync tables: %v as %v", tableNames, syncTableSQLs)
	opts := clickhouse.NewQueryOptions()
	opts.SetQueryTimeout(120 * time.Second)
	return s.ExecHost(ctx, host, syncTableSQLs, opts)
}

// HostDropReplica calls SYSTEM DROP REPLICA
func (s *Schemer) HostDropReplica(ctx context.Context, hostToRun, hostToDrop *chop.ChiHost) error {
	log.V(1).M(hostToRun).F().Info("Drop replica: %v", CreateReplicaHostname(hostToDrop))
	return s.ExecHost(ctx, hostToRun, []string{fmt.Sprintf("SYSTEM DROP REPLICA '%s'", CreateReplicaHostname(hostToDrop))})
}

// HostCreateTables creates tables on a new host
func (s *Schemer) HostCreateTables(ctx context.Context, host *chop.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	log.V(1).M(host).F().Info("Migrating schema objects to host %s", host.Address.HostName)

	var err1, err2 error

	if names, createSQLs, err := s.getCreateReplicaObjects(ctx, host); err == nil {
		if len(createSQLs) > 0 {
			log.V(1).M(host).F().Info("Creating replica objects at %s: %v", host.Address.HostName, names)
			log.V(1).M(host).F().Info("\n%v", createSQLs)
			err1 = s.ExecHost(ctx, host, createSQLs, clickhouse.NewQueryOptions().SetRetry(true))
		}
	}

	if names, createSQLs, err := s.getCreateDistributedObjects(ctx, host); err == nil {
		if len(createSQLs) > 0 {
			log.V(1).M(host).F().Info("Creating distributed objects at %s: %v", host.Address.HostName, names)
			log.V(1).M(host).F().Info("\n%v", createSQLs)
			err2 = s.ExecHost(ctx, host, createSQLs, clickhouse.NewQueryOptions().SetRetry(true))
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

// HostDropTables drops tables on a host
func (s *Schemer) HostDropTables(ctx context.Context, host *chop.ChiHost) error {
	tableNames, dropTableSQLs, _ := s.hostGetDropTables(ctx, host)
	log.V(1).M(host).F().Info("Drop tables: %v as %v", tableNames, dropTableSQLs)
	return s.ExecHost(ctx, host, dropTableSQLs)
}

// IsHostInCluster checks whether host is a member of at least one ClickHouse cluster
func (s *Schemer) IsHostInCluster(ctx context.Context, host *chop.ChiHost) bool {
	inside := false
	SQLs := []string{
		heredoc.Docf(
			`SELECT throwIf(count()=0) FROM system.clusters WHERE cluster='%s' AND is_local`,
			allShardsOneReplicaClusterName,
		),
	}
	//TODO: Change to select count() query to avoid exception in operator and ClickHouse logs
	opts := clickhouse.NewQueryOptions().SetSilent(true)
	//opts := clickhouse.NewQueryOptions()
	err := s.ExecHost(ctx, host, SQLs, opts)
	if err == nil {
		log.V(1).M(host).F().Info("Host inside the cluster")
		inside = true
	} else {
		log.V(1).M(host).F().Info("Host outside of the cluster")
		inside = false
	}
	return inside
}

// CHIDropDnsCache runs 'DROP DNS CACHE' over the whole CHI
func (s *Schemer) CHIDropDnsCache(ctx context.Context, chi *chop.ClickHouseInstallation) error {
	SQLs := []string{
		`SYSTEM DROP DNS CACHE`,
	}
	return s.ExecCHI(ctx, chi, SQLs)
}

// HostActiveQueriesNum returns how many active queries are on the host
func (s *Schemer) HostActiveQueriesNum(ctx context.Context, host *chop.ChiHost) (int, error) {
	sql := `SELECT count() FROM system.processes`
	return s.QueryHostInt(ctx, host, sql)
}
