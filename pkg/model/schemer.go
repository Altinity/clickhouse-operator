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

const ignoredDBs = `'system', 'information_schema', 'INFORMATION_SCHEMA'`
const createTableDBEngines = `'Ordinary','Atomic','Memory','Lazy'`

// NewSchemer creates new Schemer object
func NewSchemer(scheme, username, password, rootCA string, port int) *Schemer {
	return &Schemer{
		NewCluster().SetClusterConnectionParams(
			clickhouse.NewClusterConnectionParams(scheme, username, password, rootCA, port),
		),
	}
}

// shouldCreateDistributedObjects determines whether distributed objects should be created
func shouldCreateDistributedObjects(host *chop.ChiHost) bool {
	hosts := CreateFQDNs(host, chop.Cluster{}, false)

	if host.GetCluster().SchemaPolicy.Shard == SchemaPolicyShardNone {
		log.V(1).M(host).F().Info("SchemaPolicy.Shard says there is no need to distribute objects")
		return false
	}
	if len(hosts) <= 1 {
		log.V(1).M(host).F().Info("Nothing to create a schema from - single host in the cluster: %v", hosts)
		return false
	}

	log.V(1).M(host).F().Info("Should create distributed objects in the cluster: %v", hosts)
	return true
}

// getDistributedObjectsSQLs returns a list of objects that needs to be created on a shard in a cluster.
// That includes all distributed tables, corresponding local tables and databases, if necessary
func (s *Schemer) getDistributedObjectsSQLs(ctx context.Context, host *chop.ChiHost) ([]string, []string, error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil, nil, nil
	}

	if !shouldCreateDistributedObjects(host) {
		log.V(1).M(host).F().Info("Should not create distributed objects")
		return nil, nil, nil
	}

	databaseNames, createDatabaseSQLs := debugCreateSQLs(
		s.QueryUnzip2Columns(
			ctx,
			CreateFQDNs(host, chop.ClickHouseInstallation{}, false),
			createDatabaseDistributed(host.Address.ClusterName),
		),
	)
	tableNames, createTableSQLs := debugCreateSQLs(
		s.QueryUnzipAndApplyUUIDs(
			ctx,
			CreateFQDNs(host, chop.ClickHouseInstallation{}, false),
			createTableDistributed(host.Address.ClusterName),
		),
	)
	return append(databaseNames, tableNames...), append(createDatabaseSQLs, createTableSQLs...), nil
}

// shouldCreateReplicatedObjects determines whether replicated objects should be created
func shouldCreateReplicatedObjects(host *chop.ChiHost) bool {
	shard := CreateFQDNs(host, chop.ChiShard{}, false)
	cluster := CreateFQDNs(host, chop.Cluster{}, false)

	if host.GetCluster().SchemaPolicy.Shard == SchemaPolicyShardAll {
		// We have explicit request to create replicated objects on each shard
		// However, it is reasonable to have at least two instances in a cluster
		if len(cluster) >= 2 {
			log.V(1).M(host).F().Info("SchemaPolicy.Shard says we need replicated objects. Should create replicated objects for the shard: %v", shard)
			return true
		}
	}

	if host.GetCluster().SchemaPolicy.Replica == SchemaPolicyReplicaNone {
		log.V(1).M(host).F().Info("SchemaPolicy.Replica says there is no need to replicate objects")
		return false
	}

	if len(shard) <= 1 {
		log.V(1).M(host).F().Info("Single replica in a shard. Nothing to create a schema from.")
		return false
	}

	log.V(1).M(host).F().Info("Should create replicated objects for the shard: %v", shard)
	return true
}

// getReplicatedObjectsSQLs returns a list of objects that needs to be created on a host in a cluster
func (s *Schemer) getReplicatedObjectsSQLs(ctx context.Context, host *chop.ChiHost) ([]string, []string, error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil, nil, nil
	}

	if !shouldCreateReplicatedObjects(host) {
		log.V(1).M(host).F().Info("Should not create replicated objects")
		return nil, nil, nil
	}

	databaseNames, createDatabaseSQLs := debugCreateSQLs(
		s.QueryUnzip2Columns(
			ctx,
			CreateFQDNs(host, chop.ClickHouseInstallation{}, false),
			createDatabaseReplicated(host.Address.ClusterName),
		),
	)
	tableNames, createTableSQLs := debugCreateSQLs(
		s.QueryUnzipAndApplyUUIDs(
			ctx,
			CreateFQDNs(host, chop.ClickHouseInstallation{}, false),
			createTableReplicated(host.Address.ClusterName),
		),
	)
	return append(databaseNames, tableNames...), append(createDatabaseSQLs, createTableSQLs...), nil
}

// getDropTablesSQLs returns set of 'DROP TABLE ...' SQLs
func (s *Schemer) getDropTablesSQLs(ctx context.Context, host *chop.ChiHost) ([]string, []string, error) {
	// There isn't a separate query for deleting views. To delete a view, use DROP TABLE
	// See https://clickhouse.yandex/docs/en/query_language/create/
	sql := heredoc.Docf(`
	    SELECT
	        DISTINCT name,
	        concat('DROP DICTIONARY IF EXISTS "', database, '"."', name, '"') AS drop_table_query
	    FROM
	        system.dictionaries
	    WHERE database != ''
	    UNION ALL
		SELECT
			DISTINCT name,
			concat('DROP TABLE IF EXISTS "', database, '"."', name, '"') AS drop_table_query
		FROM
			system.tables
		WHERE
			database NOT IN (%s) AND
			(engine like 'Replicated%%' OR engine like '%%View%%')
		`,
		ignoredDBs,
	)

	names, sqlStatements, _ := s.QueryUnzip2Columns(ctx, CreateFQDNs(host, chop.ChiHost{}, false), sql)
	return names, sqlStatements, nil
}

// getSyncTablesSQLs returns set of 'SYSTEM SYNC REPLICA database.table ...' SQLs
func (s *Schemer) getSyncTablesSQLs(ctx context.Context, host *chop.ChiHost) ([]string, []string, error) {
	sql := heredoc.Doc(`
		SELECT
			DISTINCT name,
			concat('SYSTEM SYNC REPLICA "', database, '"."', name, '"') AS sync_table_query
		FROM
			system.tables
		WHERE
			engine LIKE 'Replicated%'
		`,
	)

	names, sqlStatements, _ := s.QueryUnzip2Columns(ctx, CreateFQDNs(host, chop.ChiHost{}, false), sql)
	return names, sqlStatements, nil
}

// HostSyncTables calls SYSTEM SYNC REPLICA for replicated tables
func (s *Schemer) HostSyncTables(ctx context.Context, host *chop.ChiHost) error {
	tableNames, syncTableSQLs, _ := s.getSyncTablesSQLs(ctx, host)
	log.V(1).M(host).F().Info("Sync tables: %v as %v", tableNames, syncTableSQLs)
	opts := clickhouse.NewQueryOptions()
	opts.SetQueryTimeout(120 * time.Second)
	return s.ExecHost(ctx, host, syncTableSQLs, opts)
}

// HostDropReplica calls SYSTEM DROP REPLICA
func (s *Schemer) HostDropReplica(ctx context.Context, hostToRun, hostToDrop *chop.ChiHost) error {
	log.V(1).M(hostToRun).F().Info("Drop replica: %v at %v", CreateInstanceHostname(hostToDrop), hostToRun.Address.HostName)
	return s.ExecHost(ctx, hostToRun, []string{fmt.Sprintf("SYSTEM DROP REPLICA '%s'", CreateInstanceHostname(hostToDrop))})
}

// createTablesSQLs makes all SQL for migrating tables
func (s *Schemer) createTablesSQLs(
	ctx context.Context,
	host *chop.ChiHost,
) (
	replicatedObjectNames []string,
	replicatedCreateSQLs []string,
	distributedObjectNames []string,
	distributedCreateSQLs []string,
) {
	if names, sql, err := s.getReplicatedObjectsSQLs(ctx, host); err == nil {
		replicatedObjectNames = names
		replicatedCreateSQLs = sql
	}
	if names, sql, err := s.getDistributedObjectsSQLs(ctx, host); err == nil {
		distributedObjectNames = names
		distributedCreateSQLs = sql
	}
	return
}

// HostCreateTables creates tables on a new host
func (s *Schemer) HostCreateTables(ctx context.Context, host *chop.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	log.V(1).M(host).F().Info("Migrating schema objects to host %s", host.Address.HostName)

	replicatedObjectNames,
		replicatedCreateSQLs,
		distributedObjectNames,
		distributedCreateSQLs := s.createTablesSQLs(ctx, host)

	var err1 error
	if len(replicatedCreateSQLs) > 0 {
		log.V(2).M(host).F().Info("Creating replica objects at %s: %v", host.Address.HostName, replicatedObjectNames)
		log.V(2).M(host).F().Info("\n%v", replicatedCreateSQLs)
		err1 = s.ExecHost(ctx, host, replicatedCreateSQLs, clickhouse.NewQueryOptions().SetRetry(true))
	}

	var err2 error
	if len(distributedCreateSQLs) > 0 {
		log.V(2).M(host).F().Info("Creating distributed objects at %s: %v", host.Address.HostName, distributedObjectNames)
		log.V(2).M(host).F().Info("\n%v", distributedCreateSQLs)
		err2 = s.ExecHost(ctx, host, distributedCreateSQLs, clickhouse.NewQueryOptions().SetRetry(true))
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
	tableNames, dropTableSQLs, _ := s.getDropTablesSQLs(ctx, host)
	log.V(1).M(host).F().Info("Drop tables: %v as %v", tableNames, dropTableSQLs)
	return s.ExecHost(ctx, host, dropTableSQLs, clickhouse.NewQueryOptions().SetRetry(true))
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
		log.V(1).M(host).F().Info("The host is inside the cluster")
		inside = true
	} else {
		log.V(1).M(host).F().Info("The host is outside of the cluster")
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

// HostVersion returns ClickHouse version on the host
func (s *Schemer) HostVersion(ctx context.Context, host *chop.ChiHost) (string, error) {
	sql := `SELECT version()`
	return s.QueryHostString(ctx, host, sql)
}

func createDatabaseDistributed(cluster string) string {
	return heredoc.Docf(`
		SELECT
			DISTINCT name,
			'CREATE DATABASE IF NOT EXISTS "' || name || '" Engine = ' || engine AS create_db_query
		FROM (
			SELECT
				*
			FROM
				clusterAllReplicas('%s', system.databases) databases
			SETTINGS skip_unavailable_shards = 1
		)
		WHERE name IN (
			SELECT
				DISTINCT arrayJoin([database, extract(engine_full, 'Distributed\\([^,]+, *\'?([^,\']+)\'?, *[^,]+')]) database
			FROM
				clusterAllReplicas('%s', system.tables) tables
			WHERE
				engine = 'Distributed'
			SETTINGS skip_unavailable_shards = 1
		)
		`,
		cluster,
		cluster,
	)
}

func createTableDistributed(cluster string) string {
	return heredoc.Docf(`
		SELECT
			DISTINCT concat(database, '.', name) AS name,
			replaceRegexpOne(create_table_query, 'CREATE (TABLE|VIEW|MATERIALIZED VIEW|DICTIONARY)', 'CREATE \\1 IF NOT EXISTS'),
			extract(create_table_query, 'UUID \'([^\(\']*)') as uuid,
			extract(create_table_query, 'INNER UUID \'([^\(\']*)') as inner_uuid
		FROM
		(
			SELECT
				database,
				name,
				create_table_query,
				2 AS order
			FROM
				clusterAllReplicas('%s', system.tables) tables
			WHERE
				engine = 'Distributed'
			SETTINGS skip_unavailable_shards = 1
			UNION ALL
			SELECT
				extract(engine_full, 'Distributed\\([^,]+, *\'?([^,\']+)\'?, *[^,]+') AS database,
				extract(engine_full, 'Distributed\\([^,]+, [^,]+, *\'?([^,\\\')]+)') AS name,
				t.create_table_query,
				1 AS order
			FROM
				clusterAllReplicas('%s', system.tables) tables
				LEFT JOIN
				(
					SELECT
						DISTINCT database,
						name,
						create_table_query
					FROM
						clusterAllReplicas('%s', system.tables)
					SETTINGS skip_unavailable_shards = 1, show_table_uuid_in_table_create_query_if_not_nil=1
				) t
				USING (database, name)
			WHERE
				engine = 'Distributed' AND t.create_table_query != ''
			SETTINGS skip_unavailable_shards = 1
		) tables
		WHERE database IN (select name from system.databases where engine in (%s))
		ORDER BY order
		`,
		cluster,
		cluster,
		cluster,
		createTableDBEngines,
	)
}

func createDatabaseReplicated(cluster string) string {
	return heredoc.Docf(`
		SELECT
			DISTINCT name,
			'CREATE DATABASE IF NOT EXISTS "' || name || '" Engine = ' || engine  AS create_db_query
		FROM
			clusterAllReplicas('%s', system.databases) databases
		WHERE
			name NOT IN (%s)
		SETTINGS skip_unavailable_shards = 1
		`,
		cluster,
		ignoredDBs,
	)
}

func createTableReplicated(cluster string) string {
	return heredoc.Docf(`
		SELECT
			DISTINCT name,
			replaceRegexpOne(create_table_query, 'CREATE (TABLE|VIEW|MATERIALIZED VIEW|DICTIONARY|LIVE VIEW|WINDOW VIEW)', 'CREATE \\1 IF NOT EXISTS'),
			extract(create_table_query, 'UUID \'([^\(\']*)') as uuid,
			extract(create_table_query, 'INNER UUID \'([^\(\']*)') as inner_uuid
		FROM
			clusterAllReplicas('%s', system.tables) tables
		WHERE
			database NOT IN (%s) AND
			database IN (select name from system.databases where engine in (%s)) AND
			create_table_query != '' AND
			name NOT LIKE '.inner.%%' AND
			name NOT LIKE '.inner_id.%%'
		SETTINGS skip_unavailable_shards=1, show_table_uuid_in_table_create_query_if_not_nil=1
		`,
		cluster,
		ignoredDBs,
		createTableDBEngines,
	)
}

func debugCreateSQLs(names, sqls []string, err error) ([]string, []string) {
	if err != nil {
		log.V(1).Warning("got error: %v", err)
	}
	log.V(2).Info("names:")
	for _, v := range names {
		log.V(2).Info("name: %s", v)
	}
	log.V(2).Info("sqls:")
	for _, v := range sqls {
		log.V(2).Info("sql: %s", v)
	}
	return names, sqls
}
