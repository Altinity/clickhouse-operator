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

package schemer

import (
	"context"
	"fmt"

	"github.com/MakeNowJust/heredoc"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/chi"
)

const ignoredDBs = `'system', 'information_schema', 'INFORMATION_SCHEMA'`
const createTableDBEngines = `'Ordinary','Atomic','Memory','Lazy'`

// sqlDropTable returns set of 'DROP TABLE ...' SQLs
func (s *ClusterSchemer) sqlDropTable(ctx context.Context, host *api.ChiHost) ([]string, []string, error) {
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

	names, sqlStatements, _ := s.QueryUnzip2Columns(ctx, chi.CreateFQDNs(host, api.ChiHost{}, false), sql)
	return names, sqlStatements, nil
}

// sqlSyncTable returns set of 'SYSTEM SYNC REPLICA database.table ...' SQLs
func (s *ClusterSchemer) sqlSyncTable(ctx context.Context, host *api.ChiHost) ([]string, []string, error) {
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

	names, sqlStatements, _ := s.QueryUnzip2Columns(ctx, chi.CreateFQDNs(host, api.ChiHost{}, false), sql)
	return names, sqlStatements, nil
}

func (s *ClusterSchemer) sqlCreateDatabaseDistributed(cluster string) string {
	var createDatabaseStmt string
	switch {
	case s.version.Matches(">= 22.12"):
		createDatabaseStmt = `'CREATE DATABASE IF NOT EXISTS "' || name || '" Engine = ' || engine_full AS create_db_query`
	default:
		createDatabaseStmt = `'CREATE DATABASE IF NOT EXISTS "' || name || '" Engine = ' || engine      AS create_db_query`
	}

	return heredoc.Docf(`
		SELECT
			DISTINCT name,
			%s
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
		createDatabaseStmt,
		cluster,
		cluster,
	)
}

func (s *ClusterSchemer) sqlCreateTableDistributed(cluster string) string {
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
		ORDER BY order
		`,
		cluster,
		cluster,
		cluster,
	)
}

func (s *ClusterSchemer) sqlCreateDatabaseReplicated(cluster string) string {
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

func (s *ClusterSchemer) sqlCreateTableReplicated(cluster string) string {
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
			has((select groupArray(name) from system.databases where engine in (%s)), database) AND
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

func (s *ClusterSchemer) sqlCreateFunction(cluster string) string {
	return heredoc.Docf(`
		SELECT
			DISTINCT name,
			replaceRegexpOne(create_query, 'CREATE (FUNCTION)', 'CREATE \\1 IF NOT EXISTS')
		FROM
			clusterAllReplicas('%s', system.functions) tables
		WHERE
			create_query != ''
		SETTINGS skip_unavailable_shards=1
		`,
		cluster,
	)
}

func (s *ClusterSchemer) sqlDropReplica(name string) string {
	return fmt.Sprintf("SYSTEM DROP REPLICA '%s'", name)
}

func (s *ClusterSchemer) sqlDropDNSCache() string {
	return `SYSTEM DROP DNS CACHE`
}

func (s *ClusterSchemer) sqlActiveQueriesNum() string {
	return `SELECT count() FROM system.processes`
}

func (s *ClusterSchemer) sqlVersion() string {
	return `SELECT version()`
}

func (s *ClusterSchemer) sqlHostInCluster() string {
	return heredoc.Docf(
		`SELECT throwIf(count()=0) FROM system.clusters WHERE cluster='%s' AND is_local`,
		chi.AllShardsOneReplicaClusterName,
	)
}
