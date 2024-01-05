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

package chi

import (
	"context"
	"fmt"
	r "github.com/altinity/clickhouse-operator/pkg/util/retry"
	"time"

	"github.com/MakeNowJust/heredoc"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// ClusterSchemer specifies cluster schema manager
type ClusterSchemer struct {
	*Cluster
}

const ignoredDBs = `'system', 'information_schema', 'INFORMATION_SCHEMA'`
const createTableDBEngines = `'Ordinary','Atomic','Memory','Lazy'`

// NewClusterSchemer creates new Schemer object
func NewClusterSchemer(clusterConnectionParams *clickhouse.ClusterConnectionParams) *ClusterSchemer {
	return &ClusterSchemer{
		NewCluster().SetClusterConnectionParams(clusterConnectionParams),
	}
}

// shouldCreateDistributedObjects determines whether distributed objects should be created
func shouldCreateDistributedObjects(host *api.ChiHost) bool {
	hosts := CreateFQDNs(host, api.Cluster{}, false)

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

func concatSlices[T any](slices [][]T) []T {
	var totalLen int

	for _, s := range slices {
		totalLen += len(s)
	}

	result := make([]T, totalLen)

	var i int

	for _, s := range slices {
		i += copy(result[i:], s)
	}

	return result
}

// getDistributedObjectsSQLs returns a list of objects that needs to be created on a shard in a cluster.
// That includes all distributed tables, corresponding local tables and databases, if necessary
func (s *ClusterSchemer) getDistributedObjectsSQLs(ctx context.Context, host *api.ChiHost) ([]string, []string, error) {
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
			CreateFQDNs(host, api.ClickHouseInstallation{}, false),
			createDatabaseDistributed(host.Address.ClusterName),
		),
	)
	tableNames, createTableSQLs := debugCreateSQLs(
		s.QueryUnzipAndApplyUUIDs(
			ctx,
			CreateFQDNs(host, api.ClickHouseInstallation{}, false),
			createTableDistributed(host.Address.ClusterName),
		),
	)
	functionNames, createFunctionSQLs := debugCreateSQLs(
		s.QueryUnzip2Columns(
			ctx,
			CreateFQDNs(host, api.ClickHouseInstallation{}, false),
			createFunction(host.Address.ClusterName),
		),
	)
	return concatSlices([][]string{databaseNames, tableNames, functionNames}),
		concatSlices([][]string{createDatabaseSQLs, createTableSQLs, createFunctionSQLs}),
		nil
}

// shouldCreateReplicatedObjects determines whether replicated objects should be created
func shouldCreateReplicatedObjects(host *api.ChiHost) bool {
	shard := CreateFQDNs(host, api.ChiShard{}, false)
	cluster := CreateFQDNs(host, api.Cluster{}, false)

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
func (s *ClusterSchemer) getReplicatedObjectsSQLs(ctx context.Context, host *api.ChiHost) ([]string, []string, error) {
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
			CreateFQDNs(host, api.ClickHouseInstallation{}, false),
			createDatabaseReplicated(host.Address.ClusterName),
		),
	)
	tableNames, createTableSQLs := debugCreateSQLs(
		s.QueryUnzipAndApplyUUIDs(
			ctx,
			CreateFQDNs(host, api.ClickHouseInstallation{}, false),
			createTableReplicated(host.Address.ClusterName),
		),
	)
	functionNames, createFunctionSQLs := debugCreateSQLs(
		s.QueryUnzip2Columns(
			ctx,
			CreateFQDNs(host, api.ClickHouseInstallation{}, false),
			createFunction(host.Address.ClusterName),
		),
	)
	return concatSlices([][]string{databaseNames, tableNames, functionNames}),
		concatSlices([][]string{createDatabaseSQLs, createTableSQLs, createFunctionSQLs}),
		nil
}

// HostSyncTables calls SYSTEM SYNC REPLICA for replicated tables
func (s *ClusterSchemer) HostSyncTables(ctx context.Context, host *api.ChiHost) error {
	tableNames, syncTableSQLs, _ := s.getSyncTablesSQLs(ctx, host)
	log.V(1).M(host).F().Info("Sync tables: %v as %v", tableNames, syncTableSQLs)
	opts := clickhouse.NewQueryOptions()
	opts.SetQueryTimeout(120 * time.Second)
	return s.ExecHost(ctx, host, syncTableSQLs, opts)
}

// HostDropReplica calls SYSTEM DROP REPLICA
func (s *ClusterSchemer) HostDropReplica(ctx context.Context, hostToRunOn, hostToDrop *api.ChiHost) error {
	log.V(1).M(hostToRunOn).F().Info("Drop replica: %v at %v", CreateInstanceHostname(hostToDrop), hostToRunOn.Address.HostName)
	return s.ExecHost(ctx, hostToRunOn, []string{fmt.Sprintf("SYSTEM DROP REPLICA '%s'", CreateInstanceHostname(hostToDrop))})
}

// createTablesSQLs makes all SQL for migrating tables
func (s *ClusterSchemer) createTablesSQLs(
	ctx context.Context,
	host *api.ChiHost,
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
func (s *ClusterSchemer) HostCreateTables(ctx context.Context, host *api.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	log.V(1).M(host).F().S().Info("Migrating schema objects to host %s", host.Address.HostName)
	defer log.V(1).M(host).F().E().Info("Migrating schema objects to host %s", host.Address.HostName)

	replicatedObjectNames,
		replicatedCreateSQLs,
		distributedObjectNames,
		distributedCreateSQLs := s.createTablesSQLs(ctx, host)

	var err1 error
	if len(replicatedCreateSQLs) > 0 {
		log.V(1).M(host).F().Info("Creating replicated objects at %s: %v", host.Address.HostName, replicatedObjectNames)
		log.V(2).M(host).F().Info("\n%v", replicatedCreateSQLs)
		err1 = s.ExecHost(ctx, host, replicatedCreateSQLs, clickhouse.NewQueryOptions().SetRetry(true))
	}

	var err2 error
	if len(distributedCreateSQLs) > 0 {
		log.V(1).M(host).F().Info("Creating distributed objects at %s: %v", host.Address.HostName, distributedObjectNames)
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
func (s *ClusterSchemer) HostDropTables(ctx context.Context, host *api.ChiHost) error {
	tableNames, dropTableSQLs, _ := s.getDropTablesSQLs(ctx, host)
	log.V(1).M(host).F().Info("Drop tables: %v as %v", tableNames, dropTableSQLs)
	return s.ExecHost(ctx, host, dropTableSQLs, clickhouse.NewQueryOptions().SetRetry(false))
}

// IsHostInCluster checks whether host is a member of at least one ClickHouse cluster
func (s *ClusterSchemer) IsHostInCluster(ctx context.Context, host *api.ChiHost) bool {
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
		log.V(1).M(host).F().Info("The host %s is inside the cluster", host.GetName())
		inside = true
	} else {
		log.V(1).M(host).F().Info("The host %s is outside of the cluster", host.GetName())
		inside = false
	}
	return inside
}

// CHIDropDnsCache runs 'DROP DNS CACHE' over the whole CHI
func (s *ClusterSchemer) CHIDropDnsCache(ctx context.Context, chi *api.ClickHouseInstallation) error {
	sql := `SYSTEM DROP DNS CACHE`
	chi.WalkHosts(func(host *api.ChiHost) error {
		return s.ExecHost(ctx, host, []string{sql})
	})
	return nil
}

// HostActiveQueriesNum returns how many active queries are on the host
func (s *ClusterSchemer) HostActiveQueriesNum(ctx context.Context, host *api.ChiHost) (int, error) {
	sql := `SELECT count() FROM system.processes`
	return s.QueryHostInt(ctx, host, sql)
}

func (s *ClusterSchemer) HostDefaultDisk(ctx context.Context, host *api.ChiHost) (string, error) {
	sql := "SELECT name, path FROM system.disks;"
	defaultPath := ""
	err := r.Retry(ctx, 10, "Query DefaultDisk", log.V(1).M(host).F(),
		func() error {
			q, err := s.QueryHost(ctx, host, sql)
			if err != nil {
				return err
			}
			if q == nil {
				return fmt.Errorf("empty query")
			}
			if q.Rows == nil {
				return fmt.Errorf("no rows")
			}
			for q.Rows.Next() {
				var name, path string
				if err := q.Rows.Scan(&name, &path); err != nil {
					log.V(1).F().Error("UNABLE to scan row err: %v", err)
					return err
				}
				if name == "default" {
					defaultPath = path
				}
			}
			defer q.Close()
			return nil
		})
	if err != nil {
		return "", fmt.Errorf("cann not found default disk path")
	}
	return defaultPath, nil
}

func (s *ClusterSchemer) HostGetUsers(ctx context.Context, host *api.ChiHost) ([]string, error) {
	sql := "SELECT name FROM system.users;"
	var users []string
	err := r.Retry(ctx, 10, "Select sql", log.V(1).M(host).F(),
		func() error {
			q, err := s.QueryHost(ctx, host, sql)
			if err != nil {
				return err
			}
			if q == nil {
				return fmt.Errorf("empty query")
			}
			if q.Rows == nil {
				return fmt.Errorf("no rows")
			}
			for q.Rows.Next() {
				var user string
				if err := q.Rows.Scan(&user); err != nil {
					log.V(1).F().Error("UNABLE to scan row err: %v", err)
					return err
				}
				users = append(users, user)
			}
			defer q.Close()
			return nil
		})
	if err != nil {
		return nil, err
	}
	return users, nil
}

// HostClickHouseVersion returns ClickHouse version on the host
func (s *ClusterSchemer) HostClickHouseVersion(ctx context.Context, host *api.ChiHost) (string, error) {
	sql := `SELECT version()`
	return s.QueryHostString(ctx, host, sql)
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
