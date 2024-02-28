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
	"time"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/chi"
	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// ClusterSchemer specifies cluster schema manager
type ClusterSchemer struct {
	*chi.Cluster
	version *api.CHVersion
}

// NewClusterSchemer creates new Schemer object
func NewClusterSchemer(clusterConnectionParams *clickhouse.ClusterConnectionParams, version *api.CHVersion) *ClusterSchemer {
	return &ClusterSchemer{
		Cluster: chi.NewCluster().SetClusterConnectionParams(clusterConnectionParams),
		version: version,
	}
}

// HostSyncTables calls SYSTEM SYNC REPLICA for replicated tables
func (s *ClusterSchemer) HostSyncTables(ctx context.Context, host *api.ChiHost) error {
	tableNames, syncTableSQLs, _ := s.sqlSyncTable(ctx, host)
	log.V(1).M(host).F().Info("Sync tables: %v as %v", tableNames, syncTableSQLs)
	opts := clickhouse.NewQueryOptions()
	opts.SetQueryTimeout(120 * time.Second)
	return s.ExecHost(ctx, host, syncTableSQLs, opts)
}

// HostDropReplica calls SYSTEM DROP REPLICA
func (s *ClusterSchemer) HostDropReplica(ctx context.Context, hostToRunOn, hostToDrop *api.ChiHost) error {
	replica := chi.CreateInstanceHostname(hostToDrop)
	shard := hostToRunOn.Address.ShardIndex
	log.V(1).M(hostToRunOn).F().Info("Drop replica: %v at %v", replica, hostToRunOn.Address.HostName)
	return s.ExecHost(ctx, hostToRunOn, s.sqlDropReplica(shard, replica), clickhouse.NewQueryOptions().SetRetry(false))
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
	tableNames, dropTableSQLs, _ := s.sqlDropTable(ctx, host)
	log.V(1).M(host).F().Info("Drop tables: %v as %v", tableNames, dropTableSQLs)
	return s.ExecHost(ctx, host, dropTableSQLs, clickhouse.NewQueryOptions().SetRetry(false))
}

// IsHostInCluster checks whether host is a member of at least one ClickHouse cluster
func (s *ClusterSchemer) IsHostInCluster(ctx context.Context, host *api.ChiHost) bool {
	inside := false
	SQLs := []string{s.sqlHostInCluster()}
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
	chi.WalkHosts(func(host *api.ChiHost) error {
		return s.ExecHost(ctx, host, []string{s.sqlDropDNSCache()})
	})
	return nil
}

// HostActiveQueriesNum returns how many active queries are on the host
func (s *ClusterSchemer) HostActiveQueriesNum(ctx context.Context, host *api.ChiHost) (int, error) {
	return s.QueryHostInt(ctx, host, s.sqlActiveQueriesNum())
}

// HostClickHouseVersion returns ClickHouse version on the host
func (s *ClusterSchemer) HostClickHouseVersion(ctx context.Context, host *api.ChiHost) (string, error) {
	return s.QueryHostString(ctx, host, s.sqlVersion())
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
