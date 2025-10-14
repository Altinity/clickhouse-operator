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
	"github.com/altinity/clickhouse-operator/pkg/apis/swversion"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// ClusterSchemer specifies cluster schema manager
type ClusterSchemer struct {
	*Cluster
	interfaces.INameManager
	version *swversion.SoftWareVersion
}

// NewClusterSchemer creates new Schemer object
func NewClusterSchemer(clusterConnectionParams *clickhouse.ClusterConnectionParams, version *swversion.SoftWareVersion) *ClusterSchemer {
	return &ClusterSchemer{
		Cluster:      NewCluster().SetClusterConnectionParams(clusterConnectionParams),
		INameManager: managers.NewNameManager(managers.NameManagerTypeClickHouse),
		version:      version,
	}
}

// HostSyncTables calls SYSTEM SYNC REPLICA for replicated tables
func (s *ClusterSchemer) HostSyncTables(ctx context.Context, host *api.Host) error {
	tableNames, syncTableSQLs, _ := s.sqlSyncTable(ctx, host)
	log.V(1).M(host).F().Info("Sync tables: %v as %v", tableNames, syncTableSQLs)
	opts := clickhouse.NewQueryOptions()
	opts.SetQueryTimeout(120 * time.Second)
	return s.ExecHost(ctx, host, syncTableSQLs, opts)
}

// HostDropReplica calls SYSTEM DROP REPLICA
func (s *ClusterSchemer) HostDropReplica(ctx context.Context, hostToRunOn, hostToDrop *api.Host) error {
	replica := s.Name(interfaces.NameInstanceHostname, hostToDrop)
	shard := hostToRunOn.Runtime.Address.ShardIndex
	log.V(1).M(hostToRunOn).F().Info("Drop replica: %v at %v", replica, hostToRunOn.Runtime.Address.HostName)
	return s.ExecHost(ctx, hostToRunOn, s.sqlDropReplica(shard, replica),
		clickhouse.NewQueryOptions().SetRetry(false).SetLogQueries(true))
}

// createTablesSQLs makes all SQL for migrating tables
func (s *ClusterSchemer) createTablesSQLs(
	ctx context.Context,
	host *api.Host,
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
func (s *ClusterSchemer) HostCreateTables(ctx context.Context, host *api.Host) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("ctx is done")
		return nil
	}

	log.V(1).M(host).F().S().Info("Migrating schema objects to host %s", host.Runtime.Address.HostName)
	defer log.V(1).M(host).F().E().Info("Migrating schema objects to host %s", host.Runtime.Address.HostName)

	replicatedObjectNames,
		replicatedCreateSQLs,
		distributedObjectNames,
		distributedCreateSQLs := s.createTablesSQLs(ctx, host)

	var err1 error
	if len(replicatedCreateSQLs) > 0 {
		log.V(1).M(host).F().Info("Creating replicated objects at %s: %v", host.Runtime.Address.HostName, replicatedObjectNames)
		log.V(2).M(host).F().Info("\n%v", replicatedCreateSQLs)
		err1 = s.ExecHost(ctx, host, replicatedCreateSQLs,
			clickhouse.NewQueryOptions().SetRetry(true).SetLogQueries(true))
	}

	var err2 error
	if len(distributedCreateSQLs) > 0 {
		log.V(1).M(host).F().Info("Creating distributed objects at %s: %v", host.Runtime.Address.HostName, distributedObjectNames)
		log.V(2).M(host).F().Info("\n%v", distributedCreateSQLs)
		err2 = s.ExecHost(ctx, host, distributedCreateSQLs,
			clickhouse.NewQueryOptions().SetRetry(true).SetLogQueries(true))
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
func (s *ClusterSchemer) HostDropTables(ctx context.Context, host *api.Host) error {
	tableNames, dropTableSQLs, _ := s.sqlDropTable(ctx, host)
	log.V(1).M(host).F().Info("Drop tables: %v as %v", tableNames, dropTableSQLs)
	return s.ExecHost(ctx, host, dropTableSQLs,
		clickhouse.NewQueryOptions().SetRetry(false).SetLogQueries(true))
}

// IsHostInCluster checks whether host is a member of at least one ClickHouse cluster
func (s *ClusterSchemer) IsHostInCluster(ctx context.Context, host *api.Host) bool {
	inside := false
	sql := s.sqlHostInCluster(host.Runtime.Address.ClusterName)
	res, err := s.QueryHostString(ctx, host, sql)
	if err == nil && res == "0" {
		log.V(1).M(host).F().Info("The host %s is outside of the cluster", host.GetName())
		inside = false
	} else {
		log.V(1).M(host).F().Info("The host %s is inside the cluster", host.GetName())
		inside = true
	}
	return inside
}

// HostActiveQueriesNum returns how many active queries are on the host
func (s *ClusterSchemer) HostActiveQueriesNum(ctx context.Context, host *api.Host) (int, error) {
	return s.QueryHostInt(ctx, host, s.sqlActiveQueriesNum())
}

// HostClickHouseVersion returns ClickHouse version on the host
func (s *ClusterSchemer) HostClickHouseVersion(ctx context.Context, host *api.Host) (string, error) {
	return s.QueryHostString(ctx, host, s.sqlVersion())
}

// HostMaxReplicaDelay returns max replica delay on the host
func (s *ClusterSchemer) HostMaxReplicaDelay(ctx context.Context, host *api.Host) (int, error) {
	return s.QueryHostInt(ctx, host, s.sqlMaxReplicaDelay())
}

// HostShutdown shutdown a host
func (s *ClusterSchemer) HostShutdown(ctx context.Context, host *api.Host) error {
	log.V(1).M(host).F().Info("Host shutdown: %s", host.GetName())
	return s.ExecHost(ctx, host, s.sqlShutDown(),
		clickhouse.NewQueryOptions().SetRetry(false).SetLogQueries(true))
}

func debugCreateSQLs(names, sqls []string, err error) ([]string, []string) {
	if err != nil {
		log.V(1).Warning("got error: %v", err)
	}
	log.V(2).Info("names:")
	for _, v := range names {
		log.V(2).Info("name: %s", v)
	}
	log.V(2).Info("sql(s):")
	for _, v := range sqls {
		log.V(2).Info("sql: %s", v)
	}
	return names, sqls
}
