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
	"errors"
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

// HostIsActiveReplica checks whether host is an active replica
func (s *ClusterSchemer) IsHostActiveReplica(ctx context.Context, hostToRunOn, hostToCheck *api.Host) bool {
	replica := s.Name(interfaces.NameInstanceHostname, hostToCheck)
	log.V(1).M(hostToRunOn).F().Info("Check active replica: %v at %v", replica, hostToRunOn.Runtime.Address.HostName)
	active := false
	res, err := s.QueryHostString(ctx, hostToRunOn, s.sqlIsReplicaActive(replica))
	if err == nil && res == "0" {
		log.V(1).M(hostToRunOn).F().Info("The host %s is not active", hostToCheck.GetName())
		active = false
	} else {
		log.V(1).M(hostToRunOn).F().Info("The host %s is active", hostToCheck.GetName())
		active = true
	}
	return active
}

// ZKReplica is a replica of a replicated table in (Zoo)Keeper
type ZKReplica struct {
	// Path is the table path in (Zoo)Keeper, as system.replicas.zookeeper_path reports it
	Path string
	// Name is the replica name of the table, as system.replicas.replica_name reports it
	Name string
}

// HostZKReplicas lists (Zoo)Keeper replicas of replicated tables of the host
func (s *ClusterSchemer) HostZKReplicas(ctx context.Context, host *api.Host) ([]ZKReplica, error) {
	paths, names, err := s.QueryUnzip2Columns(ctx, s.Names(interfaces.NameFQDNs, host, api.Host{}, false), s.sqlZKReplicas())
	if err != nil {
		return nil, err
	}
	var replicas []ZKReplica
	for i := range paths {
		if i < len(names) {
			replicas = append(replicas, ZKReplica{Path: paths[i], Name: names[i]})
		}
	}
	return replicas, nil
}

// HostDropReplica calls SYSTEM DROP REPLICA for hostToDrop on hostToRunOn.
// zkReplicas are replicas of hostToDrop collected while it was alive. Plain SYSTEM DROP REPLICA covers only tables
// which hostToRunOn has itself, so a replica of a table hostToRunOn does not have - all tables of a removed shard,
// when hostToRunOn is a host of another shard - is dropped by its (Zoo)Keeper path.
func (s *ClusterSchemer) HostDropReplica(ctx context.Context, hostToRunOn, hostToDrop *api.Host, zkReplicas []ZKReplica) error {
	replica := s.Name(interfaces.NameInstanceHostname, hostToDrop)
	// Replicated databases register a replica as '{shard}|{replica}', and the {shard} macro is the shard name
	// of the host to drop, not of the host the statement runs on
	shard := hostToDrop.Runtime.Address.ShardName
	var foreign []ZKReplica
	if len(zkReplicas) > 0 {
		// SYSTEM DROP REPLICA ... FROM ZKPATH refuses a path of a local table
		local, _ := s.HostZKReplicas(ctx, hostToRunOn)
		foreign = foreignZKReplicas(zkReplicas, local)
	}
	log.V(1).M(hostToRunOn).F().Info("Drop replica: %v at %v, by (Zoo)Keeper path: %v", replica, hostToRunOn.Runtime.Address.HostName, foreign)
	return s.ExecHost(ctx, hostToRunOn, s.sqlDropReplica(shard, replica, foreign),
		clickhouse.NewQueryOptions().SetRetry(false).SetLogQueries(true))
}

// foreignZKReplicas filters out replicas of tables which are local - have the same (Zoo)Keeper path
func foreignZKReplicas(replicas, local []ZKReplica) (res []ZKReplica) {
	localPaths := make(map[string]bool, len(local))
	for _, r := range local {
		localPaths[r.Path] = true
	}
	for _, r := range replicas {
		if !localPaths[r.Path] {
			res = append(res, r)
		}
	}
	return res
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

// HostClusterDoesNotExistErrorCount returns how many CLUSTER_DOESNT_EXIST errors the host has
// recorded (issue #2013 signal - see sqlClusterDoesNotExistErrorCount).
func (s *ClusterSchemer) HostClusterDoesNotExistErrorCount(ctx context.Context, host *api.Host) (int, error) {
	return s.QueryHostInt(ctx, host, s.sqlClusterDoesNotExistErrorCount())
}

// HostSettingsChangeableWithoutRestart returns system.server_settings names ClickHouse applies
// without a restart. supported is false when system.server_settings has no changeable_without_restart
// column; the settings query is not sent then. Any query error is returned as-is so the caller can
// fall back to configurationRestartPolicy.
func (s *ClusterSchemer) HostSettingsChangeableWithoutRestart(ctx context.Context, host *api.Host) (names []string, supported bool, err error) {
	count, err := s.QueryHostInt(ctx, host, s.sqlSettingsChangeableWithoutRestartColumn())
	if err != nil || count == 0 {
		return nil, false, err
	}
	query, err := s.QueryHost(ctx, host, s.sqlSettingsChangeableWithoutRestart())
	if query != nil {
		defer query.Close()
	}
	if err != nil {
		return nil, true, err
	}
	if query == nil {
		return nil, true, errors.New("no result")
	}
	if err := query.UnzipColumnsAsStrings(&names); err != nil {
		return nil, true, err
	}
	return names, true, nil
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
