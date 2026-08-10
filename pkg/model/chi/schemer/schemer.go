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
	"fmt"
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

type replicatedTable struct {
	DatabaseName string
	TableName    string
}

// ErrGateDeadline marks the shared sync-gate deadline being reached.
var ErrGateDeadline = errors.New("sync gate deadline exceeded")

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

// HostClusterDoesNotExistErrorCount returns how many CLUSTER_DOESNT_EXIST errors the host has
// recorded (issue #2013 signal - see sqlClusterDoesNotExistErrorCount).
func (s *ClusterSchemer) HostClusterDoesNotExistErrorCount(ctx context.Context, host *api.Host) (int, error) {
	return s.QueryHostInt(ctx, host, s.sqlClusterDoesNotExistErrorCount())
}

// HostClickHouseVersion returns ClickHouse version on the host
func (s *ClusterSchemer) HostClickHouseVersion(ctx context.Context, host *api.Host) (string, error) {
	return s.QueryHostString(ctx, host, s.sqlVersion())
}

// HostMaxReplicaDelay returns max replica delay on the host
func (s *ClusterSchemer) HostMaxReplicaDelay(ctx context.Context, host *api.Host) (int, error) {
	replicaDelay, err := s.QueryHostInt(ctx, host, s.sqlMaxReplicaDelay())
	if contextError := ctx.Err(); contextError != nil {
		return 0, contextError
	}
	return replicaDelay, err
}

func (s *ClusterSchemer) HostMaxIsReadonly(ctx context.Context, host *api.Host) (int, error) {
	readonly, err := s.QueryHostInt(ctx, host, s.sqlReplicaHealth("is_readonly"))
	if contextError := ctx.Err(); contextError != nil {
		return 0, contextError
	}
	return readonly, err
}

func (s *ClusterSchemer) HostMaxIsSessionExpired(ctx context.Context, host *api.Host) (int, error) {
	sessionExpired, err := s.QueryHostInt(ctx, host, s.sqlReplicaHealth("is_session_expired"))
	if contextError := ctx.Err(); contextError != nil {
		return 0, contextError
	}
	return sessionExpired, err
}

func (s *ClusterSchemer) PeerReplicatedObjectCount(ctx context.Context, host *api.Host, deadline time.Time) (int, error) {
	databaseNames, replicatedTables, err := s.peerReplicatedObjects(ctx, host, deadline)
	if err != nil {
		return 0, err
	}
	return len(databaseNames) + len(replicatedTables), nil
}

func (s *ClusterSchemer) HostAsyncLoadBarrier(ctx context.Context, host *api.Host, deadline time.Time) error {
	for {
		asyncLoaderExists, err := s.queryHostIntWithDeadline(ctx, host, deadline, s.sqlAsyncLoaderTableExists())
		if err != nil {
			return err
		}
		if asyncLoaderExists == 0 {
			return nil
		}

		pendingLoadJobs, failedLoadJobs, err := s.queryHostIntPairWithDeadline(ctx, host, deadline, s.sqlAsyncLoaderState())
		if err != nil {
			return err
		}
		if failedLoadJobs > 0 {
			failedLoadJob, detailErr := s.queryHostStringWithDeadline(ctx, host, deadline, s.sqlAsyncLoaderFailedDetails())
			if detailErr != nil {
				return detailErr
			}
			return fmt.Errorf("async loader failed or canceled job: %s", failedLoadJob)
		}
		if pendingLoadJobs == 0 {
			return nil
		}
		if err := waitForNextGatePoll(ctx, deadline); err != nil {
			return err
		}
	}
}

func (s *ClusterSchemer) HostSyncReplicatedObjects(ctx context.Context, host *api.Host, deadline time.Time) error {
	// LIGHTWEIGHT is available since 23.4 only. When the version is unknown (digest-pinned
	// or non-numeric image tag) or older, fall back to plain SYSTEM SYNC REPLICA rather than
	// failing the reconcile - the gate must never be harder to pass than the plain wait it replaces.
	lightweight := s.version.Matches(">= 23.4")
	if !lightweight {
		log.V(1).M(host).F().Info("SYSTEM SYNC REPLICA LIGHTWEIGHT is unavailable for version %s - falling back to full SYNC REPLICA", s.version)
	}

	if err := s.HostAsyncLoadBarrier(ctx, host, deadline); err != nil {
		return err
	}

	databaseNames, _, err := s.peerReplicatedObjects(ctx, host, deadline)
	if err != nil {
		return err
	}
	for _, databaseName := range databaseNames {
		if err := s.execHostWithDeadline(ctx, host, deadline, s.sqlSyncDatabaseReplica(databaseName)); err != nil {
			return err
		}
	}

	if err := s.HostAsyncLoadBarrier(ctx, host, deadline); err != nil {
		return err
	}

	_, replicatedTables, err := s.peerReplicatedObjects(ctx, host, deadline)
	if err != nil {
		return err
	}
	for _, replicatedTable := range replicatedTables {
		if err := s.execHostWithDeadline(ctx, host, deadline, s.sqlWaitLoadingParts(replicatedTable.DatabaseName, replicatedTable.TableName)); err != nil {
			return err
		}
		if err := s.execHostWithDeadline(ctx, host, deadline, s.sqlSyncReplica(replicatedTable.DatabaseName, replicatedTable.TableName, lightweight)); err != nil {
			return err
		}
	}
	return nil
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

func (s *ClusterSchemer) peerReplicatedObjects(ctx context.Context, host *api.Host, deadline time.Time) ([]string, []replicatedTable, error) {
	if _, err := gateRemaining(ctx, deadline); err != nil {
		return nil, nil, err
	}

	// Replication is a per-shard property - discover replicated objects from the shard peers only.
	// A cluster-wide scan would drag tables that live on other shards into this host's catch-up.
	peers := s.Names(interfaces.NameFQDNs, host, api.ChiShard{}, true)
	if len(peers) == 0 {
		return nil, nil, nil
	}

	queryCtx, cancel, err := gateQueryContext(ctx, deadline)
	if err != nil {
		return nil, nil, err
	}
	defer cancel()

	queryResult, err := s.Cluster.SetHosts(peers).QueryAny(queryCtx, s.sqlReplicatedObjects())
	if mappedErr := gateQueryError(ctx, queryCtx, err); mappedErr != nil {
		return nil, nil, mappedErr
	}
	if queryResult == nil {
		return nil, nil, fmt.Errorf("empty replicated object discovery result from peers %v", peers)
	}
	defer queryResult.Close()

	databaseNames := make([]string, 0)
	replicatedTables := make([]replicatedTable, 0)
	for queryResult.Rows.Next() {
		var objectType string
		var databaseName string
		var tableName string
		if err := queryResult.Rows.Scan(&objectType, &databaseName, &tableName); err != nil {
			if mappedErr := gateQueryError(ctx, queryCtx, err); mappedErr != nil {
				return nil, nil, mappedErr
			}
			return nil, nil, err
		}
		switch objectType {
		case "database":
			databaseNames = append(databaseNames, databaseName)
		case "table":
			replicatedTables = append(replicatedTables, replicatedTable{
				DatabaseName: databaseName,
				TableName:    tableName,
			})
		default:
			return nil, nil, fmt.Errorf("unknown replicated object type %q", objectType)
		}
	}
	if err := queryResult.Rows.Err(); err != nil {
		if mappedErr := gateQueryError(ctx, queryCtx, err); mappedErr != nil {
			return nil, nil, mappedErr
		}
		return nil, nil, err
	}
	if mappedErr := gateQueryError(ctx, queryCtx, nil); mappedErr != nil {
		return nil, nil, mappedErr
	}
	return databaseNames, replicatedTables, nil
}

func (s *ClusterSchemer) execHostWithDeadline(ctx context.Context, host *api.Host, deadline time.Time, querySQL string) error {
	remaining, err := gateRemaining(ctx, deadline)
	if err != nil {
		return err
	}

	opts := clickhouse.NewQueryOptions()
	opts.SetRetry(false)
	opts.SetQueryTimeout(remaining)

	err = s.ExecHost(ctx, host, []string{querySQL}, opts)
	if contextError := ctx.Err(); contextError != nil {
		return contextError
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrGateDeadline
	}
	return err
}

func (s *ClusterSchemer) queryHostIntWithDeadline(ctx context.Context, host *api.Host, deadline time.Time, querySQL string) (int, error) {
	queryCtx, cancel, err := gateQueryContext(ctx, deadline)
	if err != nil {
		return 0, err
	}
	defer cancel()

	queryValue, err := s.QueryHostInt(queryCtx, host, querySQL)
	if mappedErr := gateQueryError(ctx, queryCtx, err); mappedErr != nil {
		return 0, mappedErr
	}
	return queryValue, nil
}

func (s *ClusterSchemer) queryHostStringWithDeadline(ctx context.Context, host *api.Host, deadline time.Time, querySQL string) (string, error) {
	queryCtx, cancel, err := gateQueryContext(ctx, deadline)
	if err != nil {
		return "", err
	}
	defer cancel()

	queryValue, err := s.QueryHostString(queryCtx, host, querySQL)
	if mappedErr := gateQueryError(ctx, queryCtx, err); mappedErr != nil {
		return "", mappedErr
	}
	return queryValue, nil
}

func (s *ClusterSchemer) queryHostIntPairWithDeadline(ctx context.Context, host *api.Host, deadline time.Time, querySQL string) (int, int, error) {
	queryCtx, cancel, err := gateQueryContext(ctx, deadline)
	if err != nil {
		return 0, 0, err
	}
	defer cancel()

	queryResult, err := s.QueryHost(queryCtx, host, querySQL)
	if mappedErr := gateQueryError(ctx, queryCtx, err); mappedErr != nil {
		return 0, 0, mappedErr
	}
	if queryResult == nil {
		return 0, 0, fmt.Errorf("empty query result")
	}
	defer queryResult.Close()

	if !queryResult.Rows.Next() {
		if err := queryResult.Rows.Err(); err != nil {
			if mappedErr := gateQueryError(ctx, queryCtx, err); mappedErr != nil {
				return 0, 0, mappedErr
			}
			return 0, 0, err
		}
		return 0, 0, fmt.Errorf("found no rows")
	}

	var firstValue int
	var secondValue int
	if err := queryResult.Rows.Scan(&firstValue, &secondValue); err != nil {
		if mappedErr := gateQueryError(ctx, queryCtx, err); mappedErr != nil {
			return 0, 0, mappedErr
		}
		return 0, 0, err
	}
	if err := queryResult.Rows.Err(); err != nil {
		if mappedErr := gateQueryError(ctx, queryCtx, err); mappedErr != nil {
			return 0, 0, mappedErr
		}
		return 0, 0, err
	}
	if mappedErr := gateQueryError(ctx, queryCtx, nil); mappedErr != nil {
		return 0, 0, mappedErr
	}
	return firstValue, secondValue, nil
}

func gateQueryContext(ctx context.Context, deadline time.Time) (context.Context, context.CancelFunc, error) {
	remaining, err := gateRemaining(ctx, deadline)
	if err != nil {
		return nil, nil, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, remaining)
	return queryCtx, cancel, nil
}

func gateRemaining(ctx context.Context, deadline time.Time) (time.Duration, error) {
	if contextError := ctx.Err(); contextError != nil {
		return 0, contextError
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 0, ErrGateDeadline
	}
	return remaining, nil
}

func gateQueryError(parentCtx, queryCtx context.Context, err error) error {
	if contextError := parentCtx.Err(); contextError != nil {
		return contextError
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(queryCtx.Err(), context.DeadlineExceeded) {
		return ErrGateDeadline
	}
	if contextError := queryCtx.Err(); contextError != nil {
		return contextError
	}
	return err
}

func waitForNextGatePoll(ctx context.Context, deadline time.Time) error {
	remaining, err := gateRemaining(ctx, deadline)
	if err != nil {
		return err
	}
	sleepDuration := time.Second
	if remaining < sleepDuration {
		sleepDuration = remaining
	}
	timer := time.NewTimer(sleepDuration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
