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

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// shouldCreateReplicatedObjects determines whether replicated objects should be created
func (s *ClusterSchemer) shouldCreateReplicatedObjects(host *api.Host) bool {
	shard := s.Names(interfaces.NameFQDNs, host, api.ChiShard{}, false)
	cluster := s.Names(interfaces.NameFQDNs, host, api.Cluster{}, false)

	if host.GetCluster().GetSchemaPolicy().Shard == SchemaPolicyShardAll {
		// We have explicit request to create replicated objects on each shard
		// However, it is reasonable to have at least two instances in a cluster
		if len(cluster) >= 2 {
			log.V(1).M(host).F().Info("SchemaPolicy.Shard says we need replicated objects. Should create replicated objects for the shard: %v", shard)
			return true
		}
	}

	if host.GetCluster().GetSchemaPolicy().Replica == SchemaPolicyReplicaNone {
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
func (s *ClusterSchemer) getReplicatedObjectsSQLs(ctx context.Context, host *api.Host) ([]string, []string, error) {
	if util.IsContextDone(ctx) {
		log.V(1).Info("ctx is done")
		return nil, nil, nil
	}

	if !s.shouldCreateReplicatedObjects(host) {
		log.V(1).M(host).F().Info("Should not create replicated objects")
		return nil, nil, nil
	}

	databaseNames, createDatabaseSQLs := debugCreateSQLs(
		s.QueryUnzip2Columns(
			ctx,
			s.Names(interfaces.NameFQDNs, host, api.ClickHouseInstallation{}, false),
			s.sqlCreateDatabaseReplicated(host.Runtime.Address.ClusterName),
		),
	)
	tableNames, createTableSQLs := debugCreateSQLs(
		s.QueryUnzipAndApplyUUIDs(
			ctx,
			s.Names(interfaces.NameFQDNs, host, api.ClickHouseInstallation{}, false),
			s.sqlCreateTableReplicated(host.Runtime.Address.ClusterName),
		),
	)
	functionNames, createFunctionSQLs := debugCreateSQLs(
		s.QueryUnzip2Columns(
			ctx,
			s.Names(interfaces.NameFQDNs, host, api.ClickHouseInstallation{}, false),
			s.sqlCreateFunction(host.Runtime.Address.ClusterName),
		),
	)
	return util.ConcatSlices([][]string{databaseNames, tableNames, functionNames}),
		util.ConcatSlices([][]string{createDatabaseSQLs, createTableSQLs, createFunctionSQLs}),
		nil
}
