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
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chi"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// shouldCreateDistributedObjects determines whether distributed objects should be created
func shouldCreateDistributedObjects(host *api.Host) bool {
	hosts := namer.CreateFQDNs(host, api.Cluster{}, false)

	if host.GetCluster().SchemaPolicy.Shard == model.SchemaPolicyShardNone {
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
func (s *ClusterSchemer) getDistributedObjectsSQLs(ctx context.Context, host *api.Host) ([]string, []string, error) {
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
			namer.CreateFQDNs(host, api.ClickHouseInstallation{}, false),
			s.sqlCreateDatabaseDistributed(host.Runtime.Address.ClusterName),
		),
	)
	tableNames, createTableSQLs := debugCreateSQLs(
		s.QueryUnzipAndApplyUUIDs(
			ctx,
			namer.CreateFQDNs(host, api.ClickHouseInstallation{}, false),
			s.sqlCreateTableDistributed(host.Runtime.Address.ClusterName),
		),
	)
	functionNames, createFunctionSQLs := debugCreateSQLs(
		s.QueryUnzip2Columns(
			ctx,
			namer.CreateFQDNs(host, api.ClickHouseInstallation{}, false),
			s.sqlCreateFunction(host.Runtime.Address.ClusterName),
		),
	)
	return util.ConcatSlices([][]string{databaseNames, tableNames, functionNames}),
		util.ConcatSlices([][]string{createDatabaseSQLs, createTableSQLs, createFunctionSQLs}),
		nil
}
