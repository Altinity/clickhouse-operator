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
	"strings"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"
	"github.com/altinity/clickhouse-operator/pkg/model/common/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Cluster specifies ClickHouse cluster
type Cluster struct {
	*clickhouse.Cluster
	interfaces.INameManager
}

// NewCluster creates new cluster object
func NewCluster() *Cluster {
	return &Cluster{
		Cluster:      clickhouse.NewCluster(),
		INameManager: managers.NewNameManager(managers.NameManagerTypeClickHouse),
	}
}

// SetClusterConnectionParams sets endpoint credentials
func (c *Cluster) SetClusterConnectionParams(clusterConnectionParams *clickhouse.ClusterConnectionParams) *Cluster {
	if c == nil {
		return nil
	}
	c.ClusterConnectionParams = clusterConnectionParams
	return c
}

// queryUnzipColumns
func (c *Cluster) queryUnzipColumns(ctx context.Context, hosts []string, sql string, columns ...*[]string) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	if len(hosts) == 0 {
		// Nowhere to fetch data from
		return nil
	}

	// Fetch data from any of specified hosts
	query, err := c.SetHosts(hosts).QueryAny(ctx, sql)
	if err != nil {
		return nil
	}
	if query == nil {
		return nil
	}

	// Some data available, let's fetch it
	defer query.Close()
	return query.UnzipColumnsAsStrings(columns...)
}

// QueryUnzip2Columns unzips query result into two columns
func (c *Cluster) QueryUnzip2Columns(ctx context.Context, endpoints []string, sql string) ([]string, []string, error) {
	var column1 []string
	var column2 []string
	if err := c.queryUnzipColumns(ctx, endpoints, sql, &column1, &column2); err != nil {
		return nil, nil, err
	}
	return column1, column2, nil
}

// QueryUnzipAndApplyUUIDs unzips query result into two columns and applis UUID substituation if present
func (c *Cluster) QueryUnzipAndApplyUUIDs(ctx context.Context, endpoints []string, sql string) ([]string, []string, error) {
	var column1 []string
	var column2 []string
	var column3 []string
	var column4 []string
	if err := c.queryUnzipColumns(ctx, endpoints, sql, &column1, &column2, &column3, &column4); err != nil {
		return nil, nil, err
	}
	for i := 0; i < len(column1); i++ {
		if column4[i] != "" { // inner_uuid
			column2[i] = strings.ReplaceAll(column2[i], "{uuid}", column4[i])
		} else if column3[i] != "" { // uuid
			column2[i] = strings.ReplaceAll(column2[i], "{uuid}", column3[i])
		}
	}
	return column1, column2, nil
}

// ExecCHI runs set of SQL queries over the whole CHI
func (c *Cluster) ExecCHI(ctx context.Context, chi *api.ClickHouseInstallation, SQLs []string, _opts ...*clickhouse.QueryOptions) error {
	hosts := c.Names(namer.NameFQDNs, chi, nil, false)
	opts := clickhouse.QueryOptionsNormalize(_opts...)
	return c.SetHosts(hosts).ExecAll(ctx, SQLs, opts)
}

// ExecCluster runs set of SQL queries over the cluster
func (c *Cluster) ExecCluster(ctx context.Context, cluster *api.Cluster, SQLs []string, _opts ...*clickhouse.QueryOptions) error {
	hosts := c.Names(namer.NameFQDNs, cluster, nil, false)
	opts := clickhouse.QueryOptionsNormalize(_opts...)
	return c.SetHosts(hosts).ExecAll(ctx, SQLs, opts)
}

// ExecShard runs set of SQL queries over the shard replicas
func (c *Cluster) ExecShard(ctx context.Context, shard *api.ChiShard, SQLs []string, _opts ...*clickhouse.QueryOptions) error {
	hosts := c.Names(namer.NameFQDNs, shard, nil, false)
	opts := clickhouse.QueryOptionsNormalize(_opts...)
	return c.SetHosts(hosts).ExecAll(ctx, SQLs, opts)
}

// ExecHost runs set of SQL queries over the replica
func (c *Cluster) ExecHost(ctx context.Context, host *api.Host, SQLs []string, _opts ...*clickhouse.QueryOptions) error {
	hosts := c.Names(namer.NameFQDNs, host, api.Host{}, false)
	opts := clickhouse.QueryOptionsNormalize(_opts...)
	c.SetHosts(hosts)
	if opts.GetSilent() {
		c.SetLog(log.Silence())
	} else {
		c.SetLog(log.New())
	}
	return c.ExecAll(ctx, SQLs, opts)
}

// QueryHost runs specified query on specified host
func (c *Cluster) QueryHost(ctx context.Context, host *api.Host, sql string, _opts ...*clickhouse.QueryOptions) (*clickhouse.QueryResult, error) {
	hosts := c.Names(namer.NameFQDNs, host, api.Host{}, false)
	opts := clickhouse.QueryOptionsNormalize(_opts...)
	c.SetHosts(hosts)
	if opts.GetSilent() {
		c.SetLog(log.Silence())
	} else {
		c.SetLog(log.New())
	}
	// Fetch data from any of specified hosts
	return c.SetHosts(hosts).QueryAny(ctx, sql)
}

// QueryHostInt runs specified query on specified host and returns one int as a result
func (c *Cluster) QueryHostInt(ctx context.Context, host *api.Host, sql string, _opts ...*clickhouse.QueryOptions) (int, error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return 0, nil
	}

	query, err := c.QueryHost(ctx, host, sql, _opts...)
	defer query.Close()
	if query == nil {
		return 0, err
	}
	if err != nil {
		return 0, err
	}

	return query.Int()
}

// QueryHostString runs specified query on specified host and returns one string as a result
func (c *Cluster) QueryHostString(ctx context.Context, host *api.Host, sql string, _opts ...*clickhouse.QueryOptions) (string, error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return "", nil
	}

	query, err := c.QueryHost(ctx, host, sql, _opts...)
	defer query.Close()
	if query == nil {
		return "", err
	}
	if err != nil {
		return "", err
	}

	return query.String()
}
