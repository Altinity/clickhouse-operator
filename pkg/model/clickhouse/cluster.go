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

package clickhouse

import (
	"context"
	"fmt"
	"github.com/altinity/clickhouse-operator/pkg/util"
	"strings"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	r "github.com/altinity/clickhouse-operator/pkg/util/retry"
)

const (
	// Max number of tries for SQL queries
	defaultMaxTries = 10
)

// ClusterConnectionParams
type ClusterConnectionParams struct {
	Username string
	Password string
	Port int
}

// Cluster
type Cluster struct {
	ClusterConnectionParams
	Hosts []string
	A log.Announcer
}

// NewCluster
func NewCluster() *Cluster {
	return &Cluster{
		A: log.New(),
	}
}

// SetLog
func (c *Cluster) SetLog(a log.Announcer) *Cluster {
	if c == nil {
		return nil
	}
	c.A = a
	return c
}

// SetConnectionParams
func (c *Cluster)SetConnectionParams(params ClusterConnectionParams) *Cluster {
	if c == nil {
		return nil
	}
	c.ClusterConnectionParams = params
	return c
}

// SetHosts
func (c *Cluster)SetHosts(hosts []string) *Cluster {
	if c == nil {
		return nil
	}
	c.Hosts = hosts
	return c
}

// getCHConnection
func (c *Cluster) getCHConnection(host string) *CHConnection {
	return GetPooledDBConnection(NewCHConnectionParams(host, c.Username, c.Password, c.Port)).SetLog(c.A)
}

// QueryAny walks over provided endpoints and runs query sequentially on each endpoint.
// In case endpoint returned result, walk is completed and result is returned
// In case endpoint failed, continue with next endpoint
func (c *Cluster) QueryAny(ctx context.Context, sql string) (*Query, error) {
	// Fetch data from any of specified endpoints
	for _, host := range c.Hosts {
		if util.IsContextDone(ctx) {
			c.A.V(2).Info("ctx is done")
			return nil, nil
		}

		c.A.V(1).Info("Run query on: %s of %v", host, c.Hosts)
		if query, err := c.getCHConnection(host).QueryContext(ctx, sql); err == nil {
			// One of specified endpoints returned result, no need to iterate more
			return query, nil
		} else {
			c.A.V(1).A().Warning("FAILED to run query on: %s of %v skip to next. err: %v", host, c.Hosts, err)
		}
	}

	str := fmt.Sprintf("FAILED to run query on all hosts %v", c.Hosts)
	c.A.V(1).A().Error(str)
	return nil, fmt.Errorf(str)
}

// ExecAll runs set of SQL queries on all endpoints of the cluster
// Retry logic traverses the list of SQLs multiple times until all SQLs succeed
func (c *Cluster) ExecAll(ctx context.Context, queries []string, retry bool) error {
	if util.IsContextDone(ctx) {
		c.A.V(2).Info("ctx is done")
		return nil
	}

	maxTries := 1
	if retry {
		maxTries = defaultMaxTries
	}
	var errors []error

	// For each host in the list run all SQL queries
	for _, host := range c.Hosts {
		if util.IsContextDone(ctx) {
			c.A.V(2).Info("ctx is done")
			return nil
		}
		conn := c.getCHConnection(host)
		if conn == nil {
			c.A.V(1).M(host).F().Warning("Unable to get conn to host %s", host)
			continue
		}
		err := r.RetryContext(ctx, maxTries, "Applying sqls", c.A.V(1).M(host).F(),
			func() error {
			var errors []error
			for i, sql := range queries {
				if util.IsContextDone(ctx) {
					c.A.V(2).Info("ctx is done")
					return nil
				}
				if len(sql) == 0 {
					// Skip malformed or already executed SQL query, move to the next one
					continue
				}
				err := conn.ExecContext(ctx, sql)
				if err != nil && strings.Contains(err.Error(), "Code: 253,") && strings.Contains(sql, "CREATE TABLE") {
					c.A.V(1).M(host).F().Info("Replica is already in ZooKeeper. Trying ATTACH TABLE instead")
					sqlAttach := strings.ReplaceAll(sql, "CREATE TABLE", "ATTACH TABLE")
					err = conn.ExecContext(ctx, sqlAttach)
				}
				if err == nil {
					queries[i] = "" // Query is executed, removing from the list
				} else {
					errors = append(errors, err)
				}
			}

			if len(errors) > 0 {
				return errors[0]
			}
			return nil
		},
		)

		if util.ErrIsNotCanceled(err) {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return errors[0]
	}
	return nil
}
