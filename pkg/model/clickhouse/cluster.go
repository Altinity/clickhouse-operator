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

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/util"
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
	Endpoints []string
}

// NewCluster
func NewCluster() *Cluster {
	return new(Cluster)
}

// SetConnectionParams
func (c *Cluster)SetConnectionParams(params ClusterConnectionParams) *Cluster {
	if c == nil {
		return nil
	}
	c.ClusterConnectionParams = params
	return c
}

// SetEndpoints
func (c *Cluster)SetEndpoints(endpoints []string) *Cluster {
	if c == nil {
		return nil
	}
	c.Endpoints = endpoints
	return c
}

// getCHConnection
func (c *Cluster) getCHConnection(hostname string) *CHConnection {
	return GetPooledDBConnection(NewCHConnectionParams(hostname, c.Username, c.Password, c.Port))
}

// Any walks over provided endpoints and runs query sequentially on each endpoint.
// In case endpoint returned result, walk is completed and result is returned
// In case endpoint failed, continue with next endpoint
func (c *Cluster) Any(ctx context.Context, sql string) (*Query, error) {
	// Fetch data from any of specified endpoints
	for _, endpoint := range c.Endpoints {
		if util.IsContextDone(ctx) {
			log.V(2).Info("ctx is done")
			return nil, nil
		}

		log.V(1).Info("Run query on: %s of %v", endpoint, c.Endpoints)
		if query, err := c.getCHConnection(endpoint).QueryContext(ctx, sql); err == nil {
			// One of specified endpoints returned result, no need to iterate more
			return query, nil
		} else {
			log.V(1).A().Warning("FAILED to run query on: %s of %v skip to next. err: %v", endpoint, c.Endpoints, err)
		}
	}

	str := fmt.Sprintf("FAILED to run query on all endpoints %v", c.Endpoints)
	log.V(1).A().Error(str)
	return nil, fmt.Errorf(str)
}
