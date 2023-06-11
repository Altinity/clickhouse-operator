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
	v1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// ClusterConnectionParams represents connection parameters to the whole cluster
type ClusterConnectionParams struct {
	*ClusterCredentials
	*Timeouts
}

// NewClusterConnectionParams creates new ClusterConnectionParams
func NewClusterConnectionParams(scheme, username, password, rootCA string, port int) *ClusterConnectionParams {
	return &ClusterConnectionParams{
		NewClusterCredentials(scheme, username, password, rootCA, port),
		NewTimeouts(),
	}
}

// NewClusterConnectionParamsFromCHOpConfig is the same as NewClusterConnectionParams, but works with
// CHOp config to get parameters from
func NewClusterConnectionParamsFromCHOpConfig(config *v1.OperatorConfig) *ClusterConnectionParams {
	params := NewClusterConnectionParams(
		config.ClickHouse.Access.Scheme,
		config.ClickHouse.Access.Username,
		config.ClickHouse.Access.Password,
		config.ClickHouse.Access.RootCA,
		config.ClickHouse.Access.Port,
	)
	params.SetConnectTimeout(config.ClickHouse.Access.Timeouts.Connect)
	params.SetQueryTimeout(config.ClickHouse.Access.Timeouts.Query)

	return params
}

// SetTimeouts sets timeout
func (p *ClusterConnectionParams) SetTimeouts(timeouts *Timeouts) *ClusterConnectionParams {
	if p == nil {
		return nil
	}
	p.Timeouts = timeouts
	return p
}

// NewEndpointConnectionParams creates endpoint connection params for a specified host in the cluster
func (p *ClusterConnectionParams) NewEndpointConnectionParams(host string) *EndpointConnectionParams {
	if p == nil {
		return nil
	}
	return NewEndpointConnectionParams(
		p.Scheme,
		host,
		p.Username,
		p.Password,
		p.RootCA,
		p.Port,
	).SetTimeouts(p.Timeouts)
}
