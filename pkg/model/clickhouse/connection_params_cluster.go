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
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
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
func NewClusterConnectionParamsFromCHOpConfig(config *api.OperatorConfig) *ClusterConnectionParams {
	params := NewClusterConnectionParams(
		config.ClickHouse.Access.Scheme,
		config.ClickHouse.Access.Username,
		config.ClickHouse.Access.Password,
		config.ClickHouse.Access.RootCA,
		config.ClickHouse.Access.Port,
	)
	params.SetConnectTimeout(config.ClickHouse.Access.Timeouts.Connect)
	params.SetQueryTimeout(config.ClickHouse.Access.Timeouts.Query)

	// Inject CHOP-config-level TLS hardening as defaults. Per-cluster overrides
	// from CHI spec land in the EndpointCredentials when the connection is built
	// for a specific host (see Cluster.Security via the schemer/exporter wiring).
	if tls := config.Security.GetClickHouse().GetTLS(); tls != nil {
		params.ClusterCredentials.SetTLSSecurity(
			tls.GetVerify(),
			tls.GetMinVersion(),
			tls.GetServerName(),
		)
		// If chopconf provides a RootCA via Security and Access didn't, lift it.
		if (params.RootCA == "") && (tls.GetRootCA() != "") {
			params.RootCA = tls.GetRootCA()
		}
	}

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

// OverlayClusterSecurityTLS layers the per-cluster TLS knobs over the
// CHOP-config-derived defaults. Empty fields in tls leave the existing value
// alone (per-cluster fill-empty, matching the normalize-time semantics). Called
// at schemer construction time, after the base ClusterConnectionParams is built
// from chopconf — without this step the 3-level inheritance from
// InheritClusterSecurityFrom + normalizeClusterSecurity would never reach the
// dial.
func (p *ClusterConnectionParams) OverlayClusterSecurityTLS(tls *api.ClusterSecurityClickHouseTLS) *ClusterConnectionParams {
	if (p == nil) || (tls == nil) {
		return p
	}
	return p.OverlayTLS(tls.GetVerify(), tls.GetMinVersion(), tls.GetServerName(), tls.GetRootCA())
}

// OverlayTLS layers TLS knobs over the CHOP-config-derived defaults. Empty values leave
// the existing value alone, same as OverlayClusterSecurityTLS.
func (p *ClusterConnectionParams) OverlayTLS(
	verify api.TLSVerify,
	minVersion api.TLSMinVersion,
	serverName string,
	rootCA string,
) *ClusterConnectionParams {
	if p == nil {
		return p
	}
	if verify != "" {
		p.TLSVerify = verify
	}
	if minVersion != "" {
		p.TLSMinVersion = minVersion
	}
	if serverName != "" {
		p.TLSServerName = serverName
	}
	if rootCA != "" {
		p.RootCA = rootCA
	}
	return p
}

// NewEndpointConnectionParams creates endpoint connection params for a specified host in the cluster
func (p *ClusterConnectionParams) NewEndpointConnectionParams(host string) *EndpointConnectionParams {
	if p == nil {
		return nil
	}
	endpoint := NewEndpointConnectionParams(
		p.Scheme,
		host,
		p.Username,
		p.Password,
		p.RootCA,
		p.Port,
	).SetTimeouts(p.Timeouts)
	endpoint.EndpointCredentials.SetTLSSecurity(p.TLSVerify, p.TLSMinVersion, p.TLSServerName)
	return endpoint
}
