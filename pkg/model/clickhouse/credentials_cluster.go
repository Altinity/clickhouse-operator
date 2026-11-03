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

// ClusterCredentials specifies cluster endpoint credentials
type ClusterCredentials struct {
	Scheme   string
	Username string
	Password string
	RootCA   string
	Port     int

	// TLS hardening knobs — empty values preserve current behavior. Populated
	// from chopconf security.clickhouse.tls.* by NewClusterConnectionParamsFromCHOpConfig
	// and then overlaid per-cluster by OverlayClusterSecurityTLS. Valid values
	// are the api.TLSVerify*/api.TLSMinVersion* constants; empty preserves the
	// legacy path.
	TLSVerify     api.TLSVerify
	TLSMinVersion api.TLSMinVersion
	TLSServerName string
}

// NewClusterCredentials creates new ClusterCredentials
func NewClusterCredentials(scheme, username, password, rootCA string, port int) *ClusterCredentials {
	return &ClusterCredentials{
		Scheme:   scheme,
		Username: username,
		Password: password,
		RootCA:   rootCA,
		Port:     port,
	}
}

// SetTLSSecurity injects TLS hardening knobs. Empty values preserve legacy behavior.
func (c *ClusterCredentials) SetTLSSecurity(verify api.TLSVerify, minVersion api.TLSMinVersion, serverName string) *ClusterCredentials {
	if c == nil {
		return nil
	}
	c.TLSVerify = verify
	c.TLSMinVersion = minVersion
	c.TLSServerName = serverName
	return c
}
