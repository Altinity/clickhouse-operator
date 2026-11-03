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
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/metrics"
	"github.com/altinity/clickhouse-operator/pkg/chop"
)

func init() {
	chop.New(nil, nil, "")
}

const testRootCA = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----"

func secureCluster() *metrics.WatchedCluster {
	return &metrics.WatchedCluster{
		Name: "secure",
		TLS: &metrics.WatchedTLS{
			Verify:     api.TLSVerifyStrict,
			MinVersion: api.TLSMinVersion13,
			ServerName: "clickhouse.example",
			RootCA:     testRootCA,
		},
	}
}

// TestNewHostFetcherOverlaysClusterTLS pins that the exporter dials a host with its
// cluster's TLS settings on top of chopconf, as the operator does.
func TestNewHostFetcherOverlaysClusterTLS(t *testing.T) {
	cfg := chop.Config()
	prev := cfg.ClickHouse.Access.Scheme
	cfg.ClickHouse.Access.Scheme = api.ChSchemeHTTPS
	t.Cleanup(func() { cfg.ClickHouse.Access.Scheme = prev })

	host := &metrics.WatchedHost{Hostname: "chi-a-secure-0-0.ns.svc.cluster.local.", HTTPSPort: 8443}

	fetcher := (&Exporter{}).newHostFetcher(secureCluster(), host)
	params := fetcher.connectionParams
	require.Equal(t, testRootCA, params.RootCA())
	require.Equal(t, api.TLSVerifyStrict, params.TLSVerify())
	require.Equal(t, api.TLSMinVersion13, params.TLSMinVersion())
	require.Equal(t, "clickhouse.example", params.TLSServerName())

	// A cluster without TLS settings keeps chopconf's values.
	plain := (&Exporter{}).newHostFetcher(&metrics.WatchedCluster{Name: "plain"}, host).connectionParams
	require.Equal(t, cfg.ClickHouse.Access.RootCA, plain.RootCA())
	require.NotEqual(t, params.TLSConfigKey(), plain.TLSConfigKey(),
		"clusters with different TLS settings must not share a registered tls.Config")
}

// TestRegistryAddHostKeepsClusterTLS pins that a host added on its own brings the
// cluster's TLS settings along, so the exporter can dial it before the full CR update.
func TestRegistryAddHostKeepsClusterTLS(t *testing.T) {
	registry := NewCRRegistry()
	registry.AddCR(&metrics.WatchedCR{Namespace: "ns", Name: "a"})

	tls := secureCluster().TLS
	err := registry.AddHost(&HostRequest{
		CRNamespace: "ns",
		CRName:      "a",
		ClusterName: "secure",
		ClusterTLS:  tls,
		Host:        &metrics.WatchedHost{Hostname: "chi-a-secure-0-0"},
	})
	require.NoError(t, err)

	var got *metrics.WatchedTLS
	registry.Walk(func(_ *metrics.WatchedCR, cluster *metrics.WatchedCluster, _ *metrics.WatchedHost) {
		got = cluster.GetTLS()
	})
	require.Equal(t, tls, got)

	// A later request without TLS does not wipe what the cluster already has.
	err = registry.AddHost(&HostRequest{
		CRNamespace: "ns",
		CRName:      "a",
		ClusterName: "secure",
		Host:        &metrics.WatchedHost{Hostname: "chi-a-secure-0-1"},
	})
	require.NoError(t, err)
	registry.Walk(func(_ *metrics.WatchedCR, cluster *metrics.WatchedCluster, _ *metrics.WatchedHost) {
		require.Equal(t, tls, cluster.GetTLS())
	})
}
