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

package metrics

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

const testRootCA = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----"

func chiWithClusters(clusters ...*api.Cluster) *api.ClickHouseInstallation {
	return &api.ClickHouseInstallation{
		Spec: api.ChiSpec{
			Configuration: &api.Configuration{
				Clusters: clusters,
			},
		},
	}
}

func clusterWithTLS(name string, tls *api.ClusterSecurityClickHouseTLS) *api.Cluster {
	return &api.Cluster{
		Name:   name,
		Layout: &api.ChiClusterLayout{},
		Security: &api.ClusterSecurity{
			ClickHouse: &api.ClusterSecurityClickHouse{TLS: tls},
		},
	}
}

// TestWatchedCRCarriesClusterTLS pins that the metrics exporter receives the cluster's
// ClickHouse client TLS settings, including the CA the normalizer inlined from rootCASecretRef.
// Without them the exporter dials with chopconf's CA only and fails verification.
func TestWatchedCRCarriesClusterTLS(t *testing.T) {
	chi := chiWithClusters(
		clusterWithTLS("secure", &api.ClusterSecurityClickHouseTLS{
			Verify:     types.NewString(string(api.TLSVerifyStrict)),
			MinVersion: types.NewString(string(api.TLSMinVersion13)),
			ServerName: "clickhouse.example",
			RootCA:     testRootCA,
		}),
		&api.Cluster{Name: "plain", Layout: &api.ChiClusterLayout{}},
	)

	watched := NewWatchedCR(chi)
	require.Len(t, watched.Clusters, 2)

	secure := watched.Clusters[0]
	require.Equal(t, &WatchedTLS{
		Verify:     api.TLSVerifyStrict,
		MinVersion: api.TLSMinVersion13,
		ServerName: "clickhouse.example",
		RootCA:     testRootCA,
	}, secure.GetTLS())

	require.Nil(t, watched.Clusters[1].GetTLS(), "a cluster without TLS settings carries none")

	// The operator sends WatchedCR to the exporter as JSON.
	bytes, err := json.Marshal(watched)
	require.NoError(t, err)
	received := &WatchedCR{}
	require.NoError(t, json.Unmarshal(bytes, received))
	require.Equal(t, secure.GetTLS(), received.Clusters[0].GetTLS())
	require.Nil(t, received.Clusters[1].GetTLS())
}

func TestNewWatchedTLSNilSafe(t *testing.T) {
	require.Nil(t, NewWatchedTLS(nil))
	var missing *api.Cluster
	require.Nil(t, NewWatchedTLS(missing))
	require.Nil(t, NewWatchedTLS(&api.Cluster{Name: "plain"}))

	var tls *WatchedTLS
	require.Equal(t, api.TLSVerify(""), tls.GetVerify())
	require.Equal(t, api.TLSMinVersion(""), tls.GetMinVersion())
	require.Equal(t, "", tls.GetServerName())
	require.Equal(t, "", tls.GetRootCA())
	var cluster *WatchedCluster
	require.Nil(t, cluster.GetTLS())
}
