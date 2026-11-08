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

// The test lives outside package schemer because the normalizer imports schemer.
package schemer_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/apis/swversion"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/schemer"
	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
)

// The normalizer and the namer resolve configuration through the global chop singleton.
func TestMain(m *testing.M) {
	os.Unsetenv("WATCH_NAMESPACE")
	os.Unsetenv("WATCH_NAMESPACES")
	chop.New(nil, nil, "")
	os.Exit(m.Run())
}

func testConnectionParams(port int) *clickhouse.ClusterConnectionParams {
	params := clickhouse.NewClusterConnectionParams("http", "clickhouse_operator", "password", "", port)
	params.SetTimeouts(clickhouse.NewTimeouts(2*time.Second, 2*time.Second))
	return params
}

// closedPort returns a local port that nothing listens on.
func closedPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

// A peer that rejects the operator user must surface as an error. Reporting it as an empty
// result makes the schemer think the cluster has no schema to copy.
func TestQueryUnzipReportsRejectedPeer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("Code: 516. DB::Exception: clickhouse_operator: Authentication failed. (AUTHENTICATION_FAILED)\n"))
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	port, err := strconv.Atoi(u.Port())
	require.NoError(t, err)

	cluster := schemer.NewCluster().SetClusterConnectionParams(testConnectionParams(port))
	peers := []string{u.Hostname()}

	names, sqls, err := cluster.QueryUnzip2Columns(context.Background(), peers, "SELECT name, create_query FROM system.tables")
	require.Error(t, err)
	require.Empty(t, names)
	require.Empty(t, sqls)

	names, sqls, err = cluster.QueryUnzipAndApplyUUIDs(context.Background(), peers, "SELECT name, create_query, uuid, inner_uuid FROM system.tables")
	require.Error(t, err)
	require.Empty(t, names)
	require.Empty(t, sqls)
}

// HostCreateTables must fail when it cannot read the schema from any peer, so that migrateTables
// does not record the host in hostsWithTablesCreated. Before the fix it ran no DDL and returned nil.
func TestHostCreateTablesFailsWhenPeersCannotBeRead(t *testing.T) {
	chi := &api.ClickHouseInstallation{}
	chi.Name = "schema"
	chi.Namespace = "ns"
	// .invalid never resolves (RFC 6761), so every peer is unreachable without any network setup.
	chi.Spec.NamespaceDomainPattern = types.NewString("%s.invalid")
	chi.Spec.Configuration = &api.Configuration{
		Clusters: []*api.Cluster{{
			Name:   "c1",
			Layout: &api.ChiClusterLayout{ShardsCount: 1, ReplicasCount: 2},
		}},
	}
	normalized, err := normalizer.New(nil).CreateTemplated(chi, commonNormalizer.NewOptions[api.ClickHouseInstallation]())
	require.NoError(t, err)

	host := normalized.FindHost("c1", 0, 1)
	require.NotNil(t, host)

	s := schemer.NewClusterSchemer(testConnectionParams(closedPort(t)), swversion.NewSoftWareVersion("26.10.3.7"))
	require.Error(t, s.HostCreateTables(context.Background(), host),
		"a schema read failure must not be reported as a successful migration")
}
