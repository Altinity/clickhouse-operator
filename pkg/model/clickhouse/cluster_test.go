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
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// recordingDriver stands in for a ClickHouse endpoint: it records every statement it is
// asked to execute, bucketed by DSN (one DSN per host), and can be told to fail a
// statement the first time it sees it. Registering a driver lets the test drive
// Connection.Exec() through the real database/sql path with no server and no network.
type recordingDriver struct {
	mu       sync.Mutex
	executed map[string][]string
	failOnce map[string]bool
}

func (d *recordingDriver) Open(dsn string) (driver.Conn, error) {
	return &recordingConn{driver: d, dsn: dsn}, nil
}

func (d *recordingDriver) executedOn(dsn string) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.executed[dsn])
}

type recordingConn struct {
	driver *recordingDriver
	dsn    string
}

func (c *recordingConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.driver.mu.Lock()
	defer c.driver.mu.Unlock()
	c.driver.executed[c.dsn] = append(c.driver.executed[c.dsn], query)
	if c.driver.failOnce[query] {
		delete(c.driver.failOnce, query)
		return nil, fmt.Errorf("injected failure for: %s", query)
	}
	return driver.RowsAffected(0), nil
}

func (c *recordingConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused") }
func (c *recordingConn) Begin() (driver.Tx, error)           { return nil, errors.New("unused") }
func (c *recordingConn) Close() error                        { return nil }

// testDriverSeq keeps driver names unique - database/sql panics on a duplicate name and
// offers no way to unregister.
var testDriverSeq atomic.Int64

// newRecordingCluster builds a Cluster over the given hosts whose pooled connections are
// already established against a recordingDriver. Pre-seeding the pool is what keeps the
// test offline: Connection.Exec() finds dbPrimary non-nil and skips connecting.
func newRecordingCluster(t *testing.T, hosts ...string) (*Cluster, *recordingDriver, func(host string) string) {
	t.Helper()

	drv := &recordingDriver{
		executed: map[string][]string{},
		failOnce: map[string]bool{},
	}
	driverName := fmt.Sprintf("clickhouse-test-recorder-%d", testDriverSeq.Add(1))
	sql.Register(driverName, drv)

	cluster := NewCluster().SetHosts(hosts)
	cluster.ClusterConnectionParams = NewClusterConnectionParams("http", "test-user", "test-password", "", 8123)

	dsnOf := func(host string) string { return cluster.NewEndpointConnectionParams(host).GetDSN() }

	for _, host := range hosts {
		params := cluster.NewEndpointConnectionParams(host)
		db, err := sql.Open(driverName, params.GetDSN())
		require.NoError(t, err)

		key := makePoolKey(params)
		dbConnectionPool.Store(key, &Connection{
			params:      params,
			dbPrimary:   db,
			dbSecondary: db,
			l:           cluster.l,
		})
		t.Cleanup(func() {
			dbConnectionPool.Delete(key)
			_ = db.Close()
		})
	}

	return cluster, drv, dsnOf
}

// TestExecAllDoesNotConsumeCallerQueries locks in the fix for #2052.
//
// exec() blanks out queries that already succeeded so that a retry within the same call
// skips them. It used to do that in the CALLER's slice. Reconcile hooks pass the CR
// spec's sql.Queries, which is reused for every host and every shard, so the first host
// consumed the payload and every host after it silently executed nothing.
func TestExecAllDoesNotConsumeCallerQueries(t *testing.T) {
	want := []string{"SYSTEM STOP FETCHES", "SYSTEM STOP MERGES"}

	t.Run("every host of one ExecAll gets the full list", func(t *testing.T) {
		cluster, drv, dsnOf := newRecordingCluster(t, "host-0-0", "host-0-1")
		queries := slices.Clone(want)

		require.NoError(t, cluster.ExecAll(context.Background(), queries))

		require.Equal(t, want, drv.executedOn(dsnOf("host-0-0")))
		require.Equal(t, want, drv.executedOn(dsnOf("host-0-1")),
			"second host must receive the full payload, not the blanked leftovers")
		require.Equal(t, want, queries, "caller's slice must not be mutated")
	})

	t.Run("one list reused across calls, as the AllShards hook does", func(t *testing.T) {
		// A cluster hook with target AllShards calls ExecHost once per shard, handing the
		// same spec slice over each time.
		cluster, drv, dsnOf := newRecordingCluster(t, "shard-0", "shard-1")
		queries := slices.Clone(want)

		for _, host := range []string{"shard-0", "shard-1"} {
			require.NoError(t, cluster.SetHosts([]string{host}).ExecAll(context.Background(), queries))
		}

		require.Equal(t, want, drv.executedOn(dsnOf("shard-0")))
		require.Equal(t, want, drv.executedOn(dsnOf("shard-1")))
		require.Equal(t, want, queries, "caller's slice must not be mutated")
	})
}

// TestExecRetrySkipsQueriesThatAlreadyLanded guards where the clone is taken. It has to
// happen once per exec() call, outside the retry loop: cloning inside would hand each
// attempt a fresh list and re-issue DDL that already succeeded.
func TestExecRetrySkipsQueriesThatAlreadyLanded(t *testing.T) {
	if testing.Short() {
		t.Skip("retry backoff sleeps seconds between attempts")
	}

	cluster, drv, dsnOf := newRecordingCluster(t, "host-0-0")
	first, second := "CREATE TABLE first", "CREATE TABLE second"
	drv.failOnce[second] = true

	queries := []string{first, second}
	require.NoError(t, cluster.ExecAll(context.Background(), queries, &QueryOptions{Tries: 2}))

	require.Equal(t, []string{first, second, second}, drv.executedOn(dsnOf("host-0-0")),
		"attempt 2 must retry only the query that failed")
	require.Equal(t, []string{first, second}, queries, "caller's slice must not be mutated")
}
