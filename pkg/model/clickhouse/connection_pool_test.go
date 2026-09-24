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
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goch "github.com/mailru/go-clickhouse/v2"
	"github.com/stretchr/testify/require"
)

var testDriverID atomic.Uint64

type countingDriver struct {
	opens  atomic.Int64
	closes atomic.Int64
	ping   func(context.Context) error
}

func (d *countingDriver) Open(string) (driver.Conn, error) {
	d.opens.Add(1)
	return &countingConn{driver: d}, nil
}

type countingConn struct {
	driver *countingDriver
}

func (*countingConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}

func (c *countingConn) Close() error {
	c.driver.closes.Add(1)
	return nil
}

func (*countingConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not implemented")
}

func (c *countingConn) Ping(ctx context.Context) error {
	if c.driver.ping != nil {
		return c.driver.ping(ctx)
	}
	return nil
}

func registerCountingDriver(t *testing.T) (string, *countingDriver) {
	t.Helper()

	name := fmt.Sprintf("clickhouse-connection-test-%d", testDriverID.Add(1))
	d := &countingDriver{}
	sql.Register(name, d)
	return name, d
}

func TestConfigureDBConnectionPoolRecyclesExpiredConnections(t *testing.T) {
	driverName, d := registerCountingDriver(t)
	db, err := sql.Open(driverName, "")
	require.NoError(t, err)
	defer db.Close()

	configureDBConnectionPool(db, 10*time.Millisecond)
	require.NoError(t, db.PingContext(context.Background()))
	require.Equal(t, int64(1), d.opens.Load())

	require.Eventually(t, func() bool {
		if db.PingContext(context.Background()) != nil {
			return false
		}
		return d.opens.Load() >= 2 && db.Stats().MaxLifetimeClosed >= 1
	}, time.Second, 10*time.Millisecond)
	require.GreaterOrEqual(t, d.closes.Load(), int64(1))
}

func TestDBInitializesSharedConnectionOnce(t *testing.T) {
	driverName, d := registerCountingDriver(t)
	params := NewEndpointConnectionParams("http", "clickhouse.test", "user", "password", "", 8123)
	connection := NewConnection(params)

	var openCalls atomic.Int64
	connection.openDB = func(string, string) (*sql.DB, error) {
		openCalls.Add(1)
		return sql.Open(driverName, "")
	}

	const goroutines = 20
	start := make(chan struct{})
	results := make(chan error, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start
			_, err := connection.db(context.Background(), false)
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	for err := range results {
		require.NoError(t, err)
	}
	require.Equal(t, int64(2), openCalls.Load(), "only the primary and secondary pools should be opened")
	require.Equal(t, int64(2), d.opens.Load(), "only the two Ping calls should establish driver connections")

	require.NoError(t, connection.dbPrimary.Close())
	require.NoError(t, connection.dbSecondary.Close())
}

func TestDBWaiterCanCancelWithoutDiscardingInitialization(t *testing.T) {
	driverName, d := registerCountingDriver(t)
	pingStarted := make(chan struct{})
	releasePing := make(chan struct{})
	var signalOnce sync.Once
	d.ping = func(ctx context.Context) error {
		signalOnce.Do(func() { close(pingStarted) })
		select {
		case <-releasePing:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	connection := NewConnection(NewEndpointConnectionParams("http", "clickhouse.test", "user", "password", "", 8123))
	var openCalls atomic.Int64
	connection.openDB = func(string, string) (*sql.DB, error) {
		openCalls.Add(1)
		return sql.Open(driverName, "")
	}

	initialized := make(chan error, 1)
	go func() {
		_, err := connection.db(context.Background(), false)
		initialized <- err
	}()
	<-pingStarted

	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := connection.db(waitCtx, false)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	close(releasePing)
	require.NoError(t, <-initialized)
	require.NotNil(t, connection.currentDB(false))
	require.NotNil(t, connection.currentDB(true))
	require.Equal(t, int64(2), openCalls.Load(), "the canceled waiter must share, not duplicate, initialization")
	require.NoError(t, connection.dbPrimary.Close())
	require.NoError(t, connection.dbSecondary.Close())
}

type operationDriver struct {
	execErr error
	execs   atomic.Int64
	closes  atomic.Int64
}

func (d *operationDriver) Open(string) (driver.Conn, error) {
	return &operationConn{driver: d}, nil
}

type operationConn struct {
	driver *operationDriver
}

func (*operationConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}

func (c *operationConn) Close() error {
	c.driver.closes.Add(1)
	return nil
}

func (*operationConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not implemented")
}

func (*operationConn) Ping(context.Context) error {
	return nil
}

func (c *operationConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	c.driver.execs.Add(1)
	if c.driver.execErr != nil {
		return nil, c.driver.execErr
	}
	return driver.RowsAffected(0), nil
}

func newOperationDB(t *testing.T, d *operationDriver) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("clickhouse-operation-test-%d", testDriverID.Add(1))
	sql.Register(name, d)
	db, err := sql.Open(name, "")
	require.NoError(t, err)
	return db
}

func TestConnectionFailureResetsPoolsForNextRetryWithoutReplaying(t *testing.T) {
	failedErr := context.DeadlineExceeded
	oldPrimaryDriver := &operationDriver{execErr: failedErr}
	oldSecondaryDriver := &operationDriver{}
	newPrimaryDriver := &operationDriver{}
	newSecondaryDriver := &operationDriver{}
	dbs := []*sql.DB{
		newOperationDB(t, oldPrimaryDriver),
		newOperationDB(t, oldSecondaryDriver),
		newOperationDB(t, newPrimaryDriver),
		newOperationDB(t, newSecondaryDriver),
	}

	params := NewEndpointConnectionParams("http", "clickhouse.test", "user", "password", "", 8123)
	connection := NewConnection(params)
	var openIndex atomic.Int64
	connection.openDB = func(string, string) (*sql.DB, error) {
		index := int(openIndex.Add(1) - 1)
		if index >= len(dbs) {
			return nil, errors.New("unexpected pool open")
		}
		return dbs[index], nil
	}

	err := connection.Exec(context.Background(), "SELECT 1", NewQueryOptions())
	require.ErrorIs(t, err, failedErr)
	require.Equal(t, int64(1), oldPrimaryDriver.execs.Load(), "a failed statement must not be replayed")
	require.Nil(t, connection.dbPrimary)
	require.Nil(t, connection.dbSecondary)
	require.Equal(t, int64(1), oldPrimaryDriver.closes.Load())
	require.Equal(t, int64(1), oldSecondaryDriver.closes.Load())

	require.NoError(t, connection.Exec(context.Background(), "SELECT 1", NewQueryOptions()))
	require.Equal(t, int64(4), openIndex.Load(), "the next retry must create a fresh pool pair")
	require.Equal(t, int64(1), newPrimaryDriver.execs.Load())
	require.NoError(t, connection.dbPrimary.Close())
	require.NoError(t, connection.dbSecondary.Close())
}

func TestClickHouseErrorKeepsCurrentPools(t *testing.T) {
	clickHouseErr := &goch.Error{Code: 62, Message: "syntax error"}
	primaryDriver := &operationDriver{execErr: clickHouseErr}
	secondaryDriver := &operationDriver{}
	primaryDB := newOperationDB(t, primaryDriver)
	secondaryDB := newOperationDB(t, secondaryDriver)
	dbs := []*sql.DB{primaryDB, secondaryDB}

	params := NewEndpointConnectionParams("http", "clickhouse.test", "user", "password", "", 8123)
	connection := NewConnection(params)
	var openIndex atomic.Int64
	connection.openDB = func(string, string) (*sql.DB, error) {
		return dbs[int(openIndex.Add(1)-1)], nil
	}

	err := connection.Exec(context.Background(), "broken SQL", NewQueryOptions())
	require.ErrorIs(t, err, clickHouseErr)
	require.Same(t, primaryDB, connection.dbPrimary)
	require.Same(t, secondaryDB, connection.dbSecondary)
	require.Equal(t, int64(2), openIndex.Load())
	require.Zero(t, primaryDriver.closes.Load())
	require.Zero(t, secondaryDriver.closes.Load())
	require.NoError(t, primaryDB.Close())
	require.NoError(t, secondaryDB.Close())
}

func TestCallerCancellationKeepsCurrentPools(t *testing.T) {
	primaryDriver := &operationDriver{}
	secondaryDriver := &operationDriver{}
	primaryDB := newOperationDB(t, primaryDriver)
	secondaryDB := newOperationDB(t, secondaryDriver)
	dbs := []*sql.DB{primaryDB, secondaryDB}

	connection := NewConnection(NewEndpointConnectionParams("http", "clickhouse.test", "user", "password", "", 8123))
	var openIndex atomic.Int64
	connection.openDB = func(string, string) (*sql.DB, error) {
		return dbs[int(openIndex.Add(1)-1)], nil
	}
	_, err := connection.db(context.Background(), false)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = connection.Exec(ctx, "SELECT 1", NewQueryOptions())
	require.ErrorIs(t, err, context.Canceled)
	require.Same(t, primaryDB, connection.currentDB(false))
	require.Same(t, secondaryDB, connection.currentDB(true))
	require.Zero(t, primaryDriver.closes.Load())
	require.Zero(t, secondaryDriver.closes.Load())
	require.NoError(t, primaryDB.Close())
	require.NoError(t, secondaryDB.Close())
}

func TestOldGenerationFailureDoesNotResetReplacementPools(t *testing.T) {
	oldPrimary := newOperationDB(t, &operationDriver{})
	newPrimary := newOperationDB(t, &operationDriver{})
	newSecondary := newOperationDB(t, &operationDriver{})
	connection := NewConnection(NewEndpointConnectionParams("http", "clickhouse.test", "", "", "", 8123))
	connection.dbPrimary = newPrimary
	connection.dbSecondary = newSecondary

	require.False(t, connection.resetPoolsIfCurrent(oldPrimary))
	require.Same(t, newPrimary, connection.dbPrimary)
	require.Same(t, newSecondary, connection.dbSecondary)
	require.NoError(t, oldPrimary.Close())
	require.NoError(t, newPrimary.Close())
	require.NoError(t, newSecondary.Close())
}

func TestConnectionFailureClassification(t *testing.T) {
	require.True(t, isConnectionFailure(context.DeadlineExceeded))
	require.True(t, isConnectionFailure(io.ErrUnexpectedEOF))
	require.True(t, isConnectionFailure(errors.New("clickhouse: upstream connect error or disconnect/reset before headers")))
	require.False(t, isConnectionFailure(&goch.Error{Code: 60, Message: "table already exists"}))
	require.False(t, isConnectionFailure(errors.New("application validation failed")))

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.False(t, shouldResetPools(context.Canceled, canceledCtx))
	require.True(t, shouldResetPools(errors.New("upstream connect error"), canceledCtx))
	require.True(t, shouldResetPools(context.DeadlineExceeded, context.Background()))
}
