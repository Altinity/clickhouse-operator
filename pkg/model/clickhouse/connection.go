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
	"crypto/tls"
	databasesql "database/sql"
	"fmt"
	"time"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/util"

	// go-clickhouse is explicitly required in order to setup connection to clickhouse db
	goch "github.com/mailru/go-clickhouse"
)

func init() {
	goch.RegisterTLSConfig(tlsSettings, &tls.Config{InsecureSkipVerify: true})
}

// Connection specifies clickhouse database connection object
type Connection struct {
	params *ConnectionParams
	conn   *databasesql.DB
	l      log.Announcer
}

// NewConnection creates new clickhouse connection
func NewConnection(params *ConnectionParams) *Connection {
	// Do not establish connection immediately, do it in l lazy manner
	return &Connection{
		params: params,
		l:      log.New(),
	}

}

// Params gets connection params
func (c *Connection) Params() *ConnectionParams {
	if c == nil {
		return nil
	}
	return c.params
}

// SetLog sets log announcer
func (c *Connection) SetLog(l log.Announcer) *Connection {
	if c == nil {
		return nil
	}
	c.l = l
	return c
}

// connect performs connect
func (c *Connection) connect(ctx context.Context) {
	c.l.V(2).Info("Establishing connection: %s", c.params.GetDSNWithHiddenCredentials())
	dbConnection, err := databasesql.Open("clickhouse", c.params.GetDSN())
	if err != nil {
		c.l.V(1).F().Error("FAILED Open(%s). Err: %v", c.params.GetDSNWithHiddenCredentials(), err)
		return
	}

	// Ping should be deadlined
	var parentCtx context.Context
	if ctx == nil {
		parentCtx = context.Background()
	} else {
		parentCtx = ctx
	}
	pingCtx, cancel := context.WithDeadline(parentCtx, time.Now().Add(c.params.GetConnectTimeout()))
	defer cancel()

	if err := dbConnection.PingContext(pingCtx); err != nil {
		c.l.V(1).F().Error("FAILED Ping(%s). Err: %v", c.params.GetDSNWithHiddenCredentials(), err)
		_ = dbConnection.Close()
		return
	}

	c.conn = dbConnection
}

// ensureConnected ensures connection is set
func (c *Connection) ensureConnected(ctx context.Context) bool {
	if c.conn != nil {
		c.l.V(2).F().Info("Already connected: %s", c.params.GetDSNWithHiddenCredentials())
		return true
	}

	c.connect(ctx)

	return c.conn != nil
}

// QueryContext runs given sql query on behalf of specified context
func (c *Connection) QueryContext(ctx context.Context, sql string) (*QueryResult, error) {
	if len(sql) == 0 {
		return nil, nil
	}

	var parentCtx context.Context
	if ctx == nil {
		parentCtx = context.Background()
	} else {
		parentCtx = ctx
	}
	queryCtx, cancel := context.WithDeadline(parentCtx, time.Now().Add(c.params.GetQueryTimeout()))

	if !c.ensureConnected(queryCtx) {
		cancel()
		s := fmt.Sprintf("FAILED connect(%s) for SQL: %s", c.params.GetDSNWithHiddenCredentials(), sql)
		c.l.V(1).F().Error(s)
		return nil, fmt.Errorf(s)
	}

	rows, err := c.conn.QueryContext(queryCtx, sql)
	if err != nil {
		cancel()
		s := fmt.Sprintf("FAILED Query(%s) %v for SQL: %s", c.params.GetDSNWithHiddenCredentials(), err, sql)
		c.l.V(1).F().Error(s)
		return nil, err
	}

	c.l.V(2).Info("clickhouse.QueryContext():'%s'", sql)

	return NewQueryResult(queryCtx, cancel, rows), nil
}

// Query runs given sql query
func (c *Connection) Query(sql string) (*QueryResult, error) {
	return c.QueryContext(nil, sql)
}

// ctx creates context with deadline
func (c *Connection) ctx(ctx context.Context, opts *QueryOptions) (context.Context, context.CancelFunc) {
	var parentCtx context.Context
	if ctx == nil {
		parentCtx = context.Background()
	} else {
		parentCtx = ctx
	}
	return context.WithDeadline(
		parentCtx,
		time.Now().Add(
			util.ReasonableDuration(opts.GetQueryTimeout(), c.params.GetQueryTimeout()),
		),
	)
}

// Exec runs given sql query
func (c *Connection) Exec(_ctx context.Context, sql string, opts *QueryOptions) error {
	if len(sql) == 0 {
		return nil
	}

	ctx, cancel := c.ctx(_ctx, opts)
	defer cancel()

	if !c.ensureConnected(ctx) {
		cancel()
		s := fmt.Sprintf("FAILED connect(%s) for SQL: %s", c.params.GetDSNWithHiddenCredentials(), sql)
		c.l.V(1).F().Error(s)
		return fmt.Errorf(s)
	}

	_, err := c.conn.ExecContext(ctx, sql)

	if err != nil {
		cancel()
		c.l.V(1).F().Error("FAILED Exec(%s) %v for SQL: %s", c.params.GetDSNWithHiddenCredentials(), err, sql)
		return err
	}

	c.l.V(2).F().Info("\n%s", sql)

	return nil
}
