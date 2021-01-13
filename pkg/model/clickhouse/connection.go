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
	databasesql "database/sql"
	"fmt"
	"time"

	log "github.com/golang/glog"
	// log "k8s.io/klog"

	_ "github.com/mailru/go-clickhouse"
)

type CHConnection struct {
	params *CHConnectionParams
	conn   *databasesql.DB
}

func NewConnection(params *CHConnectionParams) *CHConnection {
	// DO not perform connection immediately, do it in lazy manner
	return &CHConnection{
		params: params,
	}
}

// connectContext
func (c *CHConnection) connectContext(ctx context.Context) {

	log.V(2).Infof("Establishing connection: %s", c.params.GetDSNWithHiddenCredentials())
	dbConnection, err := databasesql.Open("clickhouse", c.params.GetDSN())
	if err != nil {
		log.V(1).Infof("FAILED Open(%s) %v", c.params.GetDSNWithHiddenCredentials(), err)
		return
	}

	// Ping should be deadlined
	var parentCtx context.Context
	if ctx == nil {
		parentCtx = context.Background()
	} else {
		parentCtx = ctx
	}
	contxt, cancel := context.WithDeadline(parentCtx, time.Now().Add(defaultTimeout))
	defer cancel()

	if err := dbConnection.PingContext(contxt); err != nil {
		log.V(1).Infof("FAILED Ping(%s) %v", c.params.GetDSNWithHiddenCredentials(), err)
		_ = dbConnection.Close()
		return
	}

	c.conn = dbConnection
}

// ensureConnectedContext
func (c *CHConnection) ensureConnectedContext(ctx context.Context) bool {
	if c.conn != nil {
		log.V(2).Infof("Already connected: %s", c.params.GetDSNWithHiddenCredentials())
		return true
	}

	c.connectContext(ctx)

	return c.conn != nil
}

// Query represents query context and results
type Query struct {
	// Query execution context
	ctx        context.Context
	cancelFunc context.CancelFunc
	// Query result rows
	Rows *databasesql.Rows
}

// Close
func (q *Query) Close() {
	if q == nil {
		return
	}

	if q.Rows != nil {
		err := q.Rows.Close()
		q.Rows = nil
		if err != nil {
			log.V(1).Infof("UNABLE to close rows. err: %v", err)
		}
	}

	if q.cancelFunc != nil {
		q.cancelFunc()
		q.cancelFunc = nil
	}
}

// QueryContext runs given sql query
func (c *CHConnection) QueryContext(ctx context.Context, sql string) (*Query, error) {
	if len(sql) == 0 {
		return nil, nil
	}

	var parentCtx context.Context
	if ctx == nil {
		parentCtx = context.Background()
	} else {
		parentCtx = ctx
	}
	contxt, cancel := context.WithDeadline(parentCtx, time.Now().Add(defaultTimeout))

	if !c.ensureConnectedContext(contxt) {
		cancel()
		s := fmt.Sprintf("FAILED connect(%s) for SQL: %s", c.params.GetDSNWithHiddenCredentials(), sql)
		log.V(1).Info(s)
		return nil, fmt.Errorf(s)
	}

	rows, err := c.conn.QueryContext(contxt, sql)
	if err != nil {
		cancel()
		s := fmt.Sprintf("FAILED Query(%s) %v for SQL: %s", c.params.GetDSNWithHiddenCredentials(), err, sql)
		log.V(1).Info(s)
		return nil, err
	}

	log.V(2).Infof("clickhouse.QueryContext():'%s'", sql)

	return &Query{
		ctx:        contxt,
		cancelFunc: cancel,
		Rows:       rows,
	}, nil
}

// ExecContext runs given sql query
func (c *CHConnection) ExecContext(ctx context.Context, sql string) error {
	if len(sql) == 0 {
		return nil
	}

	var parentCtx context.Context
	if ctx == nil {
		parentCtx = context.Background()
	} else {
		parentCtx = ctx
	}
	contxt, cancel := context.WithDeadline(parentCtx, time.Now().Add(defaultTimeout))
	defer cancel()

	if !c.ensureConnectedContext(contxt) {
		cancel()
		s := fmt.Sprintf("FAILED connect(%s) for SQL: %s", c.params.GetDSNWithHiddenCredentials(), sql)
		log.V(1).Info(s)
		return fmt.Errorf(s)
	}

	_, err := c.conn.ExecContext(contxt, sql)

	if err != nil {
		cancel()
		log.V(1).Infof("FAILED Exec(%s) %v for SQL: %s", c.params.GetDSNWithHiddenCredentials(), err, sql)
		return err
	}

	log.V(2).Infof("clickhouse.Exec():\n", sql)

	return nil
}
