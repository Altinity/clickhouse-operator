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
	sqlmodule "database/sql"
	"fmt"
	"time"

	log "github.com/golang/glog"
	// log "k8s.io/klog"

	_ "github.com/mailru/go-clickhouse"
)

type CHConnection struct {
	params *CHConnectionParams
	conn   *sqlmodule.DB
}

func NewConnection(params *CHConnectionParams) *CHConnection {
	// DO not perform connection immediately, do it in lazy manner
	return &CHConnection{
		params: params,
	}
}

func (c *CHConnection) connect() {

	log.V(2).Infof("Establishing connection: %s", c.params.GetDSNWithHiddenCredentials())
	dbConnection, err := sqlmodule.Open("clickhouse", c.params.GetDSN())
	if err != nil {
		log.V(1).Infof("FAILED Open(%s) %v", c.params.GetDSNWithHiddenCredentials(), err)
		return
	}

	// Ping should be deadlined
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(defaultTimeout))
	defer cancel()

	if err := dbConnection.PingContext(ctx); err != nil {
		log.V(1).Infof("FAILED Ping(%s) %v", c.params.GetDSNWithHiddenCredentials(), err)
		_ = dbConnection.Close()
		return
	}

	c.conn = dbConnection
}

func (c *CHConnection) ensureConnected() bool {
	if c.conn != nil {
		log.V(2).Infof("Already connected: %s", c.params.GetDSNWithHiddenCredentials())
		return true
	}

	c.connect()

	return c.conn != nil
}

// Query
type Query struct {
	ctx        context.Context
	cancelFunc context.CancelFunc

	Rows *sqlmodule.Rows
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

// Query runs given sql query
func (c *CHConnection) Query(sql string) (*Query, error) {
	if len(sql) == 0 {
		return nil, nil
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(defaultTimeout))

	if !c.ensureConnected() {
		cancel()
		s := fmt.Sprintf("FAILED connect(%s) for SQL: %s", c.params.GetDSNWithHiddenCredentials(), sql)
		log.V(1).Info(s)
		return nil, fmt.Errorf(s)
	}

	rows, err := c.conn.QueryContext(ctx, sql)
	if err != nil {
		cancel()
		s := fmt.Sprintf("FAILED Query(%s) %v for SQL: %s", c.params.GetDSNWithHiddenCredentials(), err, sql)
		log.V(1).Info(s)
		return nil, err
	}

	log.V(2).Infof("clickhouse.QueryContext():'%s'", sql)

	return &Query{
		ctx:        ctx,
		cancelFunc: cancel,
		Rows:       rows,
	}, nil
}

// Exec runs given sql query
func (c *CHConnection) Exec(sql string) error {
	if len(sql) == 0 {
		return nil
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(defaultTimeout))
	defer cancel()

	if !c.ensureConnected() {
		s := fmt.Sprintf("FAILED connect(%s) for SQL: %s", c.params.GetDSNWithHiddenCredentials(), sql)
		log.V(1).Info(s)
		return fmt.Errorf(s)
	}

	_, err := c.conn.ExecContext(ctx, sql)

	if err != nil {
		log.V(1).Infof("FAILED Exec(%s) %v for SQL: %s", c.params.GetDSNWithHiddenCredentials(), err, sql)
		return err
	}

	log.V(2).Infof("clickhouse.Exec():\n", sql)

	return nil
}
