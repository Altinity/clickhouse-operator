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
	"github.com/golang/glog"
	_ "github.com/mailru/go-clickhouse"
	"strconv"
	"sync"
	"time"
)

const (
	// http://user:password@host:8123/
	chDsnUrlPattern = "http://%s%s:%s/"
	defaultTimeout  = 10 * time.Second
)

var (
	dbConnectMap       = sync.Map{}
	dbConnectInitMutex = sync.Mutex{}
)

func getDBConnect(dsn string) (*sqlmodule.DB, error) {
	if dbConnect, existed := dbConnectMap.Load(dsn); existed {
		return dbConnect.(*sqlmodule.DB), nil
	}

	dbConnectInitMutex.Lock()
	defer dbConnectInitMutex.Unlock()

	// double check
	if dbConnect, existed := dbConnectMap.Load(dsn); existed {
		return dbConnect.(*sqlmodule.DB), nil
	}

	dbConnect, err := sqlmodule.Open("clickhouse", dsn)
	if err != nil {
		glog.V(1).Infof("FAILED Open(%s) %v", dsn, err)
		return nil, err
	}

	// Ping should be deadlined
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(defaultTimeout))
	defer cancel()

	if err := dbConnect.PingContext(ctx); err != nil {
		glog.V(1).Infof("FAILED Ping(%s) %v", dsn, err)
		_ = dbConnect.Close()
		return nil, err
	}

	dbConnectMap.Store(dsn, dbConnect)
	return dbConnect, nil
}

type Conn struct {
	hostname string
	username string
	password string
	port     int

	timeout time.Duration
}

func New(hostname, username, password string, port int) *Conn {
	return &Conn{
		hostname: hostname,
		username: username,
		password: password,
		port:     port,
		timeout:  defaultTimeout,
	}
}

// makeUsernamePassword makes "username:password" pair for connection
func (c *Conn) makeUsernamePassword() string {
	if c.username == "" && c.password == "" {
		return ""
	}

	// password may be omitted
	if c.password == "" {
		return c.username + "@"
	}

	// Expecting both username and password to be in place
	return c.username + ":" + c.password + "@"
}

// makeDsn makes ClickHouse DSN
func (c *Conn) makeDsn() string {
	return fmt.Sprintf(chDsnUrlPattern, c.makeUsernamePassword(), c.hostname, strconv.Itoa(c.port))
}

// Query runs given sql query
func (c *Conn) Query(sql string) (*sqlmodule.Rows, error) {
	return c.QueryContext(context.Background(), sql)
}

func (c *Conn) QueryContext(ctx context.Context, sql string) (*sqlmodule.Rows, error) {
	if len(sql) == 0 {
		return nil, nil
	}

	dsn := c.makeDsn()
	connect, err := getDBConnect(dsn)
	if err != nil {
		glog.V(1).Infof("FAILED Open(%s) %v for SQL: %s", dsn, err, sql)
		return nil, err
	}

	rows, err := connect.QueryContext(ctx, sql)
	if err != nil {
		glog.V(1).Infof("FAILED Query(%s) %v for SQL: %s", dsn, err, sql)
		return nil, err
	}

	// glog.V(1).Infof("clickhouse.Query(%s):'%s'", c.Hostname, sql)

	return rows, nil
}

func (c *Conn) Exec(sql string) error {
	return c.ExecContext(context.Background(), sql)
}

// Exec runs given sql query
func (c *Conn) ExecContext(ctx context.Context, sql string) error {
	if len(sql) == 0 {
		return nil
	}

	dsn := c.makeDsn()
	connect, err := getDBConnect(dsn)
	if err != nil {
		glog.V(1).Infof("FAILED Open(%s) %v for SQL: %s", dsn, err, sql)
		return err
	}

	_, err = connect.ExecContext(ctx, sql)

	if err != nil {
		glog.V(1).Infof("FAILED Exec(%s) %v for SQL: %s", dsn, err, sql)
		return err
	}

	// glog.V(1).Infof("clickhouse.Exec(%s):'%s'", c.Hostname, sql)

	return nil
}
