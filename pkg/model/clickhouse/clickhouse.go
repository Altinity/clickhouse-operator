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
	"database/sql"
	"fmt"
	"github.com/golang/glog"
	_ "github.com/mailru/go-clickhouse"
	"strconv"
)

const (
	// http://user:password@host:8123/
	chDsnUrlPattern = "http://%s%s:%s/"
)

type Conn struct {
	Hostname string
	Username string
	Password string
	Port     int
}

func New(hostname, username, password string, port int) *Conn {
	return &Conn{
		Hostname: hostname,
		Username: username,
		Password: password,
		Port:     port,
	}
}

// makeUsernamePassword makes "username:password" pair for connection
func (c *Conn) makeUsernamePassword() string {
	if c.Username == "" && c.Password == "" {
		return ""
	}

	// password may be omitted
	if c.Password == "" {
		return c.Username + "@"
	}

	// Expecting both username and password to be in place
	return c.Username + ":" + c.Password + "@"
}

// makeDsn makes ClickHouse DSN
func (c *Conn) makeDsn() string {
	return fmt.Sprintf(chDsnUrlPattern, c.makeUsernamePassword(), c.Hostname, strconv.Itoa(c.Port))
}

// Query runs given sql query
func (c *Conn) Query(query string) (*sql.Rows, error) {
	if len(query) == 0 {
		return nil, nil
	}

	dsn := c.makeDsn()
	//glog.V(1).Infof("Query ClickHouse DSN: %s", dsn)
	connect, err := sql.Open("clickhouse", dsn)
	if err != nil {
		glog.V(1).Infof("sql.Open(%s) FAILED %v", dsn, err)
		return nil, err
	}

	if err := connect.Ping(); err != nil {
		glog.V(1).Infof("connect.Ping(%s) FAILED %v", dsn, err)
		return nil, err
	}

	rows, err := connect.Query(query)
	if err != nil {
		glog.V(1).Infof("connect.Query(%s) FAILED %v", dsn, err)
		return nil, err
	}

	// glog.V(1).Infof("clickhouse.Query(%s):'%s'", c.Hostname, query)

	return rows, nil
}

// Exec runs given sql query
func (c *Conn) Exec(query string) error {
	if len(query) == 0 {
		return nil
	}

	dsn := c.makeDsn()
	//glog.V(1).Infof("Exec ClickHouse DSN: %s", dsn)
	connect, err := sql.Open("clickhouse", dsn)
	if err != nil {
		glog.V(1).Infof("sql.Open(%s) FAILED %v", dsn, err)
		return err
	}

	if err := connect.Ping(); err != nil {
		glog.V(1).Infof("connect.Ping(%d) FAILED %v", dsn, err)
		return err
	}

	_, err = connect.Exec(query)

	if err != nil {
		glog.V(1).Infof("connect.Exec(%s) FAILED %v", dsn, err)
		return err
	}

	// glog.V(1).Infof("clickhouse.Exec(%s):'%s'", c.Hostname, query)

	return nil
}
