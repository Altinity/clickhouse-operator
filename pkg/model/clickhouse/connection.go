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
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/pem"
	"fmt"

	goch "github.com/mailru/go-clickhouse/v2"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// const clickHouseDriverName = "clickhouse"
const clickHouseDriverName = "chhttp"

func init() {
	setupTLSBasic()
}

// Connection specifies clickhouse database connection object
type Connection struct {
	params *EndpointConnectionParams
	db     *sql.DB
	l      log.Announcer
}

// NewConnection creates new clickhouse connection
func NewConnection(params *EndpointConnectionParams) *Connection {
	// Do not establish connection immediately, do it in l lazy manner
	return &Connection{
		params: params,
		l:      log.New(),
	}

}

// Params gets connection params
func (c *Connection) Params() *EndpointConnectionParams {
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
	// ClickHouse connection may have custom TLS options specified
	c.setupTLSAdvanced()

	c.l.V(2).Info("Establishing connection: %s", c.params.GetDSNWithHiddenCredentials())
	dbConnection, err := sql.Open(clickHouseDriverName, c.params.GetDSN())
	if err != nil {
		c.l.V(1).F().Error("FAILED Open(%s). Err: %v", c.params.GetDSNWithHiddenCredentials(), err)
		return
	}

	// Ping should have timeout
	pingCtx, cancel := context.WithTimeout(c.ensureCtx(ctx), c.params.GetConnectTimeout())
	defer cancel()

	if err := dbConnection.PingContext(pingCtx); err != nil {
		c.l.V(1).F().Error("FAILED Ping(%s). Err: %v", c.params.GetDSNWithHiddenCredentials(), err)
		_ = dbConnection.Close()
		return
	}

	c.db = dbConnection
}

func setupTLSBasic() {
	goch.RegisterTLSConfig(tlsSettings, &tls.Config{
		InsecureSkipVerify: true,
	})
}

func (c *Connection) setupTLSAdvanced() {
	// Convenience wrapper
	certString := c.params.rootCA

	if certString == "" {
		c.l.V(1).F().Info("No rootCA specified, skip TLS setup")
		return
	}

	// Cert may be base64-encoded - decode base64
	certBytes, err := base64.StdEncoding.DecodeString(certString)
	if err != nil {
		// No, it is not
		c.l.V(1).F().Info("CERT is not Base64-encoded err: %v", err)
		// Treat provided cert string as PEM-encoded
		certBytes = []byte(certString)
	}

	// Cert may be PEM-encoded - decode PEM block
	block, _ := pem.Decode(certBytes)
	if block != nil {
		// Yes, it is
		c.l.V(1).F().Info("CERT is PEM-encoded")
		certBytes = block.Bytes
	} else {
		// No, it is not
		c.l.V(1).F().Info("CERT is not PEM-encoded")
		// Treat cert string as DER-encoded
		certBytes = []byte(certString)
	}

	// Parse DER-encoded cert
	cert, err := x509.ParseCertificate(certBytes)
	if err != nil {
		c.l.V(1).F().Error("unable to parse CERT specified in rootCA err: %v", err)
		return
	}

	// Certificates pool
	rootCAs := x509.NewCertPool()
	rootCAs.AddCert(cert)

	// Setup TLS
	err = goch.RegisterTLSConfig(tlsSettings, &tls.Config{
		RootCAs: rootCAs,
		InsecureSkipVerify: true, // TODO: Make it configurable
	})
	if err != nil {
		c.l.V(1).F().Error("unable to register TLS config err: %v", err)
		return
	}

	c.l.V(1).F().Info("TLS setup OK - root Cert registered")
}

// ensureConnected ensures connection is set
func (c *Connection) ensureConnected(ctx context.Context) bool {
	if c.db != nil {
		c.l.V(2).F().Info("Already connected: %s", c.params.GetDSNWithHiddenCredentials())
		return true
	}

	c.connect(ctx)

	return c.db != nil
}

// QueryContext runs given sql query on behalf of specified context
func (c *Connection) QueryContext(ctx context.Context, sql string) (*QueryResult, error) {
	if len(sql) == 0 {
		return nil, nil
	}

	if !c.ensureConnected(ctx) {
		s := fmt.Sprintf("FAILED connect(%s) for SQL: %s", c.params.GetDSNWithHiddenCredentials(), sql)
		c.l.V(1).F().Error(s)
		return nil, fmt.Errorf(s)
	}

	if util.IsContextDone(ctx) {
		return nil, ctx.Err()
	}

	// Query should have timeout
	queryCtx, cancel := context.WithTimeout(c.ensureCtx(ctx), c.params.GetQueryTimeout())

	rows, err := c.db.QueryContext(queryCtx, sql)
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

func (c *Connection) ensureCtx(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return ctx
}

// ctx creates context with deadline
func (c *Connection) ctx(ctx context.Context, opts *QueryOptions) (context.Context, context.CancelFunc) {
	return context.WithTimeout(
		c.ensureCtx(ctx),
		util.ReasonableDuration(opts.GetQueryTimeout(), c.params.GetQueryTimeout()),
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

	_, err := c.db.ExecContext(ctx, sql)

	if err != nil {
		cancel()
		c.l.V(1).F().Error("FAILED Exec(%s) %v for SQL: %s", c.params.GetDSNWithHiddenCredentials(), err, sql)
		return err
	}

	c.l.V(2).F().Info("\n%s", sql)

	return nil
}
