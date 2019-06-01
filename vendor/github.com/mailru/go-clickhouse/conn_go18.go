// +build go1.8

package clickhouse

import (
	"context"
	"database/sql/driver"
	"net/http"
	"net/url"
)

// Ping implements the driver.Pinger
func (c *conn) Ping(ctx context.Context) error {
	if c.transport == nil {
		return driver.ErrBadConn
	}
	// make request with empty body, response should be "Ok"
	u := &url.URL{Scheme: c.url.Scheme, User: c.url.User, Host: c.url.Host, Path: "/"}
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	resp, err := c.doRequest(ctx, req)
	if err != nil || len(resp) < 7 || string(resp[4:7]) != "Ok." {
		return driver.ErrBadConn
	}
	return nil
}

// BeginTx implements the driver.ConnBeginTx
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.beginTx(ctx)
}

// PrepareContext implements the driver.ConnPrepareContext
func (c *conn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	return c.prepare(query)
}

// ExecContext implements the driver.ExecerContext
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	values, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	return c.exec(ctx, query, values)
}

// QueryContext implements the driver.QueryerContext
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	values, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	return c.query(ctx, query, values)
}

func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(named))
	for n, param := range named {
		if len(param.Name) > 0 {
			// TODO: support the use of Named Parameters #561
			return nil, ErrNameParams
		}
		dargs[n] = param.Value
	}
	return dargs, nil
}
