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
	"database/sql/driver"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	goch "github.com/mailru/go-clickhouse/v2"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
	"github.com/altinity/clickhouse-operator/pkg/util/tlsutil"
)

// const clickHouseDriverName = "clickhouse"
const clickHouseDriverName = "chhttp"

// Keep a finite lifetime as defense in depth for connections that never
// surface an error. Connection-class failures reset the pools immediately.
const defaultDBConnectionMaxLifetime = 10 * time.Minute

// defaultDBConnectionIdleTimeDivisor derives the idle cap from the lifetime cap, so the two cannot
// drift apart. 5 gives a 2m idle cap against the 10m lifetime - short enough that a socket rarely
// survives the gap between reconciles, long enough that a busy reconcile does not re-handshake.
const defaultDBConnectionIdleTimeDivisor = 5

type dbOpener func(driverName, dataSourceName string) (*sql.DB, error)

type poolInitialization struct {
	done chan struct{}
	err  error
}

// init registers the legacy `tlsSettingsLegacy` DSN key with an insecure
// TLS config for pre-0.27.1 back-compat. Under FIPS-enforced startup the
// chop fipsGate (cmd/operator/app/fips_gate.go, cmd/metrics_exporter/app/
// fips_gate.go) calls EnforceVerifiedLegacyTLS *before* any DB connect
// is opened, re-registering the same key with a verifying config. This
// invariant is load-bearing: any new caller that establishes a ClickHouse
// connection before fipsGate runs would bypass the verified-TLS override.
// Don't import this package from early-init paths (flag parsers, version
// commands) without keeping the gate-first ordering intact.
func init() {
	setupTLSBasic()
}

// Connection specifies clickhouse database connection object
type Connection struct {
	params      *EndpointConnectionParams
	dbPrimary   *sql.DB
	dbSecondary *sql.DB
	poolMutex   sync.RWMutex
	poolInit    *poolInitialization
	openDB      dbOpener
	l           log.Announcer
}

// NewConnection creates new clickhouse connection
func NewConnection(params *EndpointConnectionParams) *Connection {
	// Do not establish connection immediately, do it in l lazy manner
	return &Connection{
		params: params,
		openDB: sql.Open,
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

// openPools creates and verifies a new pair of ClickHouse pools without
// publishing them to the connection. Callers can therefore perform the slow
// network work without holding poolMutex.
func (c *Connection) openPools(ctx context.Context) (*sql.DB, *sql.DB, error) {
	// ClickHouse connection may have custom TLS options specified
	c.setupTLSAdvanced()

	c.l.V(2).Info("Establishing connection: %s", c.params.GetDSNWithHiddenCredentials())
	dbPrimaryConn, err := c.openDB(clickHouseDriverName, c.params.GetDSN())
	if err != nil {
		c.l.V(1).F().Error("FAILED Open(%s). Err: %v", c.params.GetDSNWithHiddenCredentials(), err)
		return nil, nil, err
	}
	configureDBConnectionPool(dbPrimaryConn, defaultDBConnectionMaxLifetime)

	dbSecondaryConn, err := c.openDB(clickHouseDriverName, c.params.GetDSNLogQueries())
	if err != nil {
		c.l.V(1).F().Error("FAILED Open2(%s). Err: %v", c.params.GetDSNWithHiddenCredentials(), err)
		closePools(dbPrimaryConn, nil)
		return nil, nil, err
	}
	configureDBConnectionPool(dbSecondaryConn, defaultDBConnectionMaxLifetime)

	// Ping should have timeout
	pingCtxPrimary, cancel1 := context.WithTimeout(c.ensureCtx(ctx), c.params.GetConnectTimeout())
	defer cancel1()

	if err := dbPrimaryConn.PingContext(pingCtxPrimary); err != nil {
		c.l.V(1).F().Error("FAILED Ping(%s). Err: %v", c.params.GetDSNWithHiddenCredentials(), err)
		closePools(dbPrimaryConn, dbSecondaryConn)
		return nil, nil, err
	}

	pingCtxSecondary, cancel2 := context.WithTimeout(c.ensureCtx(ctx), c.params.GetConnectTimeout())
	defer cancel2()

	if err := dbSecondaryConn.PingContext(pingCtxSecondary); err != nil {
		c.l.V(1).F().Error("FAILED Ping2(%s). Err: %v", c.params.GetDSNWithHiddenCredentials(), err)
		closePools(dbPrimaryConn, dbSecondaryConn)
		return nil, nil, err
	}

	return dbPrimaryConn, dbSecondaryConn, nil
}

func configureDBConnectionPool(db *sql.DB, maxLifetime time.Duration) {
	db.SetConnMaxLifetime(maxLifetime)
	// ConnMaxLifetime alone does not bound this failure. The stale thing here is an IDLE keep-alive
	// socket: each driver conn owns an http.Transport whose IdleConnTimeout defaults to 1h
	// (vendor/github.com/mailru/go-clickhouse/v2/config.go), and between reconciles the operator's
	// connections sit idle for exactly that kind of interval. Capping idle time means a socket that
	// nobody has used recently is retired before it can be handed to the next reconcile pointing at
	// a Pod IP that no longer exists - which is the case the reactive reset cannot catch, because
	// with no traffic there is no error to classify.
	db.SetConnMaxIdleTime(maxLifetime / defaultDBConnectionIdleTimeDivisor)
}

func closePools(primary, secondary *sql.DB) {
	if primary != nil {
		_ = primary.Close()
	}
	if secondary != nil && secondary != primary {
		_ = secondary.Close()
	}
}

func setupTLSBasic() {
	// Register a legacy-keyed config so DSNs referencing tlsSettingsLegacy work
	// out of the box for endpoints that don't set any TLS knob (preserves the
	// legacy single-registration behavior).
	goch.RegisterTLSConfig(tlsSettingsLegacy, &tls.Config{
		InsecureSkipVerify: true,
	})
}

// EnforceVerifiedLegacyTLS re-registers the legacy tlsSettingsLegacy key with a
// verifying tls.Config (system trust store, no InsecureSkipVerify). Called
// from the FIPS startup gate when chopconf.security.policy=Enforced so any
// DSN that didn't go through setupTLSAdvanced still gets verified TLS rather
// than the default-insecure legacy registration. Pre-FIPS behavior preserved
// by NOT calling this when FIPS is disabled.
//
// MinVersion is set explicitly to TLS 1.2 (the FIPS spec floor) rather than
// relying on the Go stdlib default. 1.2 is chosen over 1.3 because this legacy
// code path serves unkeyed DSNs that may target older ClickHouse servers;
// users can raise the floor at the per-cluster security.clickhouse.tls.minVersion
// knob without affecting this legacy fallback.
func EnforceVerifiedLegacyTLS() {
	goch.RegisterTLSConfig(tlsSettingsLegacy, legacyVerifiedTLSConfig())
}

// legacyVerifiedTLSConfig builds the verifying tls.Config that
// EnforceVerifiedLegacyTLS registers. Extracted as a pure helper so unit tests
// can pin the InsecureSkipVerify polarity and explicit MinVersion floor
// without reaching into the driver's unexported registry.
func legacyVerifiedTLSConfig() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: false,
		MinVersion:         tls.VersionTLS12,
	}
}

// setupTLSAdvanced builds and registers the tls.Config for this connection's
// endpoint under a content-hash key (EndpointCredentials.TLSConfigKey). Two
// endpoints with identical security knobs share one registered config; two
// endpoints with different knobs register under different keys — so concurrent
// reconciles of differently-configured clusters cannot race.
//
// Honors every security knob even when rootCA is empty: a user setting
// verify=Strict gets a verifying tls.Config that falls back to the system trust
// store (Go's stdlib semantics for tls.Config{RootCAs: nil}).
//
// Verify semantics: empty verify with no other knobs takes the legacy path
// (InsecureSkipVerify=true, preserves pre-0.27.1 behavior). Empty verify with
// other knobs (e.g. user set minVersion but not verify) is treated as Strict —
// users opting into TLS hardening should not silently get InsecureSkipVerify=true.
func (c *Connection) setupTLSAdvanced() {
	// Nothing to do for HTTP DSNs.
	if c.params.scheme != api.ChSchemeHTTPS {
		return
	}

	verify := c.params.TLSVerify()
	minVersion := c.params.TLSMinVersion()
	serverName := c.params.TLSServerName()
	certString := c.params.rootCA

	// Legacy path: no knobs set at all → the basic registration from setupTLSBasic
	// already covers this DSN (InsecureSkipVerify=true, default MinVersion).
	if (verify == "") && (minVersion == "") && (serverName == "") && (certString == "") {
		c.l.V(1).F().Info("TLS setup: no security knobs set, using legacy registration")
		return
	}

	insecure := resolveInsecureSkipVerify(verify, minVersion, serverName)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: insecure,
		MinVersion:         tlsutil.VersionUint16(string(minVersion)),
	}
	if serverName != "" {
		tlsConfig.ServerName = serverName
	}

	if certString != "" {
		rootCAs, err := parseRootCAs(certString, c.l)
		if err != nil {
			// User supplied a rootCA but it didn't parse. Refuse to register when
			// verification is on — silently falling through to the system trust
			// store would be a surprising downgrade. With verify=None the bytes
			// were going to be ignored anyway, so log and continue.
			if !insecure {
				c.l.V(1).F().Error("unparseable rootCA with verifying TLS — refusing to register TLS config for %s: %v",
					c.params.GetDSNWithHiddenCredentials(), err)
				return
			}
			c.l.V(1).F().Info("unparseable rootCA but verify=None — proceeding without RootCAs: %v", err)
		} else {
			tlsConfig.RootCAs = rootCAs
		}
	}

	if err := goch.RegisterTLSConfig(c.params.TLSConfigKey(), tlsConfig); err != nil {
		c.l.V(1).F().Error("unable to register TLS config err: %v", err)
		return
	}

	c.l.V(1).F().Info("TLS setup OK - registered as %q (verify=%s minVersion=%s serverName=%s rootCA=%t)",
		c.params.TLSConfigKey(), verify, minVersion, serverName, certString != "")
}

// resolveInsecureSkipVerify is the pure-function form of the InsecureSkipVerify
// polarity used by setupTLSAdvanced. Exposed as a separate function so its
// truth table is unit-testable without constructing a Connection.
//
// Distinguishes:
//   - explicit verify=Strict      → secure (false)
//   - explicit verify=None        → insecure (true) regardless of other knobs
//   - empty verify + minVersion or serverName set → secure (those knobs are
//     opt-in to TLS hardening; without this, minVersion=1.3 alone would
//     silently leave InsecureSkipVerify=true)
//   - empty verify + nothing else → INSECURE (legacy: pre-0.27.1 rootCA-only
//     coexisted with InsecureSkipVerify=true; promoting rootCA-only to strict
//     would break existing CHIs that supply a CA payload for the auth path
//     without expecting hostname/chain verification). rootCA is not an opt-in
//     signal here.
func resolveInsecureSkipVerify(verify api.TLSVerify, minVersion api.TLSMinVersion, serverName string) bool {
	if verify == api.TLSVerifyNone {
		return true
	}
	hardeningOptIn := (verify == api.TLSVerifyStrict) || (minVersion != "") || (serverName != "")
	return !hardeningOptIn
}

// parseRootCAs decodes a rootCA payload (PEM, base64-wrapped PEM, or raw DER)
// into a populated CertPool. Returns the parsed pool on success; returns a
// non-nil error describing the parse failure on failure.
//
// Decode order: try base64-decode → try PEM-decode on the result → try DER on
// what remains. Each step has a documented fallback: if base64 fails, treat the
// original string as already PEM or DER; if PEM fails on base64-decoded bytes,
// keep those bytes for the DER attempt (do NOT discard them back to the original
// string — that was a pre-existing bug).
func parseRootCAs(certString string, l log.Announcer) (*x509.CertPool, error) {
	certBytes, b64Err := base64.StdEncoding.DecodeString(certString)
	if b64Err != nil {
		l.V(1).F().Info("CERT is not Base64-encoded err: %v", b64Err)
		certBytes = []byte(certString)
	}
	if block, _ := pem.Decode(certBytes); block != nil {
		l.V(1).F().Info("CERT is PEM-encoded")
		certBytes = block.Bytes
	} else {
		l.V(1).F().Info("CERT is not PEM-encoded; trying DER on current bytes")
	}
	cert, err := x509.ParseCertificate(certBytes)
	if err != nil {
		return nil, fmt.Errorf("rootCA parse failed: %w", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return pool, nil
}

// currentDB returns a stable snapshot of the requested pool. Callers never
// retain the lock while performing SQL or network I/O.
func (c *Connection) currentDB(logQueries bool) *sql.DB {
	c.poolMutex.RLock()
	defer c.poolMutex.RUnlock()

	if logQueries {
		return c.dbSecondary
	}
	return c.dbPrimary
}

// startPoolInitialization returns the in-flight initialization or starts one.
// Publishing and generation changes are protected by poolMutex, while opening
// and pinging the database connections happen asynchronously without the lock.
func (c *Connection) startPoolInitialization() *poolInitialization {
	c.poolMutex.Lock()
	if c.dbPrimary != nil {
		c.poolMutex.Unlock()
		return nil
	}
	if c.poolInit != nil {
		init := c.poolInit
		c.poolMutex.Unlock()
		return init
	}

	init := &poolInitialization{done: make(chan struct{})}
	c.poolInit = init
	c.poolMutex.Unlock()

	go func() {
		primary, secondary, err := c.openPools(context.Background())

		c.poolMutex.Lock()
		if err == nil && c.dbPrimary == nil {
			c.dbPrimary, c.dbSecondary = primary, secondary
			primary, secondary = nil, nil
		}
		init.err = err
		if c.poolInit == init {
			c.poolInit = nil
		}
		close(init.done)
		c.poolMutex.Unlock()

		// A concurrent generation may already have won publication. Do not leak
		// the verified-but-unused pair in that case.
		closePools(primary, secondary)
	}()

	return init
}

// db returns the requested pool, initializing both pools once when necessary.
// Initialization is shared per host, but each waiter can stop waiting as soon
// as its own context is canceled. The initializer uses bounded Ping contexts
// in openPools and may finish publishing a healthy pool for other callers.
func (c *Connection) db(ctx context.Context, logQueries bool) (*sql.DB, error) {
	if db := c.currentDB(logQueries); db != nil {
		return db, nil
	}

	init := c.startPoolInitialization()

	if init != nil {
		waitCtx := c.ensureCtx(ctx)
		select {
		case <-waitCtx.Done():
			return nil, waitCtx.Err()
		case <-init.done:
			if init.err != nil {
				return nil, init.err
			}
		}
	}

	db := c.currentDB(logQueries)
	if db == nil {
		return nil, errors.New("ClickHouse connection pool is unavailable")
	}
	return db, nil
}

// resetPoolsIfCurrent atomically detaches both pools only when failedDB still
// belongs to the active generation. A delayed failure from an older operation
// must not tear down pools that another retry has already created.
func (c *Connection) resetPoolsIfCurrent(failedDB *sql.DB) bool {
	c.poolMutex.Lock()
	if failedDB == nil || (failedDB != c.dbPrimary && failedDB != c.dbSecondary) {
		c.poolMutex.Unlock()
		return false
	}

	primary, secondary := c.dbPrimary, c.dbSecondary
	c.dbPrimary, c.dbSecondary = nil, nil
	c.poolMutex.Unlock()

	c.l.V(1).F().Info("Resetting ClickHouse connection pools after connection-class failure: %s", c.params.GetDSNWithHiddenCredentials())
	closePools(primary, secondary)
	return true
}

// isConnectionFailure distinguishes failures for which retaining pooled HTTP
// transports is unsafe from normal ClickHouse query errors. The original error
// is always returned; invalidation only affects the next operator retry.
func isConnectionFailure(err error) bool {
	if err == nil {
		return false
	}

	var clickHouseErr *goch.Error
	if errors.As(err, &clickHouseErr) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	message := strings.ToLower(err.Error())
	for _, marker := range []string{
		"transport failed",
		"failed to read response",
		"upstream connect error",
		"no healthy upstream",
		"upstream request timeout",
		"connection reset",
		"connection refused",
		"broken pipe",
		"unexpected eof",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

// shouldResetPools excludes cancellation imposed by the caller. An internal
// query timeout or a transport failure still invalidates the active generation.
func shouldResetPools(err error, callerCtx context.Context) bool {
	callerCanceled := callerCtx != nil && callerCtx.Err() != nil
	contextFailure := errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
	if callerCanceled && contextFailure {
		return false
	}
	return isConnectionFailure(err)
}

// QueryContext runs given sql query on behalf of specified context
func (c *Connection) QueryContext(ctx context.Context, sql string) (*QueryResult, error) {
	if len(sql) == 0 {
		return nil, nil
	}

	db, err := c.db(ctx, false)
	if err != nil {
		s := fmt.Sprintf("FAILED connect(%s) for SQL: %s", c.params.GetDSNWithHiddenCredentials(), sql)
		c.l.V(1).F().Error(s)
		return nil, fmt.Errorf("%s: %w", s, err)
	}

	if util.IsContextDone(ctx) {
		return nil, ctx.Err()
	}

	// Query should have timeout
	queryCtx, cancel := context.WithTimeout(c.ensureCtx(ctx), c.params.GetQueryTimeout())

	rows, err := db.QueryContext(queryCtx, sql)
	if err != nil {
		cancel()
		if shouldResetPools(err, ctx) {
			c.resetPoolsIfCurrent(db)
		}
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

	db, err := c.db(ctx, opts.GetLogQueries())
	if err != nil {
		cancel()
		s := fmt.Sprintf("FAILED connect(%s) for SQL: %s", c.params.GetDSNWithHiddenCredentials(), sql)
		c.l.V(1).F().Error(s)
		return fmt.Errorf("%s: %w", s, err)
	}

	_, err = db.ExecContext(ctx, sql)

	if err != nil {
		cancel()
		if shouldResetPools(err, _ctx) {
			c.resetPoolsIfCurrent(db)
		}
		c.l.V(1).F().Error("FAILED Exec(%s) %v for SQL: %s", c.params.GetDSNWithHiddenCredentials(), err, sql)
		return err
	}

	c.l.V(2).F().Info("\n%s", sql)

	return nil
}
