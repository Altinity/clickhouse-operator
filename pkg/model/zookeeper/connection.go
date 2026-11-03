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

package zookeeper

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
	"golang.org/x/sync/semaphore"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util/tlsutil"
)

// Assert that zk.Conn implements ZKClient
var _ ZKClient = (*zk.Conn)(nil)

// Assert that zkLogger implements zk.Logger
var _ zk.Logger = zkLogger{}

// zkLogger adapts the announcer to the logger interface the zk library expects.
// The library dials in an unbounded loop of its own, printing one line per failed
// attempt straight to stderr unless given a logger.
type zkLogger struct {
	nodes api.ZookeeperNodes
}

func (l zkLogger) Printf(format string, args ...interface{}) {
	log.V(1).Info("zk conn %v: "+format, append([]interface{}{l.nodes}, args...)...)
}

type Connection struct {
	nodes api.ZookeeperNodes
	ConnectionParams
	sema       *semaphore.Weighted
	mu         sync.Mutex
	connection ZKClient

	// retryDelayFn is configurable for testing; must honor ctx cancellation.
	retryDelayFn func(ctx context.Context, i int) error
}

// NewConnection creates a new Zookeeper connection with the provided nodes and parameters.
func NewConnection(nodes api.ZookeeperNodes, _params ...*ConnectionParams) *Connection {
	params := BuildConnectionParams(_params...)
	return &Connection{
		nodes:            nodes,
		sema:             semaphore.NewWeighted(params.MaxConcurrentRequests),
		ConnectionParams: *params,
		retryDelayFn:     retryDelayFnFlooredCappedLn(1, 30),
		//retryDelayFn:     retryDelayFnLinear(),
	}
}

func retryDelayFnLinear() func(context.Context, int) error {
	return func(ctx context.Context, i int) error {
		return sleepContext(ctx, time.Duration(i)*time.Second+time.Duration(rand.Int63n(int64(1*time.Second))))
	}
}

func retryDelayFnFlooredCappedLn(floor int, cap int) func(context.Context, int) error {
	return func(ctx context.Context, i int) error {
		// Progressive delay
		base := int(math.Ceil(math.Log(float64(i + 1))))
		if base < floor {
			base = floor
		}
		if base > cap {
			base = cap
		}
		jitterFloor := 1
		jitterCap := 5
		jitter := int(rand.Int63n(int64(jitterCap)))
		if jitter < jitterFloor {
			jitter = jitterFloor
		}
		if jitter > jitterCap {
			jitter = jitterCap
		}
		return sleepContext(ctx, time.Duration(base+jitter)*time.Second)
	}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Get retrieves data from the specified path in Zookeeper.
func (c *Connection) Get(ctx context.Context, path string) (data []byte, stat *zk.Stat, err error) {
	err = c.retry(ctx, func(connection ZKClient) error {
		data, stat, err = connection.Get(path)
		return err
	})
	return
}

// Exists checks if the specified path exists in Zookeeper.
func (c *Connection) Exists(ctx context.Context, path string) (bool, error) {
	exists, _, err := c.Details(ctx, path)
	return exists, err
}

// Details retrieves existence and stat information for the specified path in Zookeeper.
func (c *Connection) Details(ctx context.Context, path string) (exists bool, stat *zk.Stat, err error) {
	err = c.retry(ctx, func(connection ZKClient) error {
		exists, stat, err = connection.Exists(path)
		return err
	})
	return
}

// Create creates a new node at the specified path with the given value, flags, and ACL.
func (c *Connection) Create(ctx context.Context, path string, value []byte, flags int32, acl []zk.ACL) (pathCreated string, err error) {
	err = c.retry(ctx, func(connection ZKClient) error {
		pathCreated, err = connection.Create(path, value, flags, acl)
		return err
	})
	return
}

// Set updates the value of the node at the specified path with the given version.
func (c *Connection) Set(ctx context.Context, path string, value []byte, version int32) (stat *zk.Stat, err error) {
	err = c.retry(ctx, func(connection ZKClient) error {
		stat, err = connection.Set(path, value, version)
		return err
	})
	return
}

// Delete removes the node at the specified path with the given version.
func (c *Connection) Delete(ctx context.Context, path string, version int32) error {
	return c.retry(ctx, func(connection ZKClient) error {
		return connection.Delete(path, version)
	})
}

// Close closes the Zookeeper connection if it exists. If the connection is nil, it does nothing.
func (c *Connection) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.connection != nil {
		c.connection.Close()
	}
	return nil
}

func (c *Connection) retry(ctx context.Context, fn func(connection ZKClient) error) error {
	if err := c.sema.Acquire(ctx, 1); err != nil {
		return err
	}
	defer c.sema.Release(1)

	var errs []error
	for i := 0; i < c.MaxRetriesNum; i++ {
		if err := ctx.Err(); err != nil {
			if len(errs) == 0 {
				return err
			}
			return fmt.Errorf("retries cancelled after %d attempt(s): %w", len(errs), errors.Join(append(errs, err)...))
		}
		if i > 0 {
			// Delay before each consequent retry (interruptible via ctx)
			if err := c.retryDelayFn(ctx, i); err != nil {
				if len(errs) == 0 {
					return err
				}
				return fmt.Errorf("retries cancelled after %d attempt(s): %w", len(errs), errors.Join(append(errs, err)...))
			}
		}

		connection, err := c.ensureConnection(ctx)
		if err != nil {
			log.V(1).Info("zk connect attempt %d/%d failed: %v", i+1, c.MaxRetriesNum, err)
			errs = append(errs, fmt.Errorf("retry %d: connection error: %w", i+1, err))
			continue // Retry
		}

		err = fn(connection)
		if err == nil {
			// Success - return nil, no need for caller to know about errors
			return nil
		}

		// Handle specific error cases
		if err == zk.ErrConnectionClosed {
			c.mu.Lock()
			if c.connection == connection {
				c.connection = nil
			}
			c.mu.Unlock()
			log.V(1).Info("zk operation attempt %d/%d failed: connection closed: %v", i+1, c.MaxRetriesNum, err)
			errs = append(errs, fmt.Errorf("retry %d: connection closed: %w", i+1, err))
			continue // Retry
		}

		// Collect the errors
		log.V(1).Info("zk operation attempt %d/%d failed: %v", i+1, c.MaxRetriesNum, err)
		errs = append(errs, fmt.Errorf("retry %d: %w", i+1, err))
	}

	//
	// All retries have failed - wrap accumulated errors
	//

	if len(errs) == 0 {
		// No errors found, just to response received
		return fmt.Errorf("max retries number reached: %d", c.MaxRetriesNum)
	}

	return fmt.Errorf("all retries (%d) have failed: %w", c.MaxRetriesNum, errors.Join(errs...))
}

func (c *Connection) ensureConnection(ctx context.Context) (ZKClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connection == nil {
		connection, events, err := c.dial(ctx)
		if err != nil {
			return nil, err
		}
		c.connection = connection
		go c.connectionEventsProcessor(connection, events)
		c.connectionAddAuth(ctx)
	}
	return c.connection, nil
}

func (c *Connection) connectionAddAuth(ctx context.Context) {
	if c.AuthFile == "" {
		return
	}
	authFileContent, err := os.ReadFile(c.AuthFile)
	if err != nil {
		log.Error("auth file: %v", err)
		return
	}
	authInfo := strings.TrimRight(string(authFileContent), "\n")
	authInfoParts := strings.SplitN(authInfo, ":", 2)
	if len(authInfoParts) != 2 {
		log.Error("failed to parse auth file content, expected format <scheme>:<auth> but saw: %s", authInfo)
		return
	}
	scheme := authInfoParts[0]
	// Reject ZK digest-auth under FIPS-compatible mode. The "digest" scheme
	// in the vendored go-zookeeper library uses SHA-1 password hashing
	// internally — strict FIPS runtime (`fips140=only`) would panic at
	// use-time, and `fips140=on` filters approved primitives. The operator's
	// FIPS scope specification (§2 line 46 / §3 step 3) requires an explicit
	// decision; we reject here when chopconf signals FIPS. Non-digest schemes
	// (sasl, x509, ip, world) are unaffected.
	if shouldRejectAuthScheme(c.RejectDigestAuth, scheme) {
		log.Error("zk auth scheme %q rejected under FIPS-compatible mode (security.policy=Enforced); use a non-digest scheme (sasl, x509) or disable FIPS", scheme)
		return
	}
	err = c.connection.AddAuth(scheme, []byte(authInfoParts[1]))
	if err != nil {
		log.Error("failed to add auth to zk connection: %v", err)
		return
	}
}

// shouldRejectAuthScheme returns true when the FIPS-rejected ZK auth scheme
// would be invoked. Pure function: extracted for testability of the predicate
// (the call site is tightly coupled to a live ZK connection).
func shouldRejectAuthScheme(rejectDigest bool, scheme string) bool {
	return rejectDigest && strings.EqualFold(scheme, "digest")
}

func (c *Connection) connectionEventsProcessor(connection ZKClient, events <-chan zk.Event) {
	for event := range events {
		switch event.State {
		case
			zk.StateExpired,
			zk.StateConnecting,
			zk.StateDisconnected:
			c.mu.Lock()
			if c.connection == connection {
				c.connection = nil
			}
			c.mu.Unlock()
			// Disconnected included: this goroutine returns right after, and nothing else
			// holds the handle, so an unclosed zk.Conn would keep re-dialing its cached
			// (by then dead) endpoints roughly once a second for the life of the process.
			connection.Close()
			log.Info("zk conn: session for addr %v ended: %v", c.nodes, event)
			return
		}
		log.Info("zk conn: session for addr %v event: %v", c.nodes, event)
	}
}

func (c *Connection) dial(ctx context.Context) (ZKClient, <-chan zk.Event, error) {
	ctx, cancel := context.WithTimeout(ctx, c.TimeoutConnect)
	defer cancel()

	connection, events, err := c.connect(ctx)
	if err != nil {
		return nil, nil, err
	}

	for {
		select {
		case <-ctx.Done():
			connection.Close()
			return nil, nil, ctx.Err()
		case event := <-events:
			switch event.State {
			case zk.StateConnected:
				return connection, events, nil
			case zk.StateAuthFailed:
				connection.Close()
				return nil, nil, fmt.Errorf("zk ensureConnection failed: StateAuthFailed")
			}
		}
	}
}

func (c *Connection) connect(ctx context.Context) (ZKClient, <-chan zk.Event, error) {
	servers := c.nodes.Servers()
	// Resolve under ctx so a stuck/missing DNS name cannot ignore cancel the way
	// zk.DNSHostProvider.Init (blocking net.LookupHost) does. Each dial attempt is
	// still bounded by TimeoutConnect; outer retry keeps waiting for ZK to appear.
	resolvedServers, err := resolveServers(ctx, servers)
	if err != nil {
		return nil, nil, err
	}

	optionsDialer := zk.WithDialer(net.DialTimeout)
	if c.CertFile != "" && c.KeyFile != "" {
		if len(servers) > 1 {
			log.Fatal("This TLS zk code requires that the all the zk servers validate to a single server name.")
		}

		// TLS ServerName must stay the configured hostname, not a resolved IP.
		serverName := strings.Split(servers[0], ":")[0]

		log.Info("Using TLS for %s", serverName)
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			log.Fatal("Unable to load cert %v and key %v, err: %v", c.CertFile, c.KeyFile, err)
		}
		clientCACert, err := os.ReadFile(c.CaFile)
		if err != nil {
			log.Fatal("Unable to open ca cert %v, err %v", c.CaFile, err)
		}

		clientCertPool := x509.NewCertPool()
		clientCertPool.AppendCertsFromPEM(clientCACert)

		tlsConfig := &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            clientCertPool,
			ServerName:         serverName,
			MinVersion:         tlsutil.VersionUint16(c.MinTLSVersion),
			InsecureSkipVerify: c.InsecureSkipVerify,
		}

		optionsDialer = zk.WithDialer(func(network, address string, timeout time.Duration) (net.Conn, error) {
			d := net.Dialer{
				Timeout: timeout,
			}

			return tls.DialWithDialer(&d, network, address, tlsConfig)
		})
	}

	// Pass resolved IP:port strings; zk's default DNSHostProvider.Init only does a
	// cheap literal lookup on those, so the ctx-aware resolve above remains the
	// bound that matters for missing/stuck DNS.
	return zk.Connect(resolvedServers, c.TimeoutKeepAlive, optionsDialer, zk.WithLogger(zkLogger{nodes: c.nodes}))
}

// lookupHostFn is overridable in tests.
var lookupHostFn = func(ctx context.Context, host string) ([]string, error) {
	return net.DefaultResolver.LookupHost(ctx, host)
}

// resolveServers resolves hostnames in host:port server strings using ctx so DNS
// respects cancel and TimeoutConnect. Returns host:port list with literal addresses
// for zk.Connect (whose default DNSHostProvider.Init is a cheap no-op on IP literals).
func resolveServers(ctx context.Context, servers []string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(servers) == 0 {
		return nil, fmt.Errorf("zk: server list must not be empty")
	}

	found := make([]string, 0, len(servers))
	for _, server := range servers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		host, port, err := net.SplitHostPort(server)
		if err != nil {
			return nil, fmt.Errorf("zk: invalid server %q: %w", server, err)
		}
		addrs, err := lookupHostFn(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("zk dns lookup %q: %w", host, err)
		}
		for _, addr := range addrs {
			found = append(found, net.JoinHostPort(addr, port))
		}
	}
	if len(found) == 0 {
		return nil, fmt.Errorf("zk: no hosts found for addresses %q", servers)
	}
	return found, nil
}
