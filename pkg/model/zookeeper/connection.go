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
	"fmt"
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
)

type Connection struct {
	nodes api.ZookeeperNodes
	ConnectionParams
	sema       *semaphore.Weighted
	mu         sync.Mutex
	connection *zk.Conn
}

func NewConnection(nodes api.ZookeeperNodes, _params ...*ConnectionParams) *Connection {
	var params *ConnectionParams
	if len(_params) > 0 {
		params = _params[0]
	}
	params = params.Normalize()
	return &Connection{
		nodes:            nodes,
		sema:             semaphore.NewWeighted(params.MaxConcurrentRequests),
		ConnectionParams: *params,
	}
}

func (c *Connection) Get(ctx context.Context, path string) (data []byte, stat *zk.Stat, err error) {
	err = c.retry(ctx, func(connection *zk.Conn) error {
		data, stat, err = connection.Get(path)
		return err
	})
	return
}

func (c *Connection) Exists(ctx context.Context, path string) bool {
	exists, _, _ := c.Details(ctx, path)
	return exists
}

func (c *Connection) Details(ctx context.Context, path string) (exists bool, stat *zk.Stat, err error) {
	err = c.retry(ctx, func(connection *zk.Conn) error {
		exists, stat, err = connection.Exists(path)
		return err
	})
	return
}

func (c *Connection) Create(ctx context.Context, path string, value []byte, flags int32, acl []zk.ACL) (pathCreated string, err error) {
	err = c.retry(ctx, func(connection *zk.Conn) error {
		pathCreated, err = connection.Create(path, value, flags, acl)
		return err
	})
	return
}

func (c *Connection) Set(ctx context.Context, path string, value []byte, version int32) (stat *zk.Stat, err error) {
	err = c.retry(ctx, func(connection *zk.Conn) error {
		stat, err = connection.Set(path, value, version)
		return err
	})
	return
}

func (c *Connection) Delete(ctx context.Context, path string, version int32) error {
	return c.retry(ctx, func(connection *zk.Conn) error {
		return connection.Delete(path, version)
	})
}

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

func (c *Connection) retry(ctx context.Context, fn func(*zk.Conn) error) error {
	if err := c.sema.Acquire(ctx, 1); err != nil {
		return err
	}
	defer c.sema.Release(1)

	for i := 0; i < c.MaxRetriesNum; i++ {
		if i > 0 {
			// Progressive delay before each retry
			time.Sleep(time.Duration(i)*time.Second + time.Duration(rand.Int63n(int64(1*time.Second))))
		}

		connection, err := c.ensureConnection(ctx)
		if err != nil {
			continue // Retry
		}

		err = fn(connection)
		if err == zk.ErrConnectionClosed {
			c.mu.Lock()
			if c.connection == connection {
				c.connection = nil
			}
			c.mu.Unlock()
			continue // Retry
		}

		// Got result
		return err
	}

	return fmt.Errorf("max retries number reached")
}

func (c *Connection) ensureConnection(ctx context.Context) (*zk.Conn, error) {
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
	err = c.connection.AddAuth(authInfoParts[0], []byte(authInfoParts[1]))
	if err != nil {
		log.Error("failed to add auth to zk connection: %v", err)
		return
	}
}

func (c *Connection) connectionEventsProcessor(connection *zk.Conn, events <-chan zk.Event) {
	for event := range events {
		shouldCloseConnection := false
		switch event.State {
		case
			zk.StateExpired,
			zk.StateConnecting:
			shouldCloseConnection = true
			fallthrough
		case zk.StateDisconnected:
			c.mu.Lock()
			if c.connection == connection {
				c.connection = nil
			}
			c.mu.Unlock()
			if shouldCloseConnection {
				connection.Close()
			}
			log.Info("zk conn: session for addr %v ended: %v", c.nodes, event)
			return
		}
		log.Info("zk conn: session for addr %v event: %v", c.nodes, event)
	}
}

func (c *Connection) dial(ctx context.Context) (*zk.Conn, <-chan zk.Event, error) {
	ctx, cancel := context.WithTimeout(ctx, c.TimeoutConnect)
	defer cancel()

	connection, events, err := c.connect(c.nodes.Servers())
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

func (c *Connection) connect(servers []string) (*zk.Conn, <-chan zk.Event, error) {
	optionsDialer := zk.WithDialer(net.DialTimeout)
	if c.CertFile != "" && c.KeyFile != "" {
		if len(servers) > 1 {
			log.Fatal("This TLS zk code requires that the all the zk servers validate to a single server name.")
		}

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
			Certificates: []tls.Certificate{cert},
			RootCAs:      clientCertPool,
			ServerName:   serverName,
		}

		optionsDialer = zk.WithDialer(func(network, address string, timeout time.Duration) (net.Conn, error) {
			d := net.Dialer{
				Timeout: timeout,
			}

			return tls.DialWithDialer(&d, network, address, tlsConfig)
		})
	}

	// May need to implement manually &zk.SimpleDNSHostProvider{} from github.com/z-division/go-zookeeper/zk
	hostProvider := &zk.DNSHostProvider{}
	optionsDNSHostProvider := zk.WithHostProvider(hostProvider)
	return zk.Connect(servers, c.TimeoutKeepAlive, optionsDialer, optionsDNSHostProvider)
}
