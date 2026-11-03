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
	"crypto/subtle"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	log "github.com/golang/glog"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
)

// ipcTokenWaitTimeout caps how long the exporter waits for the shared-volume
// token file to become readable at startup. The operator container generates +
// writes the token at its own startup; in normal operation it lands within
// seconds, but the two containers can start in either order so the exporter
// must tolerate a brief absence.
const ipcTokenWaitTimeout = 30 * time.Second

// ipcTokenWaitInterval is the polling cadence inside ipcTokenWaitTimeout.
const ipcTokenWaitInterval = 500 * time.Millisecond

// resolvedIPC bundles the resolved (defaults-applied) operator↔exporter IPC config.
type resolvedIPC struct {
	Mode      api.IPCMode
	BindHost  string
	TokenPath string
}

// resolveIPC reads chop.Config() and applies documented defaults. Mode defaults
// to IPCModePlain (back-compat). When Mode=Secure, BindHost defaults to 127.0.0.1
// and TokenPath defaults to /etc/clickhouse-operator-ipc/token.
func resolveIPC() resolvedIPC {
	ipc := chop.Config().Security.GetIPC()
	out := resolvedIPC{Mode: api.IPCModePlain}
	if ipc != nil {
		out.Mode = ipc.GetMode()
		out.BindHost = ipc.GetBindHost()
		out.TokenPath = ipc.GetTokenPath()
	}
	if out.Mode == api.IPCModeSecure {
		if out.BindHost == "" {
			out.BindHost = api.IPCDefaultBindHost
		}
		if out.TokenPath == "" {
			out.TokenPath = api.IPCDefaultTokenPath
		}
	}
	return out
}

// secureBindAddress rewrites a listen address of the form ":8888" or "0.0.0.0:8888"
// to "<bindHost>:8888" when Mode=Secure. Returns the input unchanged otherwise.
// Uses net.SplitHostPort/JoinHostPort so IPv6 literals in BindHost (e.g. "::1")
// are correctly bracketed in the result.
func (i resolvedIPC) secureBindAddress(addr string) string {
	if i.Mode != api.IPCModeSecure {
		return addr
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return net.JoinHostPort(i.BindHost, port)
}

// loadToken reads the token from TokenPath, trimming trailing whitespace.
// Callers must only invoke this when Mode == IPCModeSecure; loading a token in
// Plain mode is a programming error (the token file may not exist).
func (i resolvedIPC) loadToken() (string, error) {
	data, err := os.ReadFile(i.TokenPath)
	if err != nil {
		return "", fmt.Errorf("IPC token unreadable at %s: %w", i.TokenPath, err)
	}
	tok := strings.TrimSpace(string(data))
	if tok == "" {
		return "", fmt.Errorf("IPC token at %s is empty", i.TokenPath)
	}
	return tok, nil
}

// waitForToken polls loadToken until it succeeds or ipcTokenWaitTimeout elapses.
// Used by the exporter at startup to tolerate the race against operator-container
// startup (the operator writes the token to the shared emptyDir volume).
func (i resolvedIPC) waitForToken() (string, error) {
	deadline := time.Now().Add(ipcTokenWaitTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		tok, err := i.loadToken()
		if err == nil {
			return tok, nil
		}
		lastErr = err
		time.Sleep(ipcTokenWaitInterval)
	}
	return "", fmt.Errorf("IPC token never appeared at %s within %s: %w", i.TokenPath, ipcTokenWaitTimeout, lastErr)
}

// ipcAuthMiddleware wraps the next handler with a constant-time token check
// against the expected value AND a remote-address loopback check. Returns 401
// on either mismatch. Identity passthrough when expected is empty (Plain mode
// — middleware not installed).
//
// The loopback check matters because /chi and /metrics commonly share a single
// listener bound to all interfaces (so Prometheus can scrape /metrics from
// outside the Pod). Rejecting non-loopback /chi callers at handler time
// substitutes for binding the listener to 127.0.0.1.
func ipcAuthMiddleware(next http.Handler, expected string) http.Handler {
	if expected == "" {
		return next
	}
	expectedBytes := []byte(expected)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isLoopbackRemote(r.RemoteAddr) {
			// Infof not Warningf: a port-scan or misconfigured scraper hitting
			// /chi would otherwise spam SIEM alerting at Warning severity per
			// request. The reject is enforced regardless of log severity.
			log.Infof("IPC: rejected non-loopback request from %s to %s", r.RemoteAddr, r.URL.Path)
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		got := r.Header.Get(api.IPCHeaderToken)
		if subtle.ConstantTimeCompare([]byte(got), expectedBytes) != 1 {
			log.Infof("IPC: rejected request from %s missing or invalid %s header", r.RemoteAddr, api.IPCHeaderToken)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// isLoopbackRemote reports whether the http.Request.RemoteAddr (in "host:port"
// or "[host]:port" form) refers to a loopback address. Tolerates malformed
// input by returning false (= reject).
func isLoopbackRemote(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		// Some test transports leave RemoteAddr empty or unparsed; play safe.
		host = remoteAddr
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return host == "localhost"
	}
	return ip.IsLoopback()
}
