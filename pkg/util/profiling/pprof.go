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

// Package profiling serves the Go net/http/pprof handlers on an opt-in, dedicated listener.
// It is shared by the operator and metrics-exporter binaries so that both expose pprof the same way.
package profiling

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"time"

	log "github.com/golang/glog"
)

const (
	// FlagName is the CLI flag both binaries use to enable the pprof listener
	FlagName = "pprof-address"
	// FlagUsage is the CLI flag help text
	FlagUsage = "Address (host:port) to serve Go pprof on, e.g. 127.0.0.1:6060. Empty (default) disables pprof. " +
		"Served on a separate listener, never on the metrics port. Prefer a loopback address."

	// readHeaderTimeout bounds slow-header clients. There is deliberately no WriteTimeout:
	// /debug/pprof/profile and /debug/pprof/trace stream for the requested number of seconds.
	readHeaderTimeout = 10 * time.Second
	shutdownTimeout   = 5 * time.Second
)

// ResolveAddress returns the pprof listen address: the flag value when set, otherwise the value
// of the env variable envName. Empty result means pprof is disabled.
func ResolveAddress(flagValue, envName string) string {
	if flagValue != "" {
		return flagValue
	}
	return os.Getenv(envName)
}

// NewMux returns a dedicated mux with the pprof handlers registered explicitly.
// net/http/pprof's init() also registers itself on http.DefaultServeMux, which is why every
// listener in this repo serves a private mux and never DefaultServeMux.
func NewMux() *http.ServeMux {
	mux := http.NewServeMux()
	// Index also serves the named profiles: heap, goroutine, allocs, block, mutex, threadcreate
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// IsLoopbackAddress reports whether a host:port listen address binds loopback only.
// An empty host (":6060") binds all interfaces and is not loopback.
func IsLoopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return (ip != nil) && ip.IsLoopback()
}

// Start serves pprof on address in the background until ctx is done. No-op when address is empty.
// component is used in log messages only.
func Start(ctx context.Context, address, component string) {
	if address == "" {
		return
	}
	if !IsLoopbackAddress(address) {
		log.Warningf("%s: pprof is bound to non-loopback address %s. Profiles expose process internals; "+
			"prefer 127.0.0.1:<port> and kubectl port-forward", component, address)
	}

	server := &http.Server{
		Addr:              address,
		Handler:           NewMux(),
		ReadHeaderTimeout: readHeaderTimeout,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	go func() {
		// Warning, not Info: an enabled pprof endpoint must be visible in kubectl logs
		log.Warningf("%s: pprof is ENABLED at http://%s/debug/pprof/ - disable it when done profiling", component, address)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Errorf("%s: pprof server at %s failed: %v", component, address, err)
		}
	}()
}
