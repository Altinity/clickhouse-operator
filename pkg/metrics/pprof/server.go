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

package pprof

import (
	"fmt"
	"net"
	"net/http"
	"runtime/pprof"
	"strconv"
)

var servedProfiles = []string{"heap", "allocs", "goroutine"}

// StartServer starts a private, loopback-only pprof server without registering
// any handlers on http.DefaultServeMux.
func StartServer(addr string) error {
	if err := validateLoopbackAddress(addr); err != nil {
		return err
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	server := &http.Server{
		Addr:    listener.Addr().String(),
		Handler: NewMux(),
	}

	go func() {
		_ = server.Serve(listener)
	}()

	return nil
}

// NewMux builds the pprof mux. It intentionally serves only OOM-relevant
// profiles and omits CPU profile and trace endpoints.
func NewMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", index)
	for _, name := range servedProfiles {
		profileName := name
		mux.HandleFunc("/debug/pprof/"+profileName, func(w http.ResponseWriter, r *http.Request) {
			serveProfile(w, r, profileName)
		})
	}
	return mux
}

func index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/debug/pprof/" {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = fmt.Fprintln(w, "<html><body><h1>Available profiles</h1><ul>")
	for _, name := range servedProfiles {
		_, _ = fmt.Fprintf(w, `<li><a href="/debug/pprof/%[1]s">%[1]s</a></li>`+"\n", name)
	}
	_, _ = fmt.Fprintln(w, "</ul></body></html>")
}

func serveProfile(w http.ResponseWriter, r *http.Request, name string) {
	profile := pprof.Lookup(name)
	if profile == nil {
		http.NotFound(w, r)
		return
	}

	debug, err := strconv.Atoi(r.URL.Query().Get("debug"))
	if err != nil {
		debug = 0
	}

	if debug == 0 {
		w.Header().Set("Content-Type", "application/octet-stream")
	} else {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	}
	_ = profile.WriteTo(w, debug)
}

func validateLoopbackAddress(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid pprof endpoint %q: %w", addr, err)
	}
	if host == "" {
		return fmt.Errorf("invalid pprof endpoint %q: host must be loopback, for example 127.0.0.1:6060", addr)
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("invalid pprof endpoint %q: host must be loopback", addr)
	}
	return nil
}
