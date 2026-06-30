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
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	log "github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/metrics"
)

// Request type constants
const (
	RequestTypeCR   = "cr"
	RequestTypeHost = "host"
)

// RESTRequest wraps different request types for POST/DELETE operations
type RESTRequest struct {
	Type string             `json:"type"` // "cr" or "host"
	CR   *metrics.WatchedCR `json:"cr,omitempty"`
	Host *HostRequest       `json:"host,omitempty"`
}

// HostRequest contains host details with parent context
type HostRequest struct {
	CRNamespace string               `json:"crNamespace"`
	CRName      string               `json:"crName"`
	ClusterName string               `json:"clusterName"`
	Host        *metrics.WatchedHost `json:"host"`
}

// IsValid checks if HostRequest has all required fields
func (r *HostRequest) IsValid() bool {
	return r.CRNamespace != "" && r.CRName != "" && r.ClusterName != "" && r.Host != nil && r.Host.Hostname != ""
}

// RESTServer provides HTTP API for managing watched CRs and Hosts
type RESTServer struct {
	registry *CRRegistry
}

// NewRESTServer creates a new RESTServer instance
func NewRESTServer(registry *CRRegistry) *RESTServer {
	return &RESTServer{
		registry: registry,
	}
}

// ServeHTTP implements http.Handler interface
func (s *RESTServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/chi" {
		http.Error(w, "404 not found.", http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r)
	case http.MethodPost:
		s.handlePost(w, r)
	case http.MethodDelete:
		s.handleDelete(w, r)
	default:
		_, _ = fmt.Fprintf(w, "Sorry, only GET, POST and DELETE methods are supported.")
	}
}

// handleGet serves HTTP GET request to get list of watched CRs
func (s *RESTServer) handleGet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.registry.List())
}

// handlePost serves HTTP POST request to add CR or Host
func (s *RESTServer) handlePost(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	req, err := s.decodeRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotAcceptable)
		return
	}

	switch req.Type {
	case RequestTypeCR:
		s.registry.AddCR(req.CR)
	case RequestTypeHost:
		if err := s.registry.AddHost(req.Host); err != nil {
			http.Error(w, err.Error(), http.StatusNotAcceptable)
			return
		}
	default:
		http.Error(w, fmt.Sprintf("unknown request type: %s", req.Type), http.StatusNotAcceptable)
	}
}

// handleDelete serves HTTP DELETE request to delete CR or Host
func (s *RESTServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	req, err := s.decodeRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotAcceptable)
		return
	}

	switch req.Type {
	case RequestTypeCR:
		s.registry.RemoveCR(req.CR)
	case RequestTypeHost:
		s.registry.RemoveHost(req.Host)
	default:
		http.Error(w, fmt.Sprintf("unknown request type: %s", req.Type), http.StatusNotAcceptable)
	}
}

// decodeRequest decodes RESTRequest from the HTTP request body
func (s *RESTServer) decodeRequest(r *http.Request) (*RESTRequest, error) {
	req := &RESTRequest{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		return nil, fmt.Errorf("unable to parse request: %w", err)
	}

	switch req.Type {
	case RequestTypeCR:
		if req.CR == nil || !req.CR.IsValid() {
			return nil, fmt.Errorf("invalid CR in request")
		}
	case RequestTypeHost:
		if req.Host == nil || !req.Host.IsValid() {
			return nil, fmt.Errorf("invalid Host in request")
		}
	default:
		return nil, fmt.Errorf("unknown request type: %s", req.Type)
	}

	return req, nil
}

// StartMetricsREST starts Prometheus metrics exporter and REST API server
func StartMetricsREST(
	metricsAddress string,
	metricsPath string,
	collectorTimeout time.Duration,
	chiListAddress string,
	chiListPath string,
) *Exporter {
	log.V(1).Infof("Starting metrics exporter at '%s%s'\n", metricsAddress, metricsPath)

	// Create shared registry
	registry := NewCRRegistry()

	// Create and register Prometheus exporter
	exporter := NewExporter(registry, collectorTimeout)
	prometheus.MustRegister(exporter)

	// Create REST server
	restServer := NewRESTServer(registry)

	// Resolve operator↔exporter IPC mode. In Plain mode (default) preserves
	// today's behavior: server binds the supplied addresses on all interfaces,
	// no auth. In Secure mode: server binds the loopback BindHost, /chi handler
	// is wrapped in a token-check middleware reading the shared-volume token.
	ipc := resolveIPC()
	chiHandler := http.Handler(restServer)
	if ipc.Mode == api.IPCModeSecure {
		// Wait for the operator to provision the token in the shared volume.
		// Container startup order is not guaranteed; tolerate a brief race.
		token, err := ipc.waitForToken()
		if err != nil {
			log.Fatalf("IPC Secure mode requested but token cannot be loaded: %v", err)
		}
		chiHandler = ipcAuthMiddleware(restServer, token)
		log.Infof("IPC: Secure mode — binding /chi to %s, requiring %s header", ipc.BindHost, api.IPCHeaderToken)
	}

	// Setup HTTP handlers on private muxes so unrelated DefaultServeMux
	// registrations are never exposed on public metrics listeners.
	metricsMux := http.NewServeMux()
	metricsMux.Handle(metricsPath, promhttp.Handler())
	if metricsAddress == chiListAddress {
		metricsMux.Handle(chiListPath, chiHandler)
	}

	chiMux := http.NewServeMux()
	chiMux.Handle(chiListPath, chiHandler)

	// Start HTTP servers. Prometheus /metrics ALWAYS binds the original address
	// (typically all-interfaces) so ServiceMonitor scrapes from outside the Pod
	// keep working. In Secure mode the /chi handler is protected by the loopback
	// + token middleware (see chiHandler above); since /metrics and /chi commonly
	// share a port, we cannot bind /chi to a different interface on the same
	// listener — instead the middleware enforces remoteAddr=loopback at request
	// time. If a custom config splits the two addresses, the /chi listener is
	// bound to BindHost in Secure mode.
	go http.ListenAndServe(metricsAddress, metricsMux)
	if metricsAddress != chiListAddress {
		go http.ListenAndServe(ipc.secureBindAddress(chiListAddress), chiMux)
	}

	return exporter
}
