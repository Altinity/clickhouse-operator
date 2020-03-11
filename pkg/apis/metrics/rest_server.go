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

package metrics

import (
	"fmt"
	"net/http"

	log "github.com/golang/glog"
	// log "k8s.io/klog"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StartMetricsREST start Prometheus metrics exporter in background
func StartMetricsREST(
	chAccess *CHAccessInfo,

	metricsAddress string,
	metricsPath string,

	chiListAddress string,
	chiListPath string,
) *Exporter {
	log.V(1).Infof("Starting metrics exporter at '%s%s'\n", metricsAddress, metricsPath)

	exporter = NewExporter(chAccess)
	prometheus.MustRegister(exporter)

	http.Handle(metricsPath, promhttp.Handler())
	http.Handle(chiListPath, exporter)

	go http.ListenAndServe(metricsAddress, nil)
	if metricsAddress != chiListAddress {
		go http.ListenAndServe(chiListAddress, nil)
	}

	return exporter
}

// ServeHTTP is an interface method to serve HTTP requests
func (e *Exporter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/chi" {
		http.Error(w, "404 not found.", http.StatusNotFound)
		return
	}

	switch r.Method {
	case "GET":
		e.getWatchedCHI(w, r)
	case "POST":
		e.updateWatchedCHI(w, r)
	case "DELETE":
		e.deleteWatchedCHI(w, r)
	default:
		_, _ = fmt.Fprintf(w, "Sorry, only GET, POST and DELETE methods are supported.")
	}
}
