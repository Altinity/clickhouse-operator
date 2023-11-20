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

package app

import (
	"context"
	"flag"
	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/metrics"
)

// Prometheus exporter defaults
const (
	defaultMetricsEndpoint = ":9999"
	defaultMetricsPath     = "/metrics"
)

// CLI parameter variables
var (
	// metricsEP defines metrics end-point IP address
	metricsEP string
	// metricsPath defines metrics path
	metricsPath string
)

func init() {
	flag.StringVar(&metricsEP, "metrics-endpoint", defaultMetricsEndpoint, "The Prometheus exporter endpoint.")
	flag.StringVar(&metricsPath, "metrics-path", defaultMetricsPath, "The Prometheus exporter path.")
}

// initClickHouseReconcilerMetricsExporter is an entry point of the application
func initClickHouseReconcilerMetricsExporter(ctx context.Context) {
}

// runClickHouseReconcilerMetricsExporter is an entry point of the application
func runClickHouseReconcilerMetricsExporter(ctx context.Context) {
	log.S().P()
	defer log.E().P()

	log.V(1).F().Info("Starting operator metrics exporter")
	metrics.StartMetricsExporter(metricsEP, metricsPath)
}
