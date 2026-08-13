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

package operator

import (
	"context"
	"fmt"
	"net/http"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	otelApi "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	otelResource "go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/version"
)

func newOTELResource() (*otelResource.Resource, error) {
	pod, _ := chop.GetRuntimeParam(deployment.OPERATOR_POD_NAME)
	namespace, _ := chop.GetRuntimeParam(deployment.OPERATOR_POD_NAMESPACE)
	return otelResource.Merge(
		otelResource.Default(),
		otelResource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceVersion(version.Version),
			semconv.ServiceName("clickhouse-operator"),
			semconv.ServiceNamespace(namespace),
			semconv.ServiceInstanceID(pod),
		),
	)
}

func StartMetricsExporter(endpoint, path string) {
	// Create resource.
	resource, err := newOTELResource()
	if err != nil {
		log.Fatal(err.Error())
	}

	// Prometheus exporter embeds a default OpenTelemetry Reader and implements prometheus.Collector,
	// allowing it to be used as both a Reader and Collector.
	//namespace, _ := chop.Get().ConfigManager.GetRuntimeParam(api.OPERATOR_POD_NAMESPACE)
	exporter, err := prometheus.New(
		prometheus.WithoutUnits(),
		//prometheus.WithoutTargetInfo(),
		prometheus.WithoutCounterSuffixes(),
		prometheus.WithoutScopeInfo(),
		//prometheus.WithNamespace(namespace),
	)
	if err != nil {
		log.Fatal(err.Error())
	}

	// Factory of Meters
	meterProvider := metric.NewMeterProvider(
		metric.WithResource(resource),
		metric.WithReader(exporter),
	)

	// Meter can be requested either from OTEL or from meter provider directly

	// Register as global meter provider so that it can be used via otel.Meter
	// and accessed using otel.GetMeterProvider.
	// Most instrumentation libraries use the global meter provider as default.
	// If the global meter provider is not set then a no-op implementation
	// is used, which fails to generate data.
	//otel.SetMeterProvider(meterProvider)
	//meter := otel.Meter("chi_meter_2")

	meter = meterProvider.Meter("clickhouse-operator-meter", otelApi.WithInstrumentationVersion(version.Version))

	recordOperatorInfo()

	// Start the prometheus HTTP server and pass the exporter Collector to it
	serveMetrics(endpoint, path)
}

var meter otelApi.Meter

func Meter() otelApi.Meter {
	return meter
}

// recordOperatorInfo publishes an info-style metric (constant 1) carrying this operator
// instance's shard identity. Joinable against clickhouse_operator_cr_skipped_by_label_selector
// to identify CRs whose shard label matches no running operator.
func recordOperatorInfo() {
	info, err := meter.Int64Gauge(
		"clickhouse_operator_info",
		otelApi.WithDescription("operator instance configuration info; value is always 1"),
	)
	if err != nil {
		log.V(1).Warning("failed to create clickhouse_operator_info metric: %v", err)
		return
	}
	info.Record(context.Background(), 1, otelApi.WithAttributes(
		attribute.String("watch_label_selector", chop.Config().Watch.LabelSelector),
		attribute.Bool("require_label_selector", chop.Config().Watch.RequireLabelSelector),
		attribute.String("version", version.Version),
	))
}

func serveMetrics(addr, path string) {
	fmt.Printf("start serving metrics at: %s%s\n", addr, path)
	// Use ContinueOnError so that a single untranslatable OTel metric (e.g. a metric
	// with a name that cannot be mapped to a valid Prometheus name) does not cause an
	// HTTP 500 for the entire scrape. As of otel/exporters/prometheus v0.61.0+ invalid
	// metrics produce prometheus.NewInvalidMetric, which triggers HTTP 500 under the
	// default HTTPErrorOnError. ContinueOnError logs the problem and keeps the scrape alive.
	handler := promhttp.HandlerFor(prom.DefaultGatherer, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
	})
	// Serve a private mux, NOT http.DefaultServeMux. controller-runtime (pulled in by
	// the CHK controller) transitively imports net/http/pprof, whose init() registers
	// /debug/pprof/* on DefaultServeMux. Binding DefaultServeMux here — ListenAndServe(
	// addr, nil) — would expose those pprof endpoints (CPU profile, heap, goroutine) on
	// this public metrics port. A dedicated mux serves only the metrics handler.
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	err := http.ListenAndServe(addr, mux)
	if err != nil {
		fmt.Printf("error serving http: %v", err)
	}
	fmt.Printf("end serving metrics at: %s%s\n", addr, path)
}
