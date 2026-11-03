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
	"fmt"
	"net/http"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

// metricCardinalityUnlimited disables the MeterProvider's cardinality limit; the SDK reads zero or
// less as "no limit".
const metricCardinalityUnlimited = 0

func newOTELResource() (*otelResource.Resource, error) {
	pod, _ := chop.GetRuntimeParam(deployment.OPERATOR_POD_NAME)
	namespace, _ := chop.GetRuntimeParam(deployment.OPERATOR_POD_NAMESPACE)
	// Schemaless deliberately. Merge reports an error when both sides declare a schema URL and the
	// versions differ, and Default()'s schema URL is whatever semconv the SDK vendors - so it moves
	// on the otel/sdk upgrades that bump semconv, independently of the semconv import this file
	// carries. Declaring none on our side means the two can never disagree, rather than requiring
	// them to be bumped in lockstep. The schema URL is never exported as a label, so dropping ours
	// changes no series.
	return otelResource.Merge(
		otelResource.Default(),
		otelResource.NewSchemaless(
			semconv.ServiceVersion(version.Version),
			semconv.ServiceName("clickhouse-operator"),
			semconv.ServiceNamespace(namespace),
			semconv.ServiceInstanceID(pod),
		),
	)
}

// newMeterProvider builds the operator's MeterProvider. It is separate from StartMetricsExporter
// because that function binds a port and never returns, leaving the provider untestable where it
// used to be assembled; a test that assembles an equivalent provider itself would assert that the
// SDK honours these options rather than that the operator passes them.
//
// What this does not cover is the caller abandoning it - build a provider inline below and the
// tests still pass, because they drive this function. Keep provider construction here; it is the
// only thing standing between a future edit and silently inheriting the SDK defaults again.
func newMeterProvider(reader metric.Reader) *metric.MeterProvider {
	resource, err := newOTELResource()
	if err != nil {
		// Unreachable while newOTELResource stays schemaless - a schema URL conflict is Merge's
		// only error - and kept rather than dropped so that reintroducing one cannot take the
		// process down. Merge returns the fully merged resource alongside the error, so even
		// then every attribute survives and only the schema URL is in doubt; exiting here
		// instead is what turned an SDK upgrade into a CrashLoopBackOff.
		log.Warning("OTEL resource merge reported a conflict, using the merged resource anyway: %s",
			err.Error())
	}

	return metric.NewMeterProvider(
		metric.WithResource(resource),
		metric.WithReader(reader),
		// Pinned, not inherited: the SDK default was unlimited through sdk/metric v1.43.0 and
		// became 2000 data points per instrument per collect cycle in v1.44.0, folding everything
		// past it into a single otel.metric.overflow series - silently. Operator series are keyed
		// by installation, so the count has to follow the installations watched, not a fixed cap.
		// This also overrides OTEL_GO_X_CARDINALITY_LIMIT, which the SDK reads first and explicit
		// options then win over: re-capping the operator is a rebuild, not an env var.
		metric.WithCardinalityLimit(metricCardinalityUnlimited),
	)
}

func StartMetricsExporter(endpoint, path string) {
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
	meterProvider := newMeterProvider(exporter)

	// Meter can be requested either from OTEL or from meter provider directly

	// Register as global meter provider so that it can be used via otel.Meter
	// and accessed using otel.GetMeterProvider.
	// Most instrumentation libraries use the global meter provider as default.
	// If the global meter provider is not set then a no-op implementation
	// is used, which fails to generate data.
	//otel.SetMeterProvider(meterProvider)
	//meter := otel.Meter("chi_meter_2")

	meter = meterProvider.Meter("clickhouse-operator-meter", otelApi.WithInstrumentationVersion(version.Version))

	// Start the prometheus HTTP server and pass the exporter Collector to it
	serveMetrics(endpoint, path)
}

var meter otelApi.Meter

func Meter() otelApi.Meter {
	return meter
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
