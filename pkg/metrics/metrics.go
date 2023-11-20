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
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	otelApi "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	otelResource "go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"net/http"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chiV1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/version"
)

func newOTELResource() (*otelResource.Resource, error) {
	pod, _ := chop.Get().ConfigManager.GetRuntimeParam(chiV1.OPERATOR_POD_NAME)
	namespace, _ := chop.Get().ConfigManager.GetRuntimeParam(chiV1.OPERATOR_POD_NAMESPACE)
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
	//namespace, _ := chop.Get().ConfigManager.GetRuntimeParam(chiV1.OPERATOR_POD_NAMESPACE)
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

	// Start the prometheus HTTP server and pass the exporter Collector to it
	serveMetrics(endpoint, path)
}

var meter otelApi.Meter

func Meter() otelApi.Meter {
	return meter
}

func serveMetrics(addr, path string) {
	fmt.Printf("serving metrics at %s%s", addr, path)
	http.Handle(path, promhttp.Handler())
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		fmt.Printf("error serving http: %v", err)
		return
	}
}
