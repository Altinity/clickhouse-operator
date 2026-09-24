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
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	otelApi "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"

	"github.com/altinity/clickhouse-operator/pkg/version"
)

// TestNewOTELResourceMergesCleanly is the guard that would have caught the operator refusing to
// start after an SDK bump: the merge used to fail on a schema URL conflict and the failure was
// fatal, so the operator exited 255 in a crash loop while `go build` and the unit suite stayed
// green. See newOTELResource for why declaring no schema URL is what prevents that.
//
// The attribute assertions are the other half: every one of these keys is a label on the exported
// target_info series, so dropping any of them from the resource silently changes what an installed
// operator publishes about itself.
func TestNewOTELResourceMergesCleanly(t *testing.T) {
	res, err := newOTELResource()

	require.NoError(t, err,
		"resource merge failed - the operator would exit at startup; if this is a schema URL "+
			"conflict, our side must not declare one")
	require.NotNil(t, res)

	got := map[attribute.Key]attribute.Value{}
	for _, kv := range res.Attributes() {
		got[kv.Key] = kv.Value
	}

	// The telemetry.sdk.* keys come from resource.Default(); losing them means the merge dropped
	// the SDK's own side, which also silently drops OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME.
	for _, key := range []attribute.Key{
		semconv.ServiceNameKey,
		semconv.ServiceVersionKey,
		semconv.ServiceNamespaceKey,
		semconv.ServiceInstanceIDKey,
		semconv.TelemetrySDKLanguageKey,
		semconv.TelemetrySDKNameKey,
		semconv.TelemetrySDKVersionKey,
	} {
		require.Contains(t, got, key, "%s is missing - it is a target_info label", key)
	}

	require.Equal(t, "clickhouse-operator", got[semconv.ServiceNameKey].AsString())
	require.Equal(t, version.Version, got[semconv.ServiceVersionKey].AsString())
}

// TestMeterProviderKeepsEverySeries pins the cardinality limit the operator sets explicitly; see
// newMeterProvider for why the SDK default is wrong for us.
//
// It drives newMeterProvider rather than assembling an equivalent provider, so that deleting an
// option there fails here. Assembling one would pin the SDK's handling of a limit nobody sets -
// green with the operator's own limit gone.
func TestMeterProviderKeepsEverySeries(t *testing.T) {
	const series = 2500

	reader := metric.NewManualReader()
	provider := newMeterProvider(reader)

	counter, err := provider.Meter("test").Int64Counter("probe")
	require.NoError(t, err)

	for i := 0; i < series; i++ {
		counter.Add(context.Background(), 1,
			otelApi.WithAttributes(attribute.String("chi", fmt.Sprintf("chi-%d", i))))
	}

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	points := 0
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				_, overflow := dp.Attributes.Value("otel.metric.overflow")
				require.False(t, overflow,
					"series were collapsed into otel.metric.overflow - the SDK cardinality "+
						"default is in force and installations are being silently merged")
			}
			points += len(sum.DataPoints)
		}
	}
	require.Equal(t, series, points, "every attribute set must survive collection")

	// The resource travels with the collection, so the same pass proves it reached the provider:
	// without it every scrape reports target_info{service_name="unknown_service:<binary>"}.
	serviceName, ok := rm.Resource.Set().Value(semconv.ServiceNameKey)
	require.True(t, ok,
		"the provider carries no service.name - target_info cannot identify the operator")
	require.Equal(t, "clickhouse-operator", serviceName.AsString())
}

// TestUnusedTelemetryPipelinesAreNotLinked is why this package can answer an otel/sdk advisory with
// "not reachable" instead of an argument. GHSA-8wmf-6v46-5gfg logs exporter configuration, leaking
// endpoint URLs; it is scoped to the trace pipeline, which the operator does not build. The trace
// and log SDKs and every OTLP exporter are absent from both binaries' package closures - the only
// telemetry the operator emits is metrics, through the scraped Prometheus exporter. Keeping that a
// test means the next advertisement against a pipeline we do not run is answered by a command
// rather than re-derived.
func TestUnusedTelemetryPipelinesAreNotLinked(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not available")
	}

	out, err := exec.Command("go", "list", "-deps",
		"../../../cmd/operator", "../../../cmd/metrics_exporter").Output()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		// Stderr carries the reason; stdout is the whole package closure and would bury it.
		t.Fatalf("go list failed: %s", exitErr.Stderr)
	}
	require.NoError(t, err)

	forbidden := []string{
		"go.opentelemetry.io/otel/sdk/trace",
		"go.opentelemetry.io/otel/sdk/log",
		"go.opentelemetry.io/otel/exporters/otlp",
	}
	for _, line := range strings.Split(string(out), "\n") {
		for _, pkg := range forbidden {
			// Prefix, not equality: a subpackage links the parent's code just as effectively.
			require.False(t, (line == pkg) || strings.HasPrefix(line, pkg+"/"),
				"%s is now linked - the advisories this package is exempt from may apply; "+
					"re-assess them and update this list", line)
		}
	}
}
