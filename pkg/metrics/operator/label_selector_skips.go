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
	"sync"

	"go.opentelemetry.io/otel/attribute"
	otelApi "go.opentelemetry.io/otel/metric"
)

var (
	crSkippedByLabelSelector     otelApi.Int64Counter
	crSkippedByLabelSelectorOnce sync.Once
)

// CRSkippedByLabelSelector counts CR events skipped because the CR labels do not match this
// operator's watch.labelSelector. Informer resync re-fires events periodically, so a CR whose
// label matches NO operator (orphaned shard value) shows up as a steady skip rate on every
// operator while appearing in no operator's watched-CR metrics — alert on that combination.
func CRSkippedByLabelSelector(kind string, namespace string, name string) {
	if Meter() == nil {
		// Metrics machinery not started (unit tests, exporter binary)
		return
	}
	crSkippedByLabelSelectorOnce.Do(func() {
		crSkippedByLabelSelector, _ = Meter().Int64Counter(
			"clickhouse_operator_cr_skipped_by_label_selector",
			otelApi.WithDescription("number of CR events skipped because CR labels do not match watch.labelSelector"),
			otelApi.WithUnit("items"),
		)
	})
	crSkippedByLabelSelector.Add(context.Background(), 1, otelApi.WithAttributes(
		attribute.String("kind", kind),
		attribute.String("namespace", namespace),
		attribute.String("name", name),
	))
}
