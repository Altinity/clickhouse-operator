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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/altinity/clickhouse-operator/pkg/metrics/operator"
)

func labels(src labelsSource) metric.MeasurementOption {
	return metric.WithAttributes(prepareLabels(src)...)
}

type labelsSource interface {
	GetName() string
	GetNamespace() string
	GetLabels() map[string]string
	GetAnnotations() map[string]string
}

func prepareLabels(cr labelsSource) []attribute.KeyValue {
	// Prepare base set of labels
	labels, values := getBaseSetLabelsAndValues(cr)
	// Append particular metric labels
	// not yet...
	// Filter out metrics to be skipped
	// TODO
	return convert(labels, values)
}

func getBaseSetLabelsAndValues(cr labelsSource) (labelNames []string, labelValues []string) {
	return operator.GetLabelsFromSource(cr)
}

func convert(labelNames []string, labelValues []string) (attributes []attribute.KeyValue) {
	for i := range labelNames {
		name := labelNames[i]
		value := labelValues[i]
		attributes = append(attributes, attribute.String(name, value))
	}
	return attributes
}
