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

package util

import (
	"regexp"
)

// Labels may contain ASCII letters, numbers, as well as underscores.
// They must match the regex [a-zA-Z_][a-zA-Z0-9_]*
// For more details check:
// https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels

const prometheusLabelFormat string = "[a-zA-Z_][a-zA-Z0-9_]*"
const prometheusLabelNotAllowedCharsFormat string = "[^a-zA-Z0-9_]"

var prometheusLabelRegexp = regexp.MustCompile("^" + prometheusLabelFormat + "$")
var prometheusLabelNotAllowedCharsRegexp = regexp.MustCompile(prometheusLabelNotAllowedCharsFormat)

func BuildPrometheusLabel(label string) string {
	// Replace not allowed chars
	return prometheusLabelNotAllowedCharsRegexp.ReplaceAllString(label, "_")
}

func BuildPrometheusLabels(labels ...string) []string {
	var res []string
	for _, label := range labels {
		res = append(res, BuildPrometheusLabel(label))
	}
	return res
}

// IsValidPrometheusLabel tests for a string that conforms to the definition of a label in Prometheus
func IsValidPrometheusLabel(value string) bool {
	return prometheusLabelRegexp.MatchString(value)
}

func IsValidPrometheusLabelValue(value string) bool {
	// Label values may contain any Unicode characters
	// For more details check:
	// https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
	return true
}
