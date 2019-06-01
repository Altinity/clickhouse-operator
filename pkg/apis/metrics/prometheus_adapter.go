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
	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"strings"
)

// writeMetricsDataToPrometheus pushes set of prometheus.Metric objects created from the ClickHouse system data
// Expected data structure: metric, value, description, type (gauge|counter)
func writeMetricsDataToPrometheus(out chan<- prometheus.Metric, data [][]string, chiName, hostname string) {
	for _, metric := range data {
		if len(metric) < 2 {
			continue
		}
		var metricType prometheus.ValueType
		if metric[3] == "counter" {
			metricType = prometheus.CounterValue
		} else {
			metricType = prometheus.GaugeValue
		}
		writeSingleMetricToPrometheus(out,
			convertMetricName(metric[0]),
			metric[2],
			metric[1],
			metricType,
			[]string{"chi", "hostname"},
			chiName,
			hostname,
		)
	}
}

// writeTableSizesDataToPrometheus pushes set of prometheus.Metric objects created from the ClickHouse system data
// Expected data structure: database, table, partitions, parts, bytes, uncompressed_bytes, rows
func writeTableSizesDataToPrometheus(out chan<- prometheus.Metric, data [][]string, chiname, hostname string) {
	for _, metric := range data {
		if len(metric) < 2 {
			continue
		}
		writeSingleMetricToPrometheus(out, "table_partitions", "Number of partitions of the table", metric[2], prometheus.GaugeValue,
			[]string{"chi", "hostname", "database", "table"},
			chiname, hostname, metric[0], metric[1])
		writeSingleMetricToPrometheus(out, "table_parts", "Number of parts of the table", metric[3], prometheus.GaugeValue,
			[]string{"chi", "hostname", "database", "table"},
			chiname, hostname, metric[0], metric[1])
		writeSingleMetricToPrometheus(out, "table_parts_bytes", "Table size in bytes", metric[4], prometheus.GaugeValue,
			[]string{"chi", "hostname", "database", "table"},
			chiname, hostname, metric[0], metric[1])
		writeSingleMetricToPrometheus(out, "table_parts_bytes_uncompressed", "Table size in bytes uncompressed", metric[5], prometheus.GaugeValue,
			[]string{"chi", "hostname", "database", "table"},
			chiname, hostname, metric[0], metric[1])
		writeSingleMetricToPrometheus(out, "table_parts_rows", "Number of rows in the table", metric[6], prometheus.GaugeValue,
			[]string{"chi", "hostname", "database", "table"},
			chiname, hostname, metric[0], metric[1])
	}
}

func writeSingleMetricToPrometheus(out chan<- prometheus.Metric, name string, desc string, value string, metricType prometheus.ValueType, labels []string, labelValues ...string) {
	floatValue, _ := strconv.ParseFloat(value, 64)
	m, err := prometheus.NewConstMetric(
		newDescription(name, desc, labels),
		metricType,
		floatValue,
		labelValues...,
	)
	if err != nil {
		glog.Infof("Error creating metric %s: %s", name, err)
		return
	}
	select {
	case out <- m:

	default:
		glog.Infof("Error sending metric to the channel %s", name)
	}
}

// newDescription creates a new prometheus.Desc object
func newDescription(name, help string, labels []string) *prometheus.Desc {
	return prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subsystem, name),
		help,
		labels,
		nil,
	)
}

// convertMetricName converts the given string to snake case following the Golang format:
// acronyms are converted to lower-case and preceded by an underscore.
func convertMetricName(in string) string {
	/*runes := []rune(in)
	length := len(runes)

	var out []rune
	for i := 0; i < length; i++ {
		if i > 0 && unicode.IsUpper(runes[i]) && ((i+1 < length && unicode.IsLower(runes[i+1])) || unicode.IsLower(runes[i-1])) {
			out = append(out, '_')
		}
		out = append(out, unicode.ToLower(runes[i]))
	}*/

	return strings.Replace(string(in), ".", "_", -1)
}
