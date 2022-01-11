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
	"strconv"
	"strings"
	"time"

	log "github.com/golang/glog"
	// log "k8s.io/klog"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "chi"
	subsystem = "clickhouse"
)

const (
	// writeMetricWaitTimeout specifies how long to wait for metric being accepted by prometheus writer
	writeMetricWaitTimeout = 10 * time.Second
)

// PrometheusWriter specifies write to prometheus
type PrometheusWriter struct {
	out      chan<- prometheus.Metric
	chi      *WatchedCHI
	hostname string
}

// NewPrometheusWriter creates new prometheus writer
func NewPrometheusWriter(
	out chan<- prometheus.Metric,
	chi *WatchedCHI,
	hostname string,
) *PrometheusWriter {
	return &PrometheusWriter{
		out:      out,
		chi:      chi,
		hostname: hostname,
	}
}

// WriteMetrics pushes set of prometheus.Metric objects created from the ClickHouse system data
// Expected data structure: metric, value, description, type (gauge|counter)
// TODO add namespace handling. It is just skipped for now
func (w *PrometheusWriter) WriteMetrics(data [][]string) {
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
		writeSingleMetricToPrometheus(w.out,
			convertMetricName(metric[0]),
			metric[2],
			metric[1],
			metricType,
			[]string{"chi", "namespace", "hostname"},
			w.chi.Name,
			w.chi.Namespace,
			w.hostname,
		)
	}
}

// WriteTableSizes pushes set of prometheus.Metric objects created from the ClickHouse system data
// Expected data structure: database, table, partitions, parts, bytes, uncompressed_bytes, rows
// TODO add namespace handling. It is just skipped for now
func (w *PrometheusWriter) WriteTableSizes(data [][]string) {
	for _, metric := range data {
		if len(metric) < 2 {
			continue
		}
		writeSingleMetricToPrometheus(w.out, "table_partitions", "Number of partitions of the table", metric[3], prometheus.GaugeValue,
			[]string{"chi", "namespace", "hostname", "database", "table", "active"},
			w.chi.Name, w.chi.Namespace, w.hostname, metric[0], metric[1], metric[2])
		writeSingleMetricToPrometheus(w.out, "table_parts", "Number of parts of the table", metric[4], prometheus.GaugeValue,
			[]string{"chi", "namespace", "hostname", "database", "table", "active"},
			w.chi.Name, w.chi.Namespace, w.hostname, metric[0], metric[1], metric[2])
		writeSingleMetricToPrometheus(w.out, "table_parts_bytes", "Table size in bytes", metric[5], prometheus.GaugeValue,
			[]string{"chi", "namespace", "hostname", "database", "table", "active"},
			w.chi.Name, w.chi.Namespace, w.hostname, metric[0], metric[1], metric[2])
		writeSingleMetricToPrometheus(w.out, "table_parts_bytes_uncompressed", "Table size in bytes uncompressed", metric[6], prometheus.GaugeValue,
			[]string{"chi", "namespace", "hostname", "database", "table", "active"},
			w.chi.Name, w.chi.Namespace, w.hostname, metric[0], metric[1], metric[2])
		writeSingleMetricToPrometheus(w.out, "table_parts_rows", "Number of rows in the table", metric[7], prometheus.GaugeValue,
			[]string{"chi", "namespace", "hostname", "database", "table", "active"},
			w.chi.Name, w.chi.Namespace, w.hostname, metric[0], metric[1], metric[2])
	}
}

// WriteSystemParts pushesh set of prometheus.Metric object related to system.parts
func (w *PrometheusWriter) WriteSystemParts(data [][]string) {
	var diskDataBytes, memoryPrimaryKeyBytesAllocated int64
	var err error
	m := make([]int64, 2)
	for _, t := range data {
		m[0] = 0
		m[1] = 0
		for i, v := range t[len(t)-len(m):] {
			m[i], err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				log.V(2).Infof("Error parsing metrics value for chi_metric_DiskDataBytes, chi_metric_MemoryPrimaryKeyBytesAllocated: %s\n", v)
			}
		}
		diskDataBytes += m[0]
		memoryPrimaryKeyBytesAllocated += m[1]
	}
	w.WriteMetrics([][]string{
		{
			"metric.DiskDataBytes", fmt.Sprintf("%d", diskDataBytes),
			"Total data size for all ClickHouse tables", "gauge",
		},
		{
			"metric.MemoryPrimaryKeyBytesAllocated", fmt.Sprintf("%d", memoryPrimaryKeyBytesAllocated),
			"Memory size allocated for primary keys", "gauge",
		},
	})
}

// WriteSystemReplicas writes system replicas
func (w *PrometheusWriter) WriteSystemReplicas(data [][]string) {
	for _, metric := range data {
		writeSingleMetricToPrometheus(w.out, "system_replicas_is_session_expired", "Number of expired Zookeeper sessions of the table", metric[2], prometheus.GaugeValue,
			[]string{"chi", "namespace", "hostname", "database", "table"},
			w.chi.Name, w.chi.Namespace, w.hostname, metric[0], metric[1])
	}
}

// WriteMutations writes mutations
func (w *PrometheusWriter) WriteMutations(data [][]string) {
	for _, metric := range data {
		writeSingleMetricToPrometheus(w.out, "table_mutations", "Number of active mutations for the table", metric[2], prometheus.GaugeValue,
			[]string{"chi", "namespace", "hostname", "database", "table"},
			w.chi.Name, w.chi.Namespace, w.hostname, metric[0], metric[1])
		writeSingleMetricToPrometheus(w.out, "table_mutations_parts_to_do", "Number of data parts that need to be mutated for the mutation to finish", metric[3], prometheus.GaugeValue,
			[]string{"chi", "namespace", "hostname", "database", "table"},
			w.chi.Name, w.chi.Namespace, w.hostname, metric[0], metric[1])

	}
}

// WriteSystemDisks writes system disks
func (w *PrometheusWriter) WriteSystemDisks(data [][]string) {
	for _, metric := range data {
		writeSingleMetricToPrometheus(w.out, "metric_DiskFreeBytes", "Free disk space available from system.disks", metric[1], prometheus.GaugeValue,
			[]string{"chi", "namespace", "hostname", "disk"},
			w.chi.Name, w.chi.Namespace, w.hostname, metric[0])
		writeSingleMetricToPrometheus(w.out, "metric_DiskTotalBytes", "Total disk space available from system.disks", metric[2], prometheus.GaugeValue,
			[]string{"chi", "namespace", "hostname", "disk"},
			w.chi.Name, w.chi.Namespace, w.hostname, metric[0])
	}
}

// WriteDetachedParts writes detached parts
func (w *PrometheusWriter) WriteDetachedParts(data [][]string) {
	for _, metric := range data {
		writeSingleMetricToPrometheus(w.out, "metric_DetachedParts", "Count of currently detached parts from system.detached_parts", metric[0], prometheus.GaugeValue,
			[]string{"chi", "namespace", "hostname", "database", "table", "disk", "reason"},
			w.chi.Name, w.chi.Namespace, w.hostname, metric[1], metric[2], metric[3], metric[4])
	}
}

// WriteErrorFetch writes error fetch
func (w *PrometheusWriter) WriteErrorFetch(fetchType string) {
	writeSingleMetricToPrometheus(w.out, "metric_fetch_errors", "status of fetching metrics from ClickHouse 1 - unsuccessful, 0 - successful", "1", prometheus.GaugeValue,
		[]string{"chi", "namespace", "hostname", "fetch_type"},
		w.chi.Name, w.chi.Namespace, w.hostname, fetchType)
}

// WriteOKFetch writes successful fetch
func (w *PrometheusWriter) WriteOKFetch(fetchType string) {
	writeSingleMetricToPrometheus(w.out, "metric_fetch_errors", "status of fetching metrics from ClickHouse 1 - unsuccessful, 0 - successful", "0", prometheus.GaugeValue,
		[]string{"chi", "namespace", "hostname", "fetch_type"},
		w.chi.Name, w.chi.Namespace, w.hostname, fetchType)
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
		log.Infof("Error creating metric %s: %s", name, err)
		return
	}
	select {
	case out <- m:
	case <-time.After(writeMetricWaitTimeout):
		log.Infof("Error sending metric to the channel %s", name)
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

	return strings.NewReplacer("-", "_", ".", "_").Replace(in)
}
