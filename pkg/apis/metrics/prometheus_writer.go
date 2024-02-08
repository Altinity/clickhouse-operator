// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
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
	"github.com/altinity/clickhouse-operator/pkg/util"
	"sort"
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

// CHIPrometheusWriter specifies writer to prometheus
type CHIPrometheusWriter struct {
	out  chan<- prometheus.Metric
	chi  *WatchedCHI
	host *WatchedHost
}

// NewCHIPrometheusWriter creates new CHI prometheus writer
func NewCHIPrometheusWriter(
	out chan<- prometheus.Metric,
	chi *WatchedCHI,
	host *WatchedHost,
) *CHIPrometheusWriter {
	return &CHIPrometheusWriter{
		out:  out,
		chi:  chi,
		host: host,
	}
}

// WriteMetrics pushes set of prometheus.Metric objects created from the ClickHouse system data
// Expected data structure: metric, value, description, type (gauge|counter)
// TODO add namespace handling. It is just skipped for now
func (w *CHIPrometheusWriter) WriteMetrics(data [][]string) {
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
		name := convertMetricName(metric[0])
		desc := metric[2]
		value := metric[1]
		w.writeSingleMetricToPrometheus(
			w.out,
			name, desc,
			metricType, value,
			nil, nil)
	}
}

// WriteTableSizes pushes set of prometheus.Metric objects created from the ClickHouse system data
// Expected data structure: database, table, partitions, parts, bytes, uncompressed_bytes, rows
// TODO add namespace handling. It is just skipped for now
func (w *CHIPrometheusWriter) WriteTableSizes(data [][]string) {
	for _, metric := range data {
		if len(metric) < 2 {
			continue
		}
		labelNames := []string{"database", "table", "active"}
		labelValues := []string{metric[0], metric[1], metric[2]}
		w.writeSingleMetricToPrometheus(w.out,
			"table_partitions", "Number of partitions of the table",
			prometheus.GaugeValue, metric[3],
			labelNames, labelValues)
		w.writeSingleMetricToPrometheus(w.out,
			"table_parts", "Number of parts of the table",
			prometheus.GaugeValue, metric[4],
			labelNames, labelValues)
		w.writeSingleMetricToPrometheus(w.out,
			"table_parts_bytes", "Table size in bytes",
			prometheus.GaugeValue, metric[5],
			labelNames, labelValues)
		w.writeSingleMetricToPrometheus(w.out,
			"table_parts_bytes_uncompressed", "Table size in bytes uncompressed",
			prometheus.GaugeValue, metric[6],
			labelNames, labelValues)
		w.writeSingleMetricToPrometheus(w.out,
			"table_parts_rows", "Number of rows in the table",
			prometheus.GaugeValue, metric[7],
			labelNames, labelValues)
	}
}

// WriteSystemParts pushes set of prometheus.Metric object related to system.parts
func (w *CHIPrometheusWriter) WriteSystemParts(data [][]string) {
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
func (w *CHIPrometheusWriter) WriteSystemReplicas(data [][]string) {
	for _, metric := range data {
		labelNames := []string{"database", "table"}
		labelValues := []string{metric[0], metric[1]}
		w.writeSingleMetricToPrometheus(w.out,
			"system_replicas_is_session_expired", "Number of expired Zookeeper sessions of the table",
			prometheus.GaugeValue, metric[2],
			labelNames, labelValues)
	}
}

// WriteMutations writes mutations
func (w *CHIPrometheusWriter) WriteMutations(data [][]string) {
	for _, metric := range data {
		labelNames := []string{"database", "table"}
		labelValues := []string{metric[0], metric[1]}
		w.writeSingleMetricToPrometheus(w.out,
			"table_mutations", "Number of active mutations for the table",
			prometheus.GaugeValue, metric[2],
			labelNames, labelValues)
		w.writeSingleMetricToPrometheus(w.out,
			"table_mutations_parts_to_do", "Number of data parts that need to be mutated for the mutation to finish",
			prometheus.GaugeValue, metric[3],
			labelNames, labelValues)
	}
}

// WriteSystemDisks writes system disks
func (w *CHIPrometheusWriter) WriteSystemDisks(data [][]string) {
	for _, metric := range data {
		labelNames := []string{"disk"}
		labelValues := []string{metric[0]}
		w.writeSingleMetricToPrometheus(w.out,
			"metric_DiskFreeBytes", "Free disk space available from system.disks",
			prometheus.GaugeValue, metric[1],
			labelNames, labelValues)
		w.writeSingleMetricToPrometheus(w.out,
			"metric_DiskTotalBytes", "Total disk space available from system.disks",
			prometheus.GaugeValue, metric[2],
			labelNames, labelValues)
	}
}

// WriteDetachedParts writes detached parts
func (w *CHIPrometheusWriter) WriteDetachedParts(data [][]string) {
	for _, metric := range data {
		labelNames := []string{"database", "table", "disk", "reason"}
		labelValues := []string{metric[1], metric[2], metric[3], metric[4]}
		w.writeSingleMetricToPrometheus(w.out,
			"metric_DetachedParts", "Count of currently detached parts from system.detached_parts",
			prometheus.GaugeValue, metric[0],
			labelNames, labelValues)
	}
}

// WriteErrorFetch writes error fetch
func (w *CHIPrometheusWriter) WriteErrorFetch(fetchType string) {
	labelNames := []string{"fetch_type"}
	labelValues := []string{fetchType}
	w.writeSingleMetricToPrometheus(w.out,
		"metric_fetch_errors", "status of fetching metrics from ClickHouse 1 - unsuccessful, 0 - successful",
		prometheus.GaugeValue, "1",
		labelNames, labelValues)
}

// WriteOKFetch writes successful fetch
func (w *CHIPrometheusWriter) WriteOKFetch(fetchType string) {
	labelNames := []string{"fetch_type"}
	labelValues := []string{fetchType}
	w.writeSingleMetricToPrometheus(w.out,
		"metric_fetch_errors", "status of fetching metrics from ClickHouse 1 - unsuccessful, 0 - successful",
		prometheus.GaugeValue, "0",
		labelNames, labelValues)
}

func (w *CHIPrometheusWriter) getCHILabels() (labels []string) {
	for label := range w.chi.Labels {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	return labels
}

func (w *CHIPrometheusWriter) getCHILabelValue(label string) (string, bool) {
	value, ok := w.chi.Labels[label]
	return value, ok
}

func (w *CHIPrometheusWriter) getCHILabelValues() []string {
	var labelValues []string
	labels := w.getCHILabels()
	for _, label := range labels {
		if labelValue, ok := w.getCHILabelValue(label); ok {
			labelValues = append(labelValues, labelValue)
		}
	}
	return labelValues
}

func (w *CHIPrometheusWriter) getMandatoryLabels() (mandatoryLabels []string) {
	mandatoryLabels = append(mandatoryLabels, "chi", "namespace", "hostname")
	mandatoryLabels = append(mandatoryLabels, w.getCHILabels()...)
	return mandatoryLabels
}

func (w *CHIPrometheusWriter) getMandatoryLabelValues() (mandatoryLabelValues []string) {
	mandatoryLabelValues = append(mandatoryLabelValues, w.chi.Name, w.chi.Namespace, w.host.Hostname)
	mandatoryLabelValues = append(mandatoryLabelValues, w.getCHILabelValues()...)
	return mandatoryLabelValues
}

func (w *CHIPrometheusWriter) getMandatoryLabelsAndValues() (mandatoryLabels []string, mandatoryLabelValues []string) {
	mandatoryLabels = w.getMandatoryLabels()
	mandatoryLabelValues = w.getMandatoryLabelValues()
	if len(mandatoryLabels) == len(mandatoryLabelValues) {
		return mandatoryLabels, mandatoryLabelValues
	}
	log.Warningf("Unequal number of labels and values for %s/%s/%s", w.chi.Namespace, w.chi.Name, w.host.Hostname)
	return nil, nil
}

func (w *CHIPrometheusWriter) writeSingleMetricToPrometheus(
	out chan<- prometheus.Metric,
	name string,
	desc string,
	metricType prometheus.ValueType,
	value string,
	optionalLabels []string,
	optionalLabelValues []string,
) {
	labels, labelValues := w.getMandatoryLabelsAndValues()
	labels = append(labels, optionalLabels...)
	labelValues = append(labelValues, optionalLabelValues...)

	floatValue, _ := strconv.ParseFloat(value, 64)
	m, err := prometheus.NewConstMetric(
		newDescription(name, desc, labels),
		metricType,
		floatValue,
		labelValues...,
	)
	if err != nil {
		log.Warningf("Error creating metric %s: %s", name, err)
		return
	}
	select {
	case out <- m:
	case <-time.After(writeMetricWaitTimeout):
		log.Warningf("Error sending metric to the channel %s", name)
	}
}

// newDescription creates a new prometheus.Desc object
func newDescription(name, help string, labels []string) *prometheus.Desc {
	return prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subsystem, name),
		help,
		util.BuildPrometheusLabels(labels...),
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
