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
	"strconv"
	"sync"
	"unicode"
	"strings"

	clickhouse "github.com/altinity/clickhouse-operator/pkg/apis/metrics/clickhouse"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/golang/glog"
)

const (
	namespace = "chi"
	subsystem = "clickhouse"
)

// Exporter implements prometheus.Collector interface
type Exporter struct {
	// chInstallations maps CHI name to list of hostnames (of string type) of this installation
	chInstallations map[string]*chInstallationData
	mutex           sync.RWMutex
	cleanup         sync.Map
}

type chInstallationData struct {
	hostnames []string
}

// CreateExporter returns a new instance of Exporter type
func CreateExporter() *Exporter {
	return &Exporter{
		chInstallations: make(map[string]*chInstallationData),
	}
}

// Describe implements prometheus.Collector Describe method
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(e, ch)
}

// Collect implements prometheus.Collector Collect method
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.mutex.Lock()
	defer func() {
		e.mutex.Unlock()
		e.cleanup.Range(func(key, value interface{}) bool {
			switch chiName := key.(type) {
			case string:
				e.cleanup.Delete(key)
				e.removeInstallationReference(chiName)
			}
			return true
		})
	}()

	wg := &sync.WaitGroup{}
	// Getting hostnames of Pods and requesting the metrics data from ClickHouse instances within
	for chiName := range e.chInstallations {
		// Loop over all hostnames of this installation
		glog.Infof("Collect metrics for %s\n", chiName)
		for _, hostname := range e.chInstallations[chiName].hostnames {
			wg.Add(1)
			go func(name, hostname string, c chan<- prometheus.Metric) {
				defer wg.Done()

				glog.Infof("Querying metrics for %s\n", hostname)
				metricsData := make([][]string,0)
				if err := clickhouse.QueryMetrics(metricsData, hostname); err != nil {
					// In case of an error fetching data from clickhouse store CHI name in e.cleanup
					glog.Infof("Error querying metrics for %s: %s\n", hostname, err)
					e.cleanup.Store(name, struct{}{})
					return
				}
				glog.Infof("Extracted %d metrics for %s\n", len(metricsData), hostname)
				writeMetricsDataToPrometheus(c, metricsData, name, hostname)
				
				glog.Infof("Querying table sizes for %s\n", hostname)
				tableSizes := make([][]string,0)
				if err := clickhouse.QueryTableSizes(tableSizes, hostname); err != nil {
					// In case of an error fetching data from clickhouse store CHI name in e.cleanup
					glog.Infof("Error querying table sizes for %s: %s\n", hostname, err)
					e.cleanup.Store(name, struct{}{})
					return
				}
				glog.Infof("Extracted %d table sizes for %s\n", len(tableSizes), hostname)
				writeTableSizesDataToPrometheus(c, tableSizes, name, hostname)
				
			}(chiName, hostname, ch)
		}
	}
	wg.Wait()
}


// writeMetricsDataToPrometheus pushes set of prometheus.Metric objects created from the ClickHouse system data
// Expected data structure: metric, value, description, type (gauge|counter)
func writeMetricsDataToPrometheus(out chan<- prometheus.Metric, data [][]string, chiname, hostname string) {
	for _, metric := range data {
		var metricType prometheus.ValueType
		if metric[4] == "counter" { metricType = prometheus.CounterValue } else { metricType = prometheus.GaugeValue }
		writeSingleMetricToPrometheus(out, "table_partitions", "Number of partitions of the table", metric[3], metricType,
			[]string{"chi","hostname"}, 
			chiname, hostname)
	}
}

// writeTableSizesDataToPrometheus pushes set of prometheus.Metric objects created from the ClickHouse system data
// Expected data structure: database, table, partitions, parts, bytes, uncompressed_bytes, rows
func writeTableSizesDataToPrometheus(out chan<- prometheus.Metric, data [][]string, chiname, hostname string) {
	for _, metric := range data {
		writeSingleMetricToPrometheus(out, "table_partitions", "Number of partitions of the table", metric[3], prometheus.GaugeValue,
			[]string{"chi","hostname","database","table"}, 
			chiname, hostname, metric[1], metric[2])
		writeSingleMetricToPrometheus(out, "table_parts", "Number of parts of the table", metric[4], prometheus.GaugeValue,
			[]string{"chi","hostname","database","table"}, 
			chiname, hostname, metric[1], metric[2])
		writeSingleMetricToPrometheus(out, "table_parts_bytes", "Table size in bytes", metric[5], prometheus.GaugeValue,
			[]string{"chi","hostname","database","table"}, 
			chiname, hostname, metric[1], metric[2])
		writeSingleMetricToPrometheus(out, "table_parts_bytes_uncompressed", "Table size in bytes uncompressed", metric[6], prometheus.GaugeValue, 
			[]string{"chi","hostname","database","table"}, 
			chiname, hostname, metric[1], metric[2])
		writeSingleMetricToPrometheus(out, "table_parts_rows", "Number of rows in the table", metric[7], prometheus.GaugeValue,
			[]string{"chi","hostname","database","table"}, 
			chiname, hostname, metric[1], metric[2])
	}
}

func writeSingleMetricToPrometheus(out chan<- prometheus.Metric, name string, desc string, value string, metricType prometheus.ValueType, labels []string, labelValues ...string) {
	floatValue, _ := strconv.ParseFloat(value, 64)
	m, _ := prometheus.NewConstMetric(
			newDescription(name, desc, labels),
			metricType,
			floatValue,
			labelValues...)
	out <- m
}

// removeInstallationReference deletes record from Exporter.chInstallation map identified by chiName key
func (e *Exporter) removeInstallationReference(chiName string) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	delete(e.chInstallations, chiName)
}

// UpdateControlledState updates Exporter.chInstallation map with values from chInstances slice
func (e *Exporter) UpdateControlledState(chiName string, hostnames []string) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.chInstallations[chiName] = &chInstallationData{
		hostnames: make([]string, len(hostnames)),
	}
	copy(e.chInstallations[chiName].hostnames, hostnames)
}

// ControlledValuesExist returns true if Exporter.chInstallation map contains chiName key
// and `hostnames` correspond to Exporter.chInstallations[chiName].hostnames
func (e *Exporter) ControlledValuesExist(chiName string, hostnames []string) bool {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	_, ok := e.chInstallations[chiName]
	if !ok {
		return false
	}

	// chiName exists

	// Must have the same number of items
	if len(hostnames) != len(e.chInstallations[chiName].hostnames) {
		return false
	}

	// Must have the same items
	for i := range hostnames {
		if hostnames[i] != e.chInstallations[chiName].hostnames[i] {
			return false
		}
	}

	// All checks passed
	return true
}

func (e *Exporter) EnsureControlledValues(chiName string, hostnames []string) {
	if !e.ControlledValuesExist(chiName, hostnames) {
		glog.V(2).Infof("ClickHouseInstallation (%q): including hostnames into chopmetrics.Exporter", chiName)
		e.UpdateControlledState(chiName, hostnames)
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

// metricName converts the given string to snake case following the Golang format:
// acronyms are converted to lower-case and preceded by an underscore.
func metricName(in string) string {
	runes := []rune(in)
	length := len(runes)

	var out []rune
	for i := 0; i < length; i++ {
		if i > 0 && unicode.IsUpper(runes[i]) && ((i+1 < length && unicode.IsLower(runes[i+1])) || unicode.IsLower(runes[i-1])) {
			out = append(out, '_')
		}
		out = append(out, unicode.ToLower(runes[i]))
	}

	return strings.Replace(string(out), ".", "_", -1)
}
