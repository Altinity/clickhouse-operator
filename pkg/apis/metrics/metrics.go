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

	clickhouse "github.com/altinity/clickhouse-operator/pkg/apis/metrics/clickhouse"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "chi"
	subsystem = "clickhouse"
)

const (
	chiLabel      = namespace
	instanceLabel = "hostname"
)

// Exporter implements prometheus.Collector interface
type Exporter struct {
	chInstallations map[string]*chInstallationData
	mutex           sync.RWMutex
	cleanup         sync.Map
}

type chInstallationData struct {
	hostnames []string
}

// newDescription creates a new prometheus.Desc object
func newDescription(name, help string) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName(namespace, subsystem, name), help,
		[]string{chiLabel, instanceLabel}, nil)
}

// CreateExporter returns a new instance of Exporter type
func CreateExporter() *Exporter {
	return &Exporter{
		chInstallations: make(map[string]*chInstallationData),
	}
}

// Describe implements prometheus.Collector Descirbe method
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	for key := range clickhouseMetricsDescriptions {
		ch <- clickhouseMetricsDescriptions[key]
	}
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
		for i := range e.chInstallations[chiName].hostnames {
			wg.Add(1)
			go func(name, hostname string, c chan<- prometheus.Metric) {
				defer wg.Done()
				metricsData := make(map[string]string)
				if err := clickhouse.QueryDataFrom(metricsData, hostname); err != nil {
					e.cleanup.Store(name, struct{}{})
					return
				}
				parseMetricsData(c, metricsData, name, hostname)
			}(chiName, e.chInstallations[chiName].hostnames[i], ch)
		}
	}
	wg.Wait()
}

// parseMetricsData pushes set of prometheus.Metric objects created from the ClickHouse system data
func parseMetricsData(out chan<- prometheus.Metric, data map[string]string, name, hostname string) {
	for metric := range metricsNames {
		value, ok := data[metricsNames[metric]]
		var floatValue float64
		if ok {
			floatValue, _ = strconv.ParseFloat(value, 64)
		} else {
			continue
		}
		m, _ := prometheus.NewConstMetric(
			clickhouseMetricsDescriptions[metric], prometheus.GaugeValue, floatValue,
			name, hostname,
		)
		out <- m
	}
}

// removeInstallationReference deletes record from Exporter.chInstallation map identifed by chiName key
func (e *Exporter) removeInstallationReference(chiName string) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	delete(e.chInstallations, chiName)
}

// UpdateControlledState updates Exporter.chInstallation map with values from chInstances slice
func (e *Exporter) UpdateControlledState(chiName string, chInstances []string) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.chInstallations[chiName] = &chInstallationData{
		hostnames: make([]string, len(chInstances)),
	}
	copy(e.chInstallations[chiName].hostnames, chInstances)
}

// ControlledValuesExist returns true if Exporter.chInstallation map contains chiName key
// and chInstances are correspond to chInstallation.hostnames
func (e *Exporter) ControlledValuesExist(chiName string, chInstances []string) bool {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	_, ok := e.chInstallations[chiName]
	if ok {
		if len(chInstances) != len(e.chInstallations[chiName].hostnames) {
			return false
		}
		for i := range chInstances {
			if chInstances[i] != e.chInstallations[chiName].hostnames[i] {
				return false
			}
		}
		return true
	}
	return false
}
