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
	"sync"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
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
	chAccessInfo    ClickHouseAccessInfo
}

type ClickHouseAccessInfo struct {
	Username string
	Password string
	Port     int
}

type chInstallationData struct {
	hostnames []string
}

// NewExporter returns a new instance of Exporter type
func NewExporter(username, password string, port int) *Exporter {
	return &Exporter{
		chInstallations: make(map[string]*chInstallationData),
		chAccessInfo: ClickHouseAccessInfo{
			Username: username,
			Password: password,
			Port:     port,
		},
	}
}

// newFetcher returns new Metrics Fetcher for specified host
func (e *Exporter) newFetcher(hostname string) *Fetcher {
	return NewFetcher(hostname, e.chAccessInfo.Username, e.chAccessInfo.Password, e.chAccessInfo.Port)
}

// Describe implements prometheus.Collector Describe method
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(e, ch)
}

// Collect implements prometheus.Collector Collect method
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	if ch == nil {
		glog.Info("Prometheus channel is closed. Skipping")
		return
	}

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

	glog.Info("Starting Collect")
	var wg = sync.WaitGroup{}
	// Getting hostnames of Pods and requesting the metrics data from ClickHouse instances within
	for chiName := range e.chInstallations {
		// Loop over all hostnames of this installation
		glog.Infof("Collecting metrics for %s\n", chiName)
		for _, hostname := range e.chInstallations[chiName].hostnames {
			wg.Add(1)
			go func(name, hostname string, c chan<- prometheus.Metric) {
				defer wg.Done()

				glog.Infof("Querying metrics for %s\n", hostname)
				metricsData := make([][]string, 0)
				fetcher := e.newFetcher(hostname)
				if err := fetcher.clickHouseQueryMetrics(&metricsData); err != nil {
					// In case of an error fetching data from clickhouse store CHI name in e.cleanup
					glog.Infof("Error querying metrics for %s: %s\n", hostname, err)
					e.cleanup.Store(name, struct{}{})
					return
				}
				glog.Infof("Extracted %d metrics for %s\n", len(metricsData), hostname)
				writeMetricsDataToPrometheus(c, metricsData, name, hostname)

				glog.Infof("Querying table sizes for %s\n", hostname)
				tableSizes := make([][]string, 0)
				if err := fetcher.clickHouseQueryTableSizes(&tableSizes); err != nil {
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
	glog.Info("Finished Collect")
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
