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
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"sync"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
)

// Exporter implements prometheus.Collector interface
type Exporter struct {
	// chInstallations maps CHI name to list of hostnames (of string type) of this installation
	chInstallations chInstallationsIndex
	chAccessInfo    chAccessInfo

	mutex               sync.RWMutex
	toRemoveFromWatched sync.Map
}

// startMetricsExporter start Prometheus metrics exporter in background
func StartMetricsExporter(username, password string, port int, metricsEP, metricsPath string) *Exporter {
	// Initializing Prometheus Metrics Exporter
	glog.V(1).Infof("Starting metrics exporter at '%s%s'\n", metricsEP, metricsPath)
	exporter := NewExporter(username, password, port)
	prometheus.MustRegister(exporter)
	http.Handle(metricsPath, promhttp.Handler())
	go http.ListenAndServe(metricsEP, nil)

	return exporter
}

type chAccessInfo struct {
	Username string
	Password string
	Port     int
}

type chInstallationsIndex map[string]*chInstallationData

type chInstallationData struct {
	hostnames []string
}

// NewExporter returns a new instance of Exporter type
func NewExporter(username, password string, port int) *Exporter {
	return &Exporter{
		chInstallations: make(map[string]*chInstallationData),
		chAccessInfo: chAccessInfo{
			Username: username,
			Password: password,
			Port:     port,
		},
	}
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
		e.toRemoveFromWatched.Range(func(key, value interface{}) bool {
			switch chiName := key.(type) {
			case string:
				e.toRemoveFromWatched.Delete(key)
				e.removeFromWatched(chiName)
			}
			return true
		})
	}()

	glog.Info("Starting Collect")
	var wg = sync.WaitGroup{}
	e.WalkCHIIndex(func(chiName, hostname string) {
		wg.Add(1)
		go func(chiName, hostname string, c chan<- prometheus.Metric) {
			defer wg.Done()
			e.collectFromHost(chiName, hostname, c)
		}(chiName, hostname, ch)
	})
	wg.Wait()
	glog.Info("Finished Collect")
}

func (e *Exporter) enqueueToRemoveFromWatched(chiName string) {
	e.toRemoveFromWatched.Store(chiName, struct{}{})
}

// collectFromHost collect metrics from one host and write inito chan
func (e *Exporter) collectFromHost(chiName, hostname string, c chan<- prometheus.Metric) {
	fetcher := e.newFetcher(hostname)
	writer := NewPrometheusWriter(c, chiName, hostname)

	glog.Infof("Querying metrics for %s\n", hostname)
	if metrics, err := fetcher.clickHouseQueryMetrics(); err == nil {
		glog.Infof("Extracted %d metrics for %s\n", len(metrics), hostname)
		writer.WriteMetrics(metrics)
	} else {
		// In case of an error fetching data from clickhouse store CHI name in e.cleanup
		glog.Infof("Error querying metrics for %s: %s\n", hostname, err)
		e.enqueueToRemoveFromWatched(chiName)
		return
	}

	glog.Infof("Querying table sizes for %s\n", hostname)
	if tableSizes, err := fetcher.clickHouseQueryTableSizes(); err == nil {
		glog.Infof("Extracted %d table sizes for %s\n", len(tableSizes), hostname)
		writer.WriteTableSizes(tableSizes)
	} else {
		// In case of an error fetching data from clickhouse store CHI name in e.cleanup
		glog.Infof("Error querying table sizes for %s: %s\n", hostname, err)
		e.enqueueToRemoveFromWatched(chiName)
		return
	}
}

func (e *Exporter) WalkCHIIndex(f func(chiName, hostname string)) {
	// Loop over ClickHouseInstallations
	for chiName := range e.chInstallations {
		// Loop over all hostnames of this installation
		for _, hostname := range e.chInstallations[chiName].hostnames {
			f(chiName, hostname)
		}
	}
}

// Describe implements prometheus.Collector Describe method
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(e, ch)
}

// removeFromWatched deletes record from Exporter.chInstallation map identified by chiName key
func (e *Exporter) removeFromWatched(chiName string) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	_, ok := e.chInstallations[chiName]
	if ok {
		// CHI is known
		delete(e.chInstallations, chiName)
	}
}

// addToWatched updates Exporter.chInstallation map with values from chInstances slice
func (e *Exporter) addToWatched(chiName string, hostnames []string) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.chInstallations[chiName] = &chInstallationData{
		hostnames: make([]string, len(hostnames)),
	}
	copy(e.chInstallations[chiName].hostnames, hostnames)
}

// isWatched returns true if Exporter.chInstallation map contains chiName key
// and `hostnames` correspond to Exporter.chInstallations[chiName].hostnames
func (e *Exporter) isWatched(chiName string, hostnames []string) bool {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	_, ok := e.chInstallations[chiName]
	if !ok {
		// CHI is unknown
		return false
	}

	// CHI is known

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

// newFetcher returns new Metrics Fetcher for specified host
func (e *Exporter) newFetcher(hostname string) *ClickHouseFetcher {
	return NewClickHouseFetcher(hostname, e.chAccessInfo.Username, e.chAccessInfo.Password, e.chAccessInfo.Port)
}

// Ensure hostnames of the Pods from CHI object included into chopmetrics.Exporter state
// TODO add namespace handling. It is just skipped for now
func (e *Exporter) UpdateWatch(namespace, chiName string, hostnames []string) {
	if !e.isWatched(chiName, hostnames) {
		glog.V(2).Infof("ClickHouseInstallation (%q): including hostnames into chopmetrics.Exporter", chiName)
		e.addToWatched(chiName, hostnames)
	}
}
