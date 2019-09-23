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

// Exporter implements prometheus.Collector interface
type Exporter struct {
	// chInstallations maps CHI name to list of hostnames (of string type) of this installation
	chInstallations chInstallationsIndex
	chAccessInfo    chAccessInfo

	mutex               sync.RWMutex
	toRemoveFromWatched sync.Map
}

type WatchedChi struct {
	Namespace string   `json:"namespace"`
	Name      string   `json:"name"`
	Hostnames []string `json:"hostnames"`
}

func (chi *WatchedChi) indexKey() string {
	return chi.Namespace + ":" + chi.Name
}

func (chi *WatchedChi) empty() bool {
	return (len(chi.Namespace) == 0) && (len(chi.Name) == 0) && (len(chi.Hostnames) == 0)
}

func (chi *WatchedChi) equal(chi2 *WatchedChi) bool {
	// Must have the same namespace
	if chi.Namespace != chi2.Namespace {
		return false
	}

	// Must have the same name
	if chi.Name != chi2.Name {
		return false
	}

	// Must have the same number of items
	if len(chi.Hostnames) != len(chi2.Hostnames) {
		return false
	}

	// Must have the same items
	for i := range chi.Hostnames {
		if chi.Hostnames[i] != chi2.Hostnames[i] {
			return false
		}
	}

	// All checks passed
	return true
}

var exporter *Exporter

type chAccessInfo struct {
	Username string
	Password string
	Port     int
}

type chInstallationsIndex map[string]*WatchedChi

func (i chInstallationsIndex) Slice() []*WatchedChi {
	res := make([]*WatchedChi, 0)
	for _, chi := range i {
		res = append(res, chi)
	}
	return res
}

// NewExporter returns a new instance of Exporter type
func NewExporter(username, password string, port int) *Exporter {
	return &Exporter{
		chInstallations: make(map[string]*WatchedChi),
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
			switch key.(type) {
			case *WatchedChi:
				e.toRemoveFromWatched.Delete(key)
				e.removeFromWatched(key.(*WatchedChi))
			}
			return true
		})
	}()

	glog.Info("Starting Collect")
	var wg = sync.WaitGroup{}
	e.WalkWatchedChi(func(chi *WatchedChi, hostname string) {
		wg.Add(1)
		go func(chi *WatchedChi, hostname string, c chan<- prometheus.Metric) {
			defer wg.Done()
			e.collectFromHost(chi, hostname, c)
		}(chi, hostname, ch)
	})
	wg.Wait()
	glog.Info("Finished Collect")
}

func (e *Exporter) enqueueToRemoveFromWatched(chi *WatchedChi) {
	e.toRemoveFromWatched.Store(chi, struct{}{})
}

func (e *Exporter) WalkWatchedChi(f func(chi *WatchedChi, hostname string)) {
	// Loop over ClickHouseInstallations
	for _, chi := range e.chInstallations {
		// Loop over all hostnames of this installation
		for _, hostname := range chi.Hostnames {
			f(chi, hostname)
		}
	}
}

// Describe implements prometheus.Collector Describe method
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(e, ch)
}

// removeFromWatched deletes record from Exporter.chInstallation map identified by chiName key
func (e *Exporter) removeFromWatched(chi *WatchedChi) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	_, ok := e.chInstallations[chi.indexKey()]
	if ok {
		// CHI is known
		delete(e.chInstallations, chi.indexKey())
	}
}

// addToWatched updates Exporter.chInstallation map with values from chInstances slice
func (e *Exporter) addToWatched(chi *WatchedChi) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	knownChi, ok := e.chInstallations[chi.indexKey()]
	if ok {
		// CHI is known
		if chi.equal(knownChi) {
			// Already watched
			return
		}
	}

	// CHI is not watched
	glog.V(2).Infof("ClickHouseInstallation (%s/%s): including hostnames into Exporter", chi.Namespace, chi.Name)

	e.chInstallations[chi.indexKey()] = chi
}

// newFetcher returns new Metrics Fetcher for specified host
func (e *Exporter) newFetcher(hostname string) *ClickHouseFetcher {
	return NewClickHouseFetcher(hostname, e.chAccessInfo.Username, e.chAccessInfo.Password, e.chAccessInfo.Port)
}

// Ensure hostnames of the Pods from CHI object included into chopmetrics.Exporter state
func (e *Exporter) UpdateWatch(namespace, chiName string, hostnames []string) {
	chi := &WatchedChi{
		Namespace: namespace,
		Name:      chiName,
		Hostnames: hostnames,
	}
	e.addToWatched(chi)
}

// collectFromHost collect metrics from one host and write inito chan
func (e *Exporter) collectFromHost(chi *WatchedChi, hostname string, c chan<- prometheus.Metric) {
	fetcher := e.newFetcher(hostname)
	writer := NewPrometheusWriter(c, chi, hostname)

	glog.Infof("Querying metrics for %s\n", hostname)
	if metrics, err := fetcher.clickHouseQueryMetrics(); err == nil {
		glog.Infof("Extracted %d metrics for %s\n", len(metrics), hostname)
		writer.WriteMetrics(metrics)
	} else {
		// In case of an error fetching data from clickhouse store CHI name in e.cleanup
		glog.Infof("Error querying metrics for %s: %s\n", hostname, err)
		e.enqueueToRemoveFromWatched(chi)
		return
	}

	glog.Infof("Querying table sizes for %s\n", hostname)
	if tableSizes, err := fetcher.clickHouseQueryTableSizes(); err == nil {
		glog.Infof("Extracted %d table sizes for %s\n", len(tableSizes), hostname)
		writer.WriteTableSizes(tableSizes)
	} else {
		// In case of an error fetching data from clickhouse store CHI name in e.cleanup
		glog.Infof("Error querying table sizes for %s: %s\n", hostname, err)
		e.enqueueToRemoveFromWatched(chi)
		return
	}
}
