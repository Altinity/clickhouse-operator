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
	"encoding/json"
	"net/http"
	"sync"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
)

// Exporter implements prometheus.Collector interface
type Exporter struct {
	// chInstallations maps CHI name to list of hostnames (of string type) of this installation
	chInstallations chInstallationsIndex
	chAccessInfo    *CHAccessInfo

	mutex               sync.RWMutex
	toRemoveFromWatched sync.Map
}

var exporter *Exporter

type chInstallationsIndex map[string]*WatchedCHI

func (i chInstallationsIndex) Slice() []*WatchedCHI {
	res := make([]*WatchedCHI, 0)
	for _, chi := range i {
		res = append(res, chi)
	}
	return res
}

// NewExporter returns a new instance of Exporter type
func NewExporter(chAccess *CHAccessInfo) *Exporter {
	return &Exporter{
		chInstallations: make(map[string]*WatchedCHI),
		chAccessInfo:    chAccess,
	}
}

func (e *Exporter) getWatchedCHIs() []*WatchedCHI {
	return e.chInstallations.Slice()
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
			case *WatchedCHI:
				e.toRemoveFromWatched.Delete(key)
				e.removeFromWatched(key.(*WatchedCHI))
				glog.Infof("Removed ClickHouseInstallation (%s/%s) from Exporter", key.(*WatchedCHI).Name, key.(*WatchedCHI).Namespace)
			}
			return true
		})
	}()

	glog.Info("Starting Collect")
	var wg = sync.WaitGroup{}
	e.WalkWatchedChi(func(chi *WatchedCHI, hostname string) {
		wg.Add(1)
		go func(chi *WatchedCHI, hostname string, c chan<- prometheus.Metric) {
			defer wg.Done()
			e.collectFromHost(chi, hostname, c)
		}(chi, hostname, ch)
	})
	wg.Wait()
	glog.Info("Finished Collect")
}

func (e *Exporter) enqueueToRemoveFromWatched(chi *WatchedCHI) {
	e.toRemoveFromWatched.Store(chi, struct{}{})
}

func (e *Exporter) WalkWatchedChi(f func(chi *WatchedCHI, hostname string)) {
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
func (e *Exporter) removeFromWatched(chi *WatchedCHI) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	_, ok := e.chInstallations[chi.indexKey()]
	if ok {
		// CHI is known
		delete(e.chInstallations, chi.indexKey())
	}
}

// updateWatched updates Exporter.chInstallation map with values from chInstances slice
func (e *Exporter) updateWatched(chi *WatchedCHI) {
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
	glog.Infof("Added ClickHouseInstallation (%s/%s): including hostnames into Exporter", chi.Namespace, chi.Name)

	e.chInstallations[chi.indexKey()] = chi
}

// newFetcher returns new Metrics Fetcher for specified host
func (e *Exporter) newFetcher(hostname string) *ClickHouseFetcher {
	return NewClickHouseFetcher(hostname, e.chAccessInfo.Username, e.chAccessInfo.Password, e.chAccessInfo.Port)
}

// Ensure hostnames of the Pods from CHI object included into chopmetrics.Exporter state
func (e *Exporter) UpdateWatch(namespace, chiName string, hostnames []string) {
	chi := &WatchedCHI{
		Namespace: namespace,
		Name:      chiName,
		Hostnames: hostnames,
	}
	e.updateWatched(chi)
}

// collectFromHost collect metrics from one host and write inito chan
func (e *Exporter) collectFromHost(chi *WatchedCHI, hostname string, c chan<- prometheus.Metric) {
	fetcher := e.newFetcher(hostname)
	writer := NewPrometheusWriter(c, chi, hostname)

	glog.Infof("Querying metrics for %s\n", hostname)
	if metrics, err := fetcher.clickHouseQueryMetrics(); err == nil {
		glog.Infof("Extracted %d metrics for %s\n", len(metrics), hostname)
		writer.WriteMetrics(metrics)
		writer.WriteOKFetch("system.metrics")
	} else {
		// In case of an error fetching data from clickhouse store CHI name in e.cleanup
		glog.Infof("Error querying metrics for %s: %s\n", hostname, err)
		writer.WriteErrorFetch("system.metrics")
		//e.enqueueToRemoveFromWatched(chi)
		return
	}

	glog.Infof("Querying table sizes for %s\n", hostname)
	if tableSizes, err := fetcher.clickHouseQueryTableSizes(); err == nil {
		glog.Infof("Extracted %d table sizes for %s\n", len(tableSizes), hostname)
		writer.WriteTableSizes(tableSizes)
		writer.WriteOKFetch("table sizes")
	} else {
		// In case of an error fetching data from clickhouse store CHI name in e.cleanup
		glog.Infof("Error querying table sizes for %s: %s\n", hostname, err)
		writer.WriteErrorFetch("table sizes")
		// e.enqueueToRemoveFromWatched(chi)
		return
	}

	glog.Infof("Querying system replicas for %s\n", hostname)
	if systemReplicas, err := fetcher.clickHouseQuerySystemReplicas(); err == nil {
		glog.Infof("Extracted %d system replicas for %s\n", len(systemReplicas), hostname)
		writer.WriteSystemReplicas(systemReplicas)
		writer.WriteOKFetch("system.replicas")
	} else {
		// In case of an error fetching data from clickhouse store CHI name in e.cleanup
		glog.Infof("Error querying system replicas for %s: %s\n", hostname, err)
		writer.WriteErrorFetch("system.replicas")
		// e.enqueueToRemoveFromWatched(chi)
		return
	}

	glog.Infof("Querying mutations for %s\n", hostname)
	if mutations, err := fetcher.clickHouseQueryMutations(); err == nil {
		glog.Infof("Extracted %d mutations for %s\n", len(mutations), hostname)
		writer.WriteMutations(mutations)
		writer.WriteOKFetch("system.mutations")
	} else {
		// In case of an error fetching data from clickhouse store CHI name in e.cleanup
		glog.Infof("Error querying mutations for %s: %s\n", hostname, err)
		writer.WriteErrorFetch("system.mutations")
		//e.enqueueToRemoveFromWatched(chi)
		return
	}
}

// getWatchedCHI serves HTTP request to get list of watched CHIs
func (e *Exporter) getWatchedCHI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(e.getWatchedCHIs())
}

// updateWatchedCHI serves HTTPS request to add CHI to the list of watched CHIs
func (e *Exporter) updateWatchedCHI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	chi := &WatchedCHI{}
	if err := json.NewDecoder(r.Body).Decode(chi); err == nil {
		if chi.isValid() {
			e.updateWatched(chi)
			return
		}
	}

	http.Error(w, "Unable to parse CHI.", http.StatusNotAcceptable)
}

// deleteWatchedCHI serves HTTP request to delete CHI from the list of watched CHIs
func (e *Exporter) deleteWatchedCHI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	chi := &WatchedCHI{}
	if err := json.NewDecoder(r.Body).Decode(chi); err == nil {
		if chi.isValid() {
			e.enqueueToRemoveFromWatched(chi)
			return
		}
	}

	http.Error(w, "Unable to parse CHI.", http.StatusNotAcceptable)
}
