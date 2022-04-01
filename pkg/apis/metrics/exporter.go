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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	log "github.com/golang/glog"
	// log "k8s.io/klog"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/altinity/clickhouse-operator/pkg/chop"
	chopclientset "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
)

const (
	defaultTimeout = 30 * time.Second
)

// Exporter implements prometheus.Collector interface
type Exporter struct {
	// chInstallations maps CHI name to list of hostnames (of string type) of this installation
	chInstallations chInstallationsIndex
	chAccessInfo    *CHAccessInfo
	timeout         time.Duration

	mutex               sync.RWMutex
	toRemoveFromWatched sync.Map
}

var exporter *Exporter

type chInstallationsIndex map[string]*WatchedCHI

// Slice
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
		timeout:         defaultTimeout,
	}
}

// getWatchedCHIs
func (e *Exporter) getWatchedCHIs() []*WatchedCHI {
	return e.chInstallations.Slice()
}

// Collect implements prometheus.Collector Collect method
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	if ch == nil {
		log.V(2).Info("Prometheus channel is closed. Skipping")
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
				log.V(1).Infof("Removed ClickHouseInstallation (%s/%s) from Exporter", key.(*WatchedCHI).Name, key.(*WatchedCHI).Namespace)
			}
			return true
		})
	}()

	log.V(2).Info("Starting Collect")
	var wg = sync.WaitGroup{}
	e.WalkWatchedChi(func(chi *WatchedCHI, hostname string) {
		wg.Add(1)
		go func(chi *WatchedCHI, hostname string, c chan<- prometheus.Metric) {
			defer wg.Done()
			e.collectFromHost(chi, hostname, c)
		}(chi, hostname, ch)
	})
	wg.Wait()
	log.V(2).Info("Finished Collect")
}

// enqueueToRemoveFromWatched
func (e *Exporter) enqueueToRemoveFromWatched(chi *WatchedCHI) {
	e.toRemoveFromWatched.Store(chi, struct{}{})
}

// WalkWatchedChi walks over watched CHI objects
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
	log.V(1).Infof("Added ClickHouseInstallation (%s/%s): including hostnames into Exporter", chi.Namespace, chi.Name)

	e.chInstallations[chi.indexKey()] = chi
}

// newFetcher returns new Metrics Fetcher for specified host
func (e *Exporter) newFetcher(hostname string) *ClickHouseFetcher {
	return NewClickHouseFetcher(
		e.chAccessInfo.Scheme,
		hostname,
		e.chAccessInfo.Username,
		e.chAccessInfo.Password,
		e.chAccessInfo.Port,
	).SetQueryTimeout(e.timeout)
}

// UpdateWatch ensures hostnames of the Pods from CHI object included into metrics.Exporter state
func (e *Exporter) UpdateWatch(namespace, chiName string, hostnames []string) {
	chi := &WatchedCHI{
		Namespace: namespace,
		Name:      chiName,
		Hostnames: hostnames,
	}
	e.updateWatched(chi)
}

// collectFromHost collects metrics from one host and writes them into chan
func (e *Exporter) collectFromHost(chi *WatchedCHI, hostname string, c chan<- prometheus.Metric) {
	fetcher := e.newFetcher(hostname)
	writer := NewPrometheusWriter(c, chi, hostname)

	log.V(2).Infof("Querying metrics for %s\n", hostname)
	if metrics, err := fetcher.getClickHouseQueryMetrics(); err == nil {
		log.V(2).Infof("Extracted %d metrics for %s\n", len(metrics), hostname)
		writer.WriteMetrics(metrics)
		writer.WriteOKFetch("system.metrics")
	} else {
		// In case of an error fetching data from clickhouse store CHI name in e.cleanup
		log.V(2).Infof("Error querying metrics for %s: %s\n", hostname, err)
		writer.WriteErrorFetch("system.metrics")
	}

	log.V(2).Infof("Querying table sizes for %s\n", hostname)
	if systemPartsData, err := fetcher.getClickHouseSystemParts(); err == nil {
		log.V(2).Infof("Extracted %d table sizes for %s\n", len(systemPartsData), hostname)
		writer.WriteTableSizes(systemPartsData)
		writer.WriteOKFetch("table sizes")
		writer.WriteSystemParts(systemPartsData)
		writer.WriteOKFetch("system parts")

	} else {
		// In case of an error fetching data from clickhouse store CHI name in e.cleanup
		log.V(2).Infof("Error querying system.parts for %s: %s\n", hostname, err)
		writer.WriteErrorFetch("table sizes")
		writer.WriteErrorFetch("system parts")
	}

	log.V(2).Infof("Querying system replicas for %s\n", hostname)
	if systemReplicas, err := fetcher.getClickHouseQuerySystemReplicas(); err == nil {
		log.V(2).Infof("Extracted %d system replicas for %s\n", len(systemReplicas), hostname)
		writer.WriteSystemReplicas(systemReplicas)
		writer.WriteOKFetch("system.replicas")
	} else {
		// In case of an error fetching data from clickhouse store CHI name in e.cleanup
		log.V(2).Infof("Error querying system replicas for %s: %s\n", hostname, err)
		writer.WriteErrorFetch("system.replicas")
	}

	log.V(2).Infof("Querying mutations for %s\n", hostname)
	if mutations, err := fetcher.getClickHouseQueryMutations(); err == nil {
		log.V(2).Infof("Extracted %d mutations for %s\n", len(mutations), hostname)
		writer.WriteMutations(mutations)
		writer.WriteOKFetch("system.mutations")
	} else {
		// In case of an error fetching data from clickhouse store CHI name in e.cleanup
		log.V(2).Infof("Error querying mutations for %s: %s\n", hostname, err)
		writer.WriteErrorFetch("system.mutations")
	}

	log.V(2).Infof("Querying disks for %s\n", hostname)
	if disks, err := fetcher.getClickHouseQuerySystemDisks(); err == nil {
		log.V(2).Infof("Extracted %d disks for %s\n", len(disks), hostname)
		writer.WriteSystemDisks(disks)
		writer.WriteOKFetch("system.disks")
	} else {
		// In case of an error fetching data from clickhouse store CHI name in e.cleanup
		log.V(2).Infof("Error querying disks for %s: %s\n", hostname, err)
		writer.WriteErrorFetch("system.disks")
	}

	log.V(2).Infof("Querying detached parts for %s\n", hostname)
	if detachedParts, err := fetcher.getClickHouseQueryDetachedParts(); err == nil {
		log.V(2).Infof("Extracted %d detached parts info for %s\n", len(detachedParts), hostname)
		writer.WriteDetachedParts(detachedParts)
		writer.WriteOKFetch("system.detached_parts")
	} else {
		// In case of an error fetching data from clickhouse store CHI name in e.cleanup
		log.V(2).Infof("Error querying detached parts for %s: %s\n", hostname, err)
		writer.WriteErrorFetch("system.detached_parts")
	}
}

// getWatchedCHI serves HTTP request to get list of watched CHIs
func (e *Exporter) getWatchedCHI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(e.getWatchedCHIs())
}

// fetchCHI decodes chi from request
func (e *Exporter) fetchCHI(r *http.Request) (*WatchedCHI, error) {
	chi := &WatchedCHI{}
	if err := json.NewDecoder(r.Body).Decode(chi); err == nil {
		if chi.isValid() {
			return chi, nil
		}
	}

	return nil, fmt.Errorf("unable to parse CHI from request")
}

// updateWatchedCHI serves HTTPS request to add CHI to the list of watched CHIs
func (e *Exporter) updateWatchedCHI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if chi, err := e.fetchCHI(r); err == nil {
		e.updateWatched(chi)
	} else {
		http.Error(w, err.Error(), http.StatusNotAcceptable)
	}
}

// deleteWatchedCHI serves HTTP request to delete CHI from the list of watched CHIs
func (e *Exporter) deleteWatchedCHI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if chi, err := e.fetchCHI(r); err == nil {
		e.enqueueToRemoveFromWatched(chi)
	} else {
		http.Error(w, err.Error(), http.StatusNotAcceptable)
	}
}

// DiscoveryWatchedCHIs discovers all ClickHouseInstallation objects available for monitoring and adds them to watched list
func (e *Exporter) DiscoveryWatchedCHIs(chopClient *chopclientset.Clientset) {
	// Get all CHI objects from watched namespace(s)
	watchedNamespace := chop.Config().GetInformerNamespace()
	list, err := chopClient.ClickhouseV1().ClickHouseInstallations(watchedNamespace).List(context.TODO(), v1.ListOptions{})
	if err != nil {
		log.V(1).Infof("Error read ClickHouseInstallations %v", err)
		return
	}
	if list == nil {
		return
	}

	// Walk over the list of ClickHouseInstallation objects and add them as watched
	for i := range list.Items {
		chi := &list.Items[i]
		if chi.IsStopped() {
			log.V(1).Infof("Skip stopped CHI %s/%s with %d hosts\n", chi.Namespace, chi.Name, len(chi.Status.FQDNs))
		} else {
			log.V(1).Infof("Add explicitly found CHI %s/%s with %d hosts\n", chi.Namespace, chi.Name, len(chi.Status.FQDNs))
			watchedCHI := &WatchedCHI{
				Namespace: chi.Namespace,
				Name:      chi.Name,
				Hostnames: chi.Status.FQDNs,
			}
			e.updateWatched(watchedCHI)
		}
	}
}
