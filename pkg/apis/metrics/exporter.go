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
	"github.com/prometheus/client_golang/prometheus"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	kube "k8s.io/client-go/kubernetes"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	chopAPI "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	chiNormalizer "github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"
)

// Exporter implements prometheus.Collector interface
type Exporter struct {
	collectorTimeout time.Duration

	// chInstallations maps CHI name to list of hostnames (of string type) of this installation
	chInstallations chInstallationsIndex

	mutex               sync.RWMutex
	toRemoveFromWatched sync.Map
}

// Type compatibility
var _ prometheus.Collector = &Exporter{}

// NewExporter returns a new instance of Exporter type
func NewExporter(collectorTimeout time.Duration) *Exporter {
	return &Exporter{
		chInstallations:  make(map[string]*WatchedCHI),
		collectorTimeout: collectorTimeout,
	}
}

// getWatchedCHIs
func (e *Exporter) getWatchedCHIs() []*WatchedCHI {
	return e.chInstallations.slice()
}

// Collect implements prometheus.Collector Collect method
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	// Run cleanup on each collect
	e.cleanup()

	if ch == nil {
		log.Warning("Prometheus channel is closed. Unable to write metrics")
		return
	}

	start := time.Now()

	log.V(1).Info("Collect started")
	defer func() {
		log.V(1).Infof("Collect completed [%s]", time.Now().Sub(start))
	}()

	// Collect should have timeout
	ctx, cancel := context.WithTimeout(context.Background(), e.collectorTimeout)
	defer cancel()

	// This method may be called concurrently and must therefore be implemented in a concurrency safe way
	e.mutex.Lock()
	defer e.mutex.Unlock()

	log.V(1).Infof("Launching host collectors [%s]", time.Now().Sub(start))

	var wg = sync.WaitGroup{}
	e.chInstallations.walk(func(chi *WatchedCHI, _ *WatchedCluster, host *WatchedHost) {
		wg.Add(1)
		go func(ctx context.Context, chi *WatchedCHI, host *WatchedHost, ch chan<- prometheus.Metric) {
			defer wg.Done()
			e.collectHostMetrics(ctx, chi, host, ch)
		}(ctx, chi, host, ch)
	})
	wg.Wait()
}

// Describe implements prometheus.Collector Describe method
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(e, ch)
}

// enqueueToRemoveFromWatched
func (e *Exporter) enqueueToRemoveFromWatched(chi *WatchedCHI) {
	e.toRemoveFromWatched.Store(chi, struct{}{})
}

// cleanup cleans all pending for cleaning
func (e *Exporter) cleanup() {
	// Clean up all pending for cleaning CHIs
	log.V(2).Info("Starting cleanup")
	e.toRemoveFromWatched.Range(func(key, value interface{}) bool {
		switch key.(type) {
		case *WatchedCHI:
			e.toRemoveFromWatched.Delete(key)
			e.removeFromWatched(key.(*WatchedCHI))
			log.V(1).Infof("Removed ClickHouseInstallation (%s/%s) from Exporter", key.(*WatchedCHI).Name, key.(*WatchedCHI).Namespace)
		}
		return true
	})
	log.V(2).Info("Completed cleanup")
}

// removeFromWatched deletes record from Exporter.chInstallation map identified by chiName key
func (e *Exporter) removeFromWatched(chi *WatchedCHI) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	log.V(1).Infof("Remove ClickHouseInstallation (%s/%s)", chi.Namespace, chi.Name)
	e.chInstallations.remove(chi.indexKey())
}

// updateWatched updates Exporter.chInstallation map with values from chInstances slice
func (e *Exporter) updateWatched(chi *WatchedCHI) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	log.V(1).Infof("Update ClickHouseInstallation (%s/%s): %s", chi.Namespace, chi.Name, chi)
	e.chInstallations.set(chi.indexKey(), chi)
}

// newFetcher returns new Metrics Fetcher for specified host
func (e *Exporter) newHostFetcher(host *WatchedHost) *ClickHouseMetricsFetcher {
	// Make base cluster connection params
	clusterConnectionParams := clickhouse.NewClusterConnectionParamsFromCHOpConfig(chop.Config())
	// Adjust base cluster connection params with per-host props
	switch clusterConnectionParams.Scheme {
	case api.ChSchemeAuto:
		switch {
		case api.IsPortAssigned(host.HTTPPort):
			clusterConnectionParams.Scheme = "http"
			clusterConnectionParams.Port = int(host.HTTPPort)
		case api.IsPortAssigned(host.HTTPSPort):
			clusterConnectionParams.Scheme = "https"
			clusterConnectionParams.Port = int(host.HTTPSPort)
		}
	case api.ChSchemeHTTP:
		clusterConnectionParams.Port = int(host.HTTPPort)
	case api.ChSchemeHTTPS:
		clusterConnectionParams.Port = int(host.HTTPSPort)
	}

	return NewClickHouseFetcher(clusterConnectionParams.NewEndpointConnectionParams(host.Hostname))
}

// collectHostMetrics collects metrics from one host and writes them into chan
func (e *Exporter) collectHostMetrics(ctx context.Context, chi *WatchedCHI, host *WatchedHost, c chan<- prometheus.Metric) {
	fetcher := e.newHostFetcher(host)
	writer := NewCHIPrometheusWriter(c, chi, host)

	wg := sync.WaitGroup{}
	wg.Add(6)
	go func(ctx context.Context, host *WatchedHost, fetcher *ClickHouseMetricsFetcher, writer *CHIPrometheusWriter) {
		e.collectHostSystemMetrics(ctx, host, fetcher, writer)
		wg.Done()
	}(ctx, host, fetcher, writer)
	go func(ctx context.Context, host *WatchedHost, fetcher *ClickHouseMetricsFetcher, writer *CHIPrometheusWriter) {
		e.collectHostSystemPartsMetrics(ctx, host, fetcher, writer)
		wg.Done()
	}(ctx, host, fetcher, writer)
	go func(ctx context.Context, host *WatchedHost, fetcher *ClickHouseMetricsFetcher, writer *CHIPrometheusWriter) {
		e.collectHostSystemReplicasMetrics(ctx, host, fetcher, writer)
		wg.Done()
	}(ctx, host, fetcher, writer)
	go func(ctx context.Context, host *WatchedHost, fetcher *ClickHouseMetricsFetcher, writer *CHIPrometheusWriter) {
		e.collectHostMutationsMetrics(ctx, host, fetcher, writer)
		wg.Done()
	}(ctx, host, fetcher, writer)
	go func(ctx context.Context, host *WatchedHost, fetcher *ClickHouseMetricsFetcher, writer *CHIPrometheusWriter) {
		e.collectHostSystemDisksMetrics(ctx, host, fetcher, writer)
		wg.Done()
	}(ctx, host, fetcher, writer)
	go func(ctx context.Context, host *WatchedHost, fetcher *ClickHouseMetricsFetcher, writer *CHIPrometheusWriter) {
		e.collectHostDetachedPartsMetrics(ctx, host, fetcher, writer)
		wg.Done()
	}(ctx, host, fetcher, writer)
	wg.Wait()
}

func (e *Exporter) collectHostSystemMetrics(
	ctx context.Context,
	host *WatchedHost,
	fetcher *ClickHouseMetricsFetcher,
	writer *CHIPrometheusWriter,
) {
	log.V(1).Infof("Querying system metrics for host %s", host.Hostname)
	start := time.Now()
	metrics, err := fetcher.getClickHouseQueryMetrics(ctx)
	elapsed := time.Now().Sub(start)
	if err == nil {
		log.V(1).Infof("Extracted [%s] %d system metrics for host %s", elapsed, len(metrics), host.Hostname)
		writer.WriteMetrics(metrics)
		writer.WriteOKFetch("system.metrics")
	} else {
		// In case of an error fetching data from clickhouse store CHI name in e.cleanup
		log.Warningf("Error [%s] querying system.metrics for host %s err: %s", elapsed, host.Hostname, err)
		writer.WriteErrorFetch("system.metrics")
	}
}

func (e *Exporter) collectHostSystemPartsMetrics(
	ctx context.Context,
	host *WatchedHost,
	fetcher *ClickHouseMetricsFetcher,
	writer *CHIPrometheusWriter,
) {
	log.V(1).Infof("Querying table sizes for host %s", host.Hostname)
	start := time.Now()
	systemPartsData, err := fetcher.getClickHouseSystemParts(ctx)
	elapsed := time.Now().Sub(start)
	if err == nil {
		log.V(1).Infof("Extracted [%s] %d table sizes for host %s", elapsed, len(systemPartsData), host.Hostname)
		writer.WriteTableSizes(systemPartsData)
		writer.WriteOKFetch("table sizes")
		writer.WriteSystemParts(systemPartsData)
		writer.WriteOKFetch("system parts")
	} else {
		// In case of an error fetching data from clickhouse store CHI name in e.cleanup
		log.Warningf("Error [%s] querying system.parts for host %s err: %s", elapsed, host.Hostname, err)
		writer.WriteErrorFetch("table sizes")
		writer.WriteErrorFetch("system parts")
	}
}

func (e *Exporter) collectHostSystemReplicasMetrics(
	ctx context.Context,
	host *WatchedHost,
	fetcher *ClickHouseMetricsFetcher,
	writer *CHIPrometheusWriter,
) {
	log.V(1).Infof("Querying system replicas for host %s", host.Hostname)
	start := time.Now()
	systemReplicas, err := fetcher.getClickHouseQuerySystemReplicas(ctx)
	elapsed := time.Now().Sub(start)
	if err == nil {
		log.V(1).Infof("Extracted [%s] %d system replicas for host %s", elapsed, len(systemReplicas), host.Hostname)
		writer.WriteSystemReplicas(systemReplicas)
		writer.WriteOKFetch("system.replicas")
	} else {
		// In case of an error fetching data from clickhouse store CHI name in e.cleanup
		log.Warningf("Error [%s] querying system.replicas for host %s err: %s", elapsed, host.Hostname, err)
		writer.WriteErrorFetch("system.replicas")
	}
}

func (e *Exporter) collectHostMutationsMetrics(
	ctx context.Context,
	host *WatchedHost,
	fetcher *ClickHouseMetricsFetcher,
	writer *CHIPrometheusWriter,
) {
	log.V(1).Infof("Querying mutations for host %s", host.Hostname)
	start := time.Now()
	mutations, err := fetcher.getClickHouseQueryMutations(ctx)
	elapsed := time.Now().Sub(start)
	if err == nil {
		log.V(1).Infof("Extracted [%s] %d mutations for %s", elapsed, len(mutations), host.Hostname)
		writer.WriteMutations(mutations)
		writer.WriteOKFetch("system.mutations")
	} else {
		// In case of an error fetching data from clickhouse store CHI name in e.cleanup
		log.Warningf("Error [%s] querying system.mutations for host %s err: %s", elapsed, host.Hostname, err)
		writer.WriteErrorFetch("system.mutations")
	}
}

func (e *Exporter) collectHostSystemDisksMetrics(
	ctx context.Context,
	host *WatchedHost,
	fetcher *ClickHouseMetricsFetcher,
	writer *CHIPrometheusWriter,
) {
	log.V(1).Infof("Querying disks for host %s", host.Hostname)
	start := time.Now()
	disks, err := fetcher.getClickHouseQuerySystemDisks(ctx)
	elapsed := time.Now().Sub(start)
	if err == nil {
		log.V(1).Infof("Extracted [%s] %d disks for host %s", elapsed, len(disks), host.Hostname)
		writer.WriteSystemDisks(disks)
		writer.WriteOKFetch("system.disks")
	} else {
		// In case of an error fetching data from clickhouse store CHI name in e.cleanup
		log.Warningf("Error [%s] querying system.disks for host %s err: %s", elapsed, host.Hostname, err)
		writer.WriteErrorFetch("system.disks")
	}
}

func (e *Exporter) collectHostDetachedPartsMetrics(
	ctx context.Context,
	host *WatchedHost,
	fetcher *ClickHouseMetricsFetcher,
	writer *CHIPrometheusWriter,
) {
	log.V(1).Infof("Querying detached parts for host %s", host.Hostname)
	start := time.Now()
	detachedParts, err := fetcher.getClickHouseQueryDetachedParts(ctx)
	elapsed := time.Now().Sub(start)
	if err == nil {
		log.V(1).Infof("Extracted [%s] %d detached parts info for host %s", elapsed, len(detachedParts), host.Hostname)
		writer.WriteDetachedParts(detachedParts)
		writer.WriteOKFetch("system.detached_parts")
	} else {
		// In case of an error fetching data from clickhouse store CHI name in e.cleanup
		log.Warningf("Error [%s] querying system.detached_parts for host %s err: %s", elapsed, host.Hostname, err)
		writer.WriteErrorFetch("system.detached_parts")
	}
}

// getWatchedCHI serves HTTP request to get list of watched CHIs
func (e *Exporter) getWatchedCHI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(e.getWatchedCHIs())
}

// fetchCHI decodes chi from the request
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
func (e *Exporter) DiscoveryWatchedCHIs(kubeClient kube.Interface, chopClient *chopAPI.Clientset) {
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
		// Convenience wrapper
		chi := &list.Items[i]

		if chi.IsStopped() {
			log.V(1).Infof("CHI %s/%s is stopped, skip it", chi.Namespace, chi.Name)
			continue
		}

		if !chi.GetStatus().HasNormalizedCHICompleted() {
			log.V(1).Infof("CHI %s/%s is not completed yet, skip it", chi.Namespace, chi.Name)
			continue
		}

		log.V(1).Infof("CHI %s/%s is completed, add it", chi.Namespace, chi.Name)
		normalizer := chiNormalizer.NewNormalizer(func(namespace, name string) (*core.Secret, error) {
			return kubeClient.CoreV1().Secrets(namespace).Get(context.TODO(), name, controller.NewGetOptions())
		})
		normalized, _ := normalizer.CreateTemplatedCHI(chi, chiNormalizer.NewOptions())

		watchedCHI := NewWatchedCHI(normalized)
		e.updateWatched(watchedCHI)
	}
}
