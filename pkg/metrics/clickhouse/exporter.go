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

package clickhouse

import (
	"context"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"

	core "k8s.io/api/core/v1"
	kube "k8s.io/client-go/kubernetes"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/apis/metrics"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	chopAPI "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/metrics/clickhouse/filters"
	chiNormalizer "github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"
	normalizerCommon "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
)

// Exporter implements prometheus.Collector interface
type Exporter struct {
	collectorTimeout time.Duration
	registry         *CRRegistry
	metricsFilter    MetricsFilter
}

// Type compatibility
var _ prometheus.Collector = &Exporter{}
var _ MetricsFilter = &filters.Regexp{}

// NewExporter returns a new instance of Exporter type
func NewExporter(registry *CRRegistry, collectorTimeout time.Duration) *Exporter {
	excludeRegexps := chop.Config().ClickHouse.Metrics.ExcludeRegexp
	// Make active exclusions discoverable in operator logs so users hitting silent
	// metric loss (e.g. CPU/OS series filtered by the default pattern) can find the
	// cause via `kubectl logs metrics-exporter`.
	if len(excludeRegexps) == 0 {
		log.Info("Metrics exporter: no exclusion regexps configured")
	} else {
		log.Infof("Metrics exporter: excluding metric names matching %v", excludeRegexps)
	}
	return &Exporter{
		registry:         registry,
		collectorTimeout: collectorTimeout,
		metricsFilter:    filters.NewRegexp(excludeRegexps),
	}
}

// Collect implements prometheus.Collector Collect method
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	if ch == nil {
		log.Warning("Prometheus channel is closed. Unable to write metrics")
		return
	}

	start := time.Now()

	log.V(1).Info("Collect started")
	defer func() {
		log.V(1).Infof("Collect completed [%s]", time.Since(start))
	}()

	// Collection process should have limited duration
	ctx, cancel := context.WithTimeout(context.Background(), e.collectorTimeout)
	defer cancel()

	log.V(1).Infof("Launching host collectors [%s]", time.Since(start))

	var wg = sync.WaitGroup{}
	e.registry.Walk(func(cr *metrics.WatchedCR, _ *metrics.WatchedCluster, host *metrics.WatchedHost) {
		wg.Add(1)
		go func(ctx context.Context, cr *metrics.WatchedCR, host *metrics.WatchedHost, ch chan<- prometheus.Metric) {
			defer wg.Done()
			e.collectHostMetrics(ctx, cr, host, ch)
		}(ctx, cr, host, ch)
	})
	wg.Wait()
}

// Describe implements prometheus.Collector Describe method
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(e, ch)
}

// collectHostMetrics collects metrics from one host and writes them into chan
func (e *Exporter) collectHostMetrics(ctx context.Context, chi *metrics.WatchedCR, host *metrics.WatchedHost, c chan<- prometheus.Metric) {
	collector := NewCollector(
		e.newHostFetcher(host),
		NewCHIPrometheusWriter(c, chi, host, e.metricsFilter),
	)
	collector.CollectHostMetrics(ctx, host)
}

// newHostFetcher returns new Metrics Fetcher for specified host
func (e *Exporter) newHostFetcher(host *metrics.WatchedHost) *MetricsFetcher {
	// Make base cluster connection params
	clusterConnectionParams := clickhouse.NewClusterConnectionParamsFromCHOpConfig(chop.Config())
	// Adjust base cluster connection params with per-host props
	switch clusterConnectionParams.Scheme {
	case api.ChSchemeAuto:
		switch {
		case types.IsPortAssigned(host.HTTPPort):
			clusterConnectionParams.Scheme = "http"
			clusterConnectionParams.Port = int(host.HTTPPort)
		case types.IsPortAssigned(host.HTTPSPort):
			clusterConnectionParams.Scheme = "https"
			clusterConnectionParams.Port = int(host.HTTPSPort)
		}
	case api.ChSchemeHTTP:
		clusterConnectionParams.Port = int(host.HTTPPort)
	case api.ChSchemeHTTPS:
		clusterConnectionParams.Port = int(host.HTTPSPort)
	}

	return NewMetricsFetcher(
		clusterConnectionParams.NewEndpointConnectionParams(host.Hostname),
		chop.Config().ClickHouse.Metrics.TablesRegexp,
		e.metricsFilter,
	)
}

// DiscoveryWatchedCHIs discovers all ClickHouseInstallation objects available for monitoring and adds them to watched list
func (e *Exporter) DiscoveryWatchedCHIs(kubeClient kube.Interface, chopClient *chopAPI.Clientset) {
	// Get all CHI objects from watched namespace(s)
	watchedNamespace := chop.Config().GetInformerNamespace()
	list, err := chopClient.ClickhouseV1().ClickHouseInstallations(watchedNamespace).List(context.TODO(), controller.NewListOptions())
	if err != nil {
		log.V(1).Infof("Error read ClickHouseInstallations %v", err)
		return
	}
	if list == nil {
		return
	}

	// Walk over the list of ClickHouseInstallation objects and add them as watched
	for i := range list.Items {
		e.processDiscoveredCR(kubeClient, &list.Items[i])
	}
}

func (e *Exporter) processDiscoveredCR(kubeClient kube.Interface, chi *api.ClickHouseInstallation) {
	if !e.shouldWatchCR(chi) {
		log.V(1).Infof("Skip discovered CHI: %s/%s", chi.Namespace, chi.Name)
		return
	}

	log.V(1).Infof("Add discovered CHI: %s/%s", chi.Namespace, chi.Name)
	normalizer := chiNormalizer.New(func(namespace, name string) (*core.Secret, error) {
		return kubeClient.CoreV1().Secrets(namespace).Get(context.TODO(), name, controller.NewGetOptions())
	})

	normalized, _ := normalizer.CreateTemplated(chi, normalizerCommon.NewOptions[api.ClickHouseInstallation]())

	watchedCR := metrics.NewWatchedCR(normalized)
	e.registry.AddCR(watchedCR)
}

func (e *Exporter) shouldWatchCR(chi *api.ClickHouseInstallation) bool {
	if chi.IsStopped() {
		log.V(1).Infof("CHI %s/%s is stopped, unable to watch it", chi.Namespace, chi.Name)
		return false
	}

	return true
}
