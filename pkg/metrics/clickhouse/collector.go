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

	"github.com/altinity/clickhouse-operator/pkg/apis/metrics"
)

// Collector collects metrics from a single ClickHouse host
type Collector struct {
	fetcher *MetricsFetcher
	writer  *CHIPrometheusWriter
}

// NewCollector creates a new Collector instance
func NewCollector(fetcher *MetricsFetcher, writer *CHIPrometheusWriter) *Collector {
	return &Collector{
		fetcher: fetcher,
		writer:  writer,
	}
}

// CollectHostMetrics runs all metric collectors for a host in parallel
func (c *Collector) CollectHostMetrics(ctx context.Context, host *metrics.WatchedHost) {
	wg := sync.WaitGroup{}
	wg.Add(6)
	go func() { defer wg.Done(); c.collectSystemMetrics(ctx, host) }()
	go func() { defer wg.Done(); c.collectSystemParts(ctx, host) }()
	go func() { defer wg.Done(); c.collectSystemReplicas(ctx, host) }()
	go func() { defer wg.Done(); c.collectMutations(ctx, host) }()
	go func() { defer wg.Done(); c.collectSystemDisks(ctx, host) }()
	go func() { defer wg.Done(); c.collectDetachedParts(ctx, host) }()
	wg.Wait()
}

func (c *Collector) collectSystemMetrics(ctx context.Context, host *metrics.WatchedHost) {
	log.V(1).Infof("Querying system metrics for host %s", host.Hostname)
	start := time.Now()
	metrics, err := c.fetcher.getClickHouseQueryMetrics(ctx)
	elapsed := time.Since(start)
	if err == nil {
		log.V(1).Infof("Extracted [%s] %d system metrics for host %s", elapsed, len(metrics), host.Hostname)
		c.writer.WriteMetrics(metrics)
		c.writer.WriteOKFetch("system_metrics")
	} else {
		log.Warningf("Error [%s] querying system.metrics for host %s err: %s", elapsed, host.Hostname, err)
		c.writer.WriteErrorFetch("system_metrics")
	}
}

func (c *Collector) collectSystemParts(ctx context.Context, host *metrics.WatchedHost) {
	log.V(1).Infof("Querying table sizes for host %s", host.Hostname)
	start := time.Now()
	systemPartsData, err := c.fetcher.getClickHouseSystemParts(ctx)
	elapsed := time.Since(start)
	if err == nil {
		log.V(1).Infof("Extracted [%s] %d table sizes for host %s", elapsed, len(systemPartsData), host.Hostname)
		c.writer.WriteTableSizes(systemPartsData)
		c.writer.WriteOKFetch("table_sizes")
		c.writer.WriteSystemParts(systemPartsData)
		c.writer.WriteOKFetch("system_parts")
	} else {
		log.Warningf("Error [%s] querying system.parts for host %s err: %s", elapsed, host.Hostname, err)
		c.writer.WriteErrorFetch("table_sizes")
		c.writer.WriteErrorFetch("system_parts")
	}
}

func (c *Collector) collectSystemReplicas(ctx context.Context, host *metrics.WatchedHost) {
	log.V(1).Infof("Querying system replicas for host %s", host.Hostname)
	start := time.Now()
	systemReplicas, err := c.fetcher.getClickHouseQuerySystemReplicas(ctx)
	elapsed := time.Since(start)
	if err == nil {
		log.V(1).Infof("Extracted [%s] %d system replicas for host %s", elapsed, len(systemReplicas), host.Hostname)
		c.writer.WriteSystemReplicas(systemReplicas)
		c.writer.WriteOKFetch("system_replicas")
	} else {
		log.Warningf("Error [%s] querying system.replicas for host %s err: %s", elapsed, host.Hostname, err)
		c.writer.WriteErrorFetch("system_replicas")
	}
}

func (c *Collector) collectMutations(ctx context.Context, host *metrics.WatchedHost) {
	log.V(1).Infof("Querying mutations for host %s", host.Hostname)
	start := time.Now()
	mutations, err := c.fetcher.getClickHouseQueryMutations(ctx)
	elapsed := time.Since(start)
	if err == nil {
		log.V(1).Infof("Extracted [%s] %d mutations for %s", elapsed, len(mutations), host.Hostname)
		c.writer.WriteMutations(mutations)
		c.writer.WriteOKFetch("system_mutations")
	} else {
		log.Warningf("Error [%s] querying system.mutations for host %s err: %s", elapsed, host.Hostname, err)
		c.writer.WriteErrorFetch("system_mutations")
	}
}

func (c *Collector) collectSystemDisks(ctx context.Context, host *metrics.WatchedHost) {
	log.V(1).Infof("Querying disks for host %s", host.Hostname)
	start := time.Now()
	disks, err := c.fetcher.getClickHouseQuerySystemDisks(ctx)
	elapsed := time.Since(start)
	if err == nil {
		log.V(1).Infof("Extracted [%s] %d disks for host %s", elapsed, len(disks), host.Hostname)
		c.writer.WriteSystemDisks(disks)
		c.writer.WriteOKFetch("system_disks")
	} else {
		log.Warningf("Error [%s] querying system.disks for host %s err: %s", elapsed, host.Hostname, err)
		c.writer.WriteErrorFetch("system_disks")
	}
}

func (c *Collector) collectDetachedParts(ctx context.Context, host *metrics.WatchedHost) {
	log.V(1).Infof("Querying detached parts for host %s", host.Hostname)
	start := time.Now()
	detachedParts, err := c.fetcher.getClickHouseQueryDetachedParts(ctx)
	elapsed := time.Since(start)
	if err == nil {
		log.V(1).Infof("Extracted [%s] %d detached parts info for host %s", elapsed, len(detachedParts), host.Hostname)
		c.writer.WriteDetachedParts(detachedParts)
		c.writer.WriteOKFetch("system_detached_parts")
	} else {
		log.Warningf("Error [%s] querying system.detached_parts for host %s err: %s", elapsed, host.Hostname, err)
		c.writer.WriteErrorFetch("system_detached_parts")
	}
}
