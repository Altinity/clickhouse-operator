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

// Worker-side keeper-ref resolution.
//
// This file holds methods on *worker that drive the reconcile-time keeper-ref resolution:
// waiting for the referenced CHK to be ready, then invoking the pure Controller-level
// resolver and appending the resolved endpoints into the CHI's ZookeeperConfig.
//
// Pure Controller-level resolvers live in controller-keeper-resolver.go.

package chi

import (
	"context"
	"fmt"
	"time"

	core "k8s.io/api/core/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/poller"
)

// resolveAllKeeperReferences resolves all keeper references in the CHI spec to ZooKeeper nodes.
// Must be called before normalization.
//
// The nodes are resolved into a copy of the CHI, which is returned - also along with an error, holding
// whatever has been resolved by then. The CHI given stays as it is, so it can be built from again without
// its nodes piling up.
//
// Each ZookeeperConfig level is resolved independently — resolved nodes are appended to
// that level's own Nodes slice, not to a shared one:
//   - Top-level spec.configuration.zookeeper.keeper → resolved into spec.configuration.zookeeper.nodes
//   - Cluster-level clusters[i].zookeeper.keeper    → resolved into clusters[i].zookeeper.nodes
//
// Inheritance between levels is NOT handled here — that happens later in the normalizer
// via InheritZookeeperFrom (type_cluster.go). The inheritance rule is either/or:
//   - If a cluster has NO zookeeper config (empty) → it inherits the top-level config entirely
//   - If a cluster has ANY zookeeper config (own keeper ref or own nodes) → top-level is ignored
//     for that cluster (InheritZookeeperFrom returns early when cluster.Zookeeper is non-empty)
func (w *worker) resolveAllKeeperReferences(ctx context.Context, cr *api.ClickHouseInstallation) (*api.ClickHouseInstallation, error) {
	cr = cr.DeepCopy()

	// Pass CR's namespace domain pattern to the resolver.
	// resolveKeeperReference is nil-safe (HasKeeper handles nil ZookeeperConfig),
	// so we can pass the chained getter output directly without a pre-check.
	domainPattern := cr.Spec.GetNamespaceDomainPattern()

	// Resolve top-level zookeeper keeper ref into top-level nodes
	if err := w.resolveKeeperReference(ctx, cr.GetNamespace(), domainPattern, cr.Spec.GetConfiguration().GetZookeeper()); err != nil {
		return cr, err
	}

	// Resolve each cluster's zookeeper keeper ref into that cluster's own nodes
	for _, cluster := range cr.Spec.GetConfiguration().GetClusters() {
		if err := w.resolveKeeperReference(ctx, cr.GetNamespace(), domainPattern, cluster.GetZookeeper()); err != nil {
			return cr, err
		}
	}

	return cr, nil
}

// resolveKeeperReference resolves a single KeeperRef into ZookeeperNodes within the given
// ZookeeperConfig. Waits for the CHK to be ready before resolving, then appends the resolved
// nodes to the ZookeeperConfig's Nodes slice.
func (w *worker) resolveKeeperReference(ctx context.Context, chiNamespace string, domainPattern *types.String, zkc *api.ZookeeperConfig) error {
	// Sanity check: if no keeper reference, nothing to do
	if !zkc.HasKeeper() {
		return nil
	}

	keeper := zkc.Keeper
	ns := keeper.GetNamespace(chiNamespace)
	name := keeper.Name

	// Wait until the referenced keeper (CHK) is ready before resolving it to zookeeper nodes
	if err := w.waitKeeperReady(ctx, ns, name); err != nil {
		return err
	}

	nodes, err := w.c.resolveKeeperNodes(ctx, keeper, chiNamespace, domainPattern)
	if err != nil {
		return err
	}

	// Looks good, append resolved nodes to the CHI's ZookeeperConfig
	log.V(1).Info("Resolved keeper ref %s/%s to %d node(s): %s", ns, name, len(nodes), nodes)

	zkc.Nodes = zkc.Nodes.Append(nodes...)
	return nil
}

const (
	// keeperReadyPollInterval is how often to re-check CHK pod readiness.
	keeperReadyPollInterval = 5 * time.Second
)

// waitKeeperReady blocks until the referenced CHK has all pods in Running phase.
// Uses the project's standard poller infrastructure.
// Timeout is configurable via CHOP config: clickhouse.keeperReadyTimeout (seconds).
// Returns ErrKeeperNotReady if the timeout expires.
func (w *worker) waitKeeperReady(ctx context.Context, namespace, name string) error {
	opts := chkListOptions(name, namespace)

	timeout := time.Duration(chop.Config().Reconcile.Coordination.Keeper.ReadyTimeout) * time.Second

	pollerName := fmt.Sprintf("keeper-ready/%s/%s", namespace, name)
	if err := poller.New(ctx, pollerName).
		WithOptions(&poller.Options{
			Timeout:                    timeout,
			MainInterval:               keeperReadyPollInterval,
			StartBotheringAfterTimeout: 30 * time.Second,
		}).
		WithFunctions(&poller.Functions{
			Get: func(ctx context.Context) (any, error) {
				return w.c.kubeClient.CoreV1().Pods(namespace).List(ctx, opts)
			},
			IsDone: func(_ context.Context, item any) bool {
				pods, ok := item.(*core.PodList)
				if !ok || len(pods.Items) == 0 {
					return false
				}
				for _, pod := range pods.Items {
					if pod.Status.Phase != core.PodRunning {
						return false
					}
				}
				return true
			},
			ShouldContinueOnGetError: func(_ context.Context, _ any, _ error) bool {
				return true
			},
		}).
		Poll(); err != nil {
		return fmt.Errorf("%w %s/%s: %w", ErrKeeperNotReady, namespace, name, err)
	}
	return nil
}
