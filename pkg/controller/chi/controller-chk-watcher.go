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

package chi

import (
	"context"
	"time"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	cmd_queue "github.com/altinity/clickhouse-operator/pkg/controller/chi/cmd_queue"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi/metrics"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
)

var chkGVR = schema.GroupVersionResource{
	Group:    "clickhouse-keeper.altinity.com",
	Version:  "v1",
	Resource: "clickhousekeeperinstallations",
}

const (
	chkWatcherResyncPeriod = 60 * time.Second
)

// StartCHKWatcher starts a dynamic informer for CHK resources.
// When a CHK transitions to Completed status, finds dependent CHIs and enqueues reconcile for them.
// Respects the reconcile.coordination.keeper.onKeeperResourceUpdate config setting.
func (c *Controller) StartCHKWatcher(ctx context.Context) {
	if !c.isKeeperWatchEnabled() {
		log.V(1).Info("CHK watcher disabled (reconcile.coordination.keeper.onKeeperResourceUpdate != 'reconcile')")
		return
	}

	if c.dynamicClient == nil {
		log.V(1).Info("CHK watcher disabled: no dynamic client available")
		return
	}

	ns := chop.Config().GetInformerNamespace()
	listWatcher := &cache.ListWatch{
		ListFunc: func(options meta.ListOptions) (runtime.Object, error) {
			return c.dynamicClient.Resource(chkGVR).Namespace(ns).List(ctx, options)
		},
		WatchFunc: func(options meta.ListOptions) (watch.Interface, error) {
			return c.dynamicClient.Resource(chkGVR).Namespace(ns).Watch(ctx, options)
		},
	}

	_, informer := cache.NewInformer(listWatcher, &unstructured.Unstructured{}, chkWatcherResyncPeriod,
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(old, new interface{}) {
				c.onCHKUpdate(old, new)
			},
		},
	)

	log.V(1).Info("Starting CHK watcher for keeper resource changes")
	go informer.Run(ctx.Done())
}

// isKeeperWatchEnabled checks if the CHK watch is configured.
func (c *Controller) isKeeperWatchEnabled() bool {
	policy := chop.Config().Reconcile.Coordination.Keeper.OnKeeperResourceUpdate
	// EqualFoldString: the CRD accepts both "Reconcile" and "reconcile"; the chopconf value
	// is not re-folded after load, so compare case-insensitively here.
	return policy.HasValue() && policy.EqualFoldString(api.KeeperOnResourceUpdateReconcile)
}

// onCHKUpdate handles CHK update events. Triggers CHI reconcile only when CHK transitions to Completed.
func (c *Controller) onCHKUpdate(old, new interface{}) {
	newObj, ok := new.(*unstructured.Unstructured)
	if !ok {
		return
	}
	oldObj, ok := old.(*unstructured.Unstructured)
	if !ok {
		return
	}

	newStatus := getUnstructuredStatus(newObj)
	oldStatus := getUnstructuredStatus(oldObj)

	// Only react when CHK transitions TO Completed (not on every update while already Completed)
	if newStatus != "Completed" || oldStatus == "Completed" {
		return
	}

	chkName := newObj.GetName()
	chkNamespace := newObj.GetNamespace()
	log.V(1).Info("CHK %s/%s reached Completed — looking for dependent CHIs", chkNamespace, chkName)

	c.enqueueDependentCHIs(chkNamespace, chkName)
}

// getUnstructuredStatus extracts .status.status from an unstructured object.
func getUnstructuredStatus(obj *unstructured.Unstructured) string {
	status, found, err := unstructured.NestedString(obj.Object, "status", "status")
	if err != nil || !found {
		return ""
	}
	return status
}

// enqueueDependentCHIs finds all CHIs (across all namespaces the operator watches) that
// reference the given CHK and enqueues a reconcile for each one — but only if
// shouldReconcileOnKeeperUpdate() confirms the resolved zookeeper endpoint list has
// actually changed. CHK changes that don't affect the endpoint list (e.g. disk resize)
// are skipped.
//
// Cross-namespace note: a CHI can reference a CHK in a different namespace via
// `spec.configuration.zookeeper.keeper.namespace`. Listing only the CHK's own namespace
// would miss such CHIs, so we list across the operator's watch scope (same namespace the
// CHK informer runs in — cluster-wide for NamespaceAll, or a single namespace otherwise).
func (c *Controller) enqueueDependentCHIs(chkNamespace, chkName string) {
	// Bound the worst-case under API-server slowness. The watcher is informer-
	// driven and has no shutdown context plumbed through, so we cap here rather
	// than blocking the informer goroutine indefinitely on a hung List/Get.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// We'll fetch all CHIs in the namespace the operator watches with the informer.
	// The informer may watch namespaces that are not watched by the operator - regexp may play this trick.
	// So we need to fetch all CHIs, even those that may be located in namespaces not watched by the operator.
	// Filtered below via IsNamespaceWatched before enqueueing.
	watchNs := chop.Config().GetInformerNamespace()
	chiList, err := c.chopClient.ClickhouseV1().ClickHouseInstallations(watchNs).List(
		ctx,
		meta.ListOptions{},
	)
	if err != nil {
		log.V(1).Info("Failed to list CHIs (watch ns=%q) for CHK %s/%s: %v", watchNs, chkNamespace, chkName, err)
		return
	}

	// Iterate over all CHIs and filter out those that do not reference the given CHK.
	for i := range chiList.Items {
		chi := &chiList.Items[i]

		// Filter out CHIs in namespaces the operator doesn't actually watch.
		// The List above may surface namespaces beyond the watch scope when
		// the watch is regexp-driven; the normal informer path applies
		// IsNamespaceWatched at ShouldEnqueue time, and we must mirror that.
		if !chop.Config().IsNamespaceWatched(chi.Namespace) {
			continue
		}

		// First step - check if the CHI references the given CHK.
		if !chiReferencesKeeper(chi, chkName, chkNamespace) {
			continue
		}

		// Re-fetch via kube CR interface so Status.NormalizedCRCompleted (stored in a side
		// ConfigMap, not on the CR status subresource) is merged in — shouldReconcileOnKeeperUpdate
		// needs the completed spec to diff against.
		fullChi := chi
		if c.kube != nil {
			if got, err := c.kube.CR().Get(ctx, chi.Namespace, chi.Name); err == nil {
				if typed, ok := got.(*api.ClickHouseInstallation); ok && typed != nil {
					fullChi = typed
				}
			}
		}

		// Second step - check if the CHI needs to be reconciled.
		// Not all CHK changes require a CHI reconcile.
		// For example, if CHK has disk size change or some other change that does not affect
		// the list of the resolved zookeeper endpoints, we don't need to reconcile the CHI.
		if !c.shouldReconcileOnKeeperUpdate(ctx, fullChi, chkName, chkNamespace) {
			c.recordCHIReconcileSkipped(ctx, fullChi, chkNamespace, chkName)
			continue
		}

		// Third step - enqueue the CHI for reconciliation.

		log.V(1).Info("Triggering reconcile for CHI %s/%s due to CHK %s/%s completing",
			chi.Namespace, chi.Name, chkNamespace, chkName)
		// Use ReconcileAdd (not ReconcileUpdate): prepareCHIUpdate computes an old-vs-new
		// ActionPlan diff and drops the command if nothing changed. With (old=new=chi),
		// there is no diff, so the update would be silently rejected. ReconcileAdd always
		// enqueues (prepareCHIAdd returns true unconditionally), and the downstream reconcile
		// path still re-fetches the CR and computes a fresh diff against its ancestor — the
		// proper reference for detecting "CHK endpoints changed, CHI normalized spec must
		// update".
		c.enqueueObject(cmd_queue.NewReconcileCHI(cmd_queue.ReconcileAdd, nil, chi))
	}
}

// shouldReconcileOnKeeperUpdate decides whether a CHK completion event should trigger a CHI
// reconcile. Returns true only if the CHK's current state would produce a zookeeper endpoint
// list that differs from what the CHI consumed in its last completed reconcile.
//
// Conservative fallbacks — return true (enqueue anyway) when:
//   - CHI has no completed state yet (first reconcile path)
//   - Resolver errors on the current CHK state (let reconcile surface the problem properly)
func (c *Controller) shouldReconcileOnKeeperUpdate(ctx context.Context, cr *api.ClickHouseInstallation, chkName, chkNamespace string) bool {
	// No completed CHI yet — cannot diff, must enqueue
	completedChi := cr.GetAncestorT()
	if completedChi == nil || completedChi.Spec.GetConfiguration() == nil {
		log.V(1).M(cr).Info("No NormalizedCRCompleted for CHI %s/%s — cannot diff, enqueueing", cr.Namespace, cr.Name)
		return true
	}

	domainPattern := cr.Spec.GetNamespaceDomainPattern()

	// Top-level keeper ref
	topKeeper := cr.Spec.GetConfiguration().GetZookeeper().GetKeeper()
	if matchesKeeperRef(topKeeper, cr.Namespace, chkName, chkNamespace) {
		// CHI points to the same CHK as the one that completed.
		// Check if the resolved zookeeper endpoints list has changed.
		completedNodes := completedChi.Spec.GetConfiguration().GetZookeeper().GetNodes()
		if changed, err := c.keeperNodeListChanged(ctx, topKeeper, cr.Namespace, domainPattern, completedNodes); err != nil || changed {
			return true
		}
	}

	// Per-cluster keeper refs.
	//
	// We match each current cluster to its completed counterpart by cluster NAME, not by
	// position. Positional matching would mis-compare if the user re-ordered clusters in
	// the spec between reconciles (same length, different order → name at index i in current
	// points to a different cluster than in the completed spec). Name-based matching is
	// robust to reordering and still trivially handles newly-added clusters (no completed
	// entry → empty ZookeeperNodes → set-equality returns changed=true → conservatively
	// enqueue).
	completedNodesByClusterName := buildCompletedNodesByClusterName(completedChi.Spec.GetConfiguration().GetClusters())
	for _, cluster := range cr.Spec.GetConfiguration().GetClusters() {
		clusterKeeper := cluster.GetZookeeper().GetKeeper()
		if !matchesKeeperRef(clusterKeeper, cr.Namespace, chkName, chkNamespace) {
			continue
		}

		// completedNodesByClusterName returns the zero value (nil) for unknown names —
		// that's the correct behaviour for newly-added clusters with no completed entry yet.
		completedNodes := completedNodesByClusterName[cluster.GetName()]
		if changed, err := c.keeperNodeListChanged(ctx, clusterKeeper, cr.Namespace, domainPattern, completedNodes); err != nil || changed {
			return true
		}
	}

	return false
}

// buildCompletedNodesByClusterName indexes a last-completed cluster list by cluster name for
// fast per-name lookup. Empty-named clusters (shouldn't happen in a normalized completed
// spec) are skipped. On duplicate names (also shouldn't happen), last-writer wins — harmless
// for the set-equality comparison done by shouldReconcileOnKeeperUpdate.
func buildCompletedNodesByClusterName(clusters []*api.Cluster) map[string]api.ZookeeperNodes {
	index := make(map[string]api.ZookeeperNodes, len(clusters))
	for _, cluster := range clusters {
		name := cluster.GetName()
		if name == "" {
			continue
		}
		index[name] = cluster.GetZookeeper().GetNodes()
	}
	return index
}

// recordCHIReconcileSkipped records observability when the CHK completion does not warrant
// a CHI reconcile (resolved zookeeper endpoints unchanged): bumps the metrics counter and
// emits a k8s event on the CHI so operators can see in `kubectl describe chi` why no
// reconcile was triggered.
func (c *Controller) recordCHIReconcileSkipped(ctx context.Context, cr *api.ClickHouseInstallation, chkNamespace, chkName string) {
	metrics.CRKeeperUpdatesSkipped(ctx, cr)

	message := "CHK " + chkNamespace + "/" + chkName + " reconcile completed; resolved zookeeper endpoints unchanged — skipping CHI reconcile"
	log.V(1).M(cr).Info(message)

	if c.kube == nil {
		return
	}
	emitter := a.NewEventEmitter(c.kube.Event(), "ClickHouseInstallation", "chop-chi-", componentName)
	emitter.EventInfo(cr, a.EventActionReconcile, a.EventReasonKeeperUpdateNoEndpointChange, message)
}

// matchesKeeperRef reports whether the given KeeperRef points to the specified CHK,
// using the CHI's own namespace as the default when the ref does not specify one.
func matchesKeeperRef(ref *api.KeeperRef, chiNamespace, chkName, chkNamespace string) bool {
	return ref.HasName() &&
		(ref.Name == chkName) &&
		(ref.GetNamespace(chiNamespace) == chkNamespace)
}

// keeperNodeListChanged resolves the given keeper ref against the current CHK state and
// compares the resulting node set against the nodes from the CHI's last completed reconcile.
// Returns (changed, err). changed=false means the two sets are identical (safe to skip CHI
// reconcile).
func (c *Controller) keeperNodeListChanged(
	ctx context.Context,
	keeper *api.KeeperRef,
	chiNamespace string,
	domainPattern *types.String,
	completedNodes api.ZookeeperNodes,
) (bool, error) {
	newNodes, err := c.resolveKeeperNodes(ctx, keeper, chiNamespace, domainPattern)
	if err != nil {
		// Conservative: let the normal reconcile path deal with resolver errors
		log.V(1).Info("Keeper resolver error — treating as change: %v", err)
		return true, err
	}

	// If the current CHK scope produced nodes that differ from what the CHI last consumed,
	// a reconcile is required. Use set equality — order of per-replica service discovery
	// may differ between calls, but the endpoint set itself is what drives the CH config.
	changed := !newNodes.Equals(completedNodes)

	log.V(2).Info("Keeper node list diff: keeper=%s/%s completed=[%s] current=[%s] changed=%t",
		keeper.GetNamespace(chiNamespace), keeper.Name, completedNodes, newNodes, changed)

	return changed, nil
}

// chiReferencesKeeper checks if a CHI references the given CHK by name.
// Looks at both top-level and per-cluster zookeeper.keeper refs.
func chiReferencesKeeper(chi *api.ClickHouseInstallation, chkName, chkNamespace string) bool {
	// Top-level zookeeper.keeper
	if matchesKeeperRef(chi.Spec.GetConfiguration().GetZookeeper().GetKeeper(), chi.Namespace, chkName, chkNamespace) {
		return true
	}
	// Per-cluster zookeeper.keeper
	for _, cluster := range chi.Spec.GetConfiguration().GetClusters() {
		if matchesKeeperRef(cluster.GetZookeeper().GetKeeper(), chi.Namespace, chkName, chkNamespace) {
			return true
		}
	}
	return false
}
