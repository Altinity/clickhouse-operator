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
	"errors"
	"fmt"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/model"
	chiLabeler "github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	commonLabeler "github.com/altinity/clickhouse-operator/pkg/model/common/tags/labeler"
)

var (
	// errClusterHasNoHostsForHook is returned when a cluster-scoped hook has no host to run on.
	errClusterHasNoHostsForHook = errors.New("no hosts for hook execution")
)

// runClusterPreHooks executes cluster-level pre-reconcile hooks.
// Returns error on failure — caller should abort the cluster reconcile.
//
// Skipped when there's nothing live to talk to:
//   - first cluster creation (no ancestor hosts → no pods exist yet)
//   - stopped cluster (CR-level stop → ancestor pods already torn down)
//
// Both states are "no work to do", not errors.
func (w *worker) runClusterPreHooks(ctx context.Context, cluster *api.Cluster) error {
	// Do we have any hooks to run?
	hooks := cluster.GetReconcile().GetHooks()
	if hooks.IsEmpty() {
		return nil
	}

	// Do we have any host to run hooks on?
	if !cluster.HasRunningHosts() {
		w.a.V(1).M(cluster).F().Info("Skipping cluster pre-hooks: no live hosts yet (first creation)")
		return nil
	}

	// Is the cluster stopped? Pre-reconcile state for a stopped CR has no live pods.
	if cluster.IsStopped() {
		w.a.V(1).M(cluster).F().Info("Skipping cluster pre-hooks: cluster is stopped (no live hosts)")
		return nil
	}

	// Run hooks on the cluster.
	return w.runClusterHookActions(ctx, hooks.GetPre(), cluster, w.firedClusterEvents(cluster))
}

// runClusterPostHooks executes cluster-level post-reconcile hooks.
// Errors are logged but do not fail the reconcile.
//
// Skipped on a stopped cluster: post-hooks run AFTER reconcile completes, so on a
// stopped CR the pods are already gone — there's no live host to execute SQL against.
// This is "no work to do", not an error.
func (w *worker) runClusterPostHooks(ctx context.Context, cluster *api.Cluster) {
	// Do we have any hooks to run?
	hooks := cluster.GetReconcile().GetHooks()
	if hooks.IsEmpty() {
		return
	}

	// Is the cluster stopped? Post-reconcile state has no live pods.
	if cluster.IsStopped() {
		w.a.V(1).M(cluster).F().Info("Skipping cluster post-hooks: cluster is stopped (no live hosts)")
		return
	}

	// Cluster looks like it's running, so run hooks on the cluster.
	events := w.firedClusterEvents(cluster)
	if err := w.runClusterHookActions(ctx, hooks.GetPost(), cluster, events); err != nil {
		w.a.V(1).M(cluster).F().
			WithEvent(cluster.GetRuntime().GetCR(), a.EventActionReconcile, a.EventReasonReconcileFailed).
			Warning("Cluster post-hook failed: %v", err)
	}
}

// runHostPreHooks executes host-level pre-reconcile hooks.
// Returns error on failure — caller should abort the host reconcile.
//
// Skipped when the host's pod is not reachable: covers first-creation (no pod yet),
// stopped state (pod gone), crashed/not-ready pods. Uses isHostReachableForHook —
// same live check as the pre-delete path — so the four hook entry points share one
// authoritative reachability gate.
func (w *worker) runHostPreHooks(ctx context.Context, host *api.Host) error {
	// Do we have any hooks to run?
	hooks := host.GetCluster().GetReconcile().Host.GetHooks()
	if hooks.IsEmpty() {
		return nil
	}

	// Is the host's pod alive right now? (pre-hook timing = ancestor state)
	if !w.isHostReachableForHook(ctx, host) {
		w.a.V(1).M(host).F().Info("Skipping host pre-hooks: host is not reachable. Host: %s", host.GetName())
		return nil
	}

	// Run hooks on the host.
	return w.runHostHookActions(ctx, hooks.GetPre(), host, w.firedHostEvents(ctx, host))
}

// runHostPostHooks executes host-level post-reconcile hooks.
// Errors are logged but do not fail the reconcile.
//
// Skipped when the host's pod is not reachable: covers stopped state (pod gone after
// reconcile), crashed pods, or hosts that failed to come up. Uses isHostReachableForHook
// — same live check as the pre-delete path.
func (w *worker) runHostPostHooks(ctx context.Context, host *api.Host) {
	// Do we have any hooks to run?
	hooks := host.GetCluster().GetReconcile().Host.GetHooks()
	if hooks.IsEmpty() {
		return
	}

	// Is the host's pod alive right now? (post-hook timing = current state)
	if !w.isHostReachableForHook(ctx, host) {
		w.a.V(1).M(host).F().Info("Skipping host post-hooks: host is not reachable. Host: %s", host.GetName())
		return
	}

	events := w.firedHostEvents(ctx, host)
	if err := w.runHostHookActions(ctx, hooks.GetPost(), host, events); err != nil {
		w.a.V(1).M(host).F().
			WithEvent(host.GetCR(), a.EventActionReconcile, a.EventReasonReconcileFailed).
			Warning("Host post-hook failed: %v", err)
	}
}

// runClusterHookActions iterates over cluster-level hook actions, filters by event match,
// and executes the matching ones sequentially. Empty fired-event set means nothing fires.
// Action errors are propagated unless the action sets failurePolicy: Ignore.
func (w *worker) runClusterHookActions(ctx context.Context, actions []*api.HookAction, cluster *api.Cluster, events []api.HookEvent) error {
	// Run all hook actions on the cluster
	for _, action := range actions {
		// Does the action is triggered by any of the fired events?
		if !action.MatchesAnyEvent(events) {
			w.a.V(2).M(cluster).F().Info("Cluster hook skipped: events=%v fired=%v", action.Events, events)
			continue
		}

		// Yes, the action is triggered by one of the fired events, so run it.
		err := w.dispatchClusterHookAction(ctx, action, cluster)
		switch {
		case err == nil:
			continue
		case action.ShouldIgnoreFailure():
			w.a.V(1).M(cluster).F().Warning("Cluster hook failed (failurePolicy=Ignore, continuing): %v", err)
			continue
		default:
			return err
		}
	}

	// All hook actions on the cluster completed successfully.
	return nil
}

// runHostHookActions iterates over host-level hook actions, filters by event match,
// and executes the matching ones sequentially. Empty fired-event set means nothing fires.
// Action errors are propagated unless the action sets failurePolicy: Ignore.
func (w *worker) runHostHookActions(ctx context.Context, actions []*api.HookAction, host *api.Host, events []api.HookEvent) error {
	// Run all hook actions on the host
	for _, action := range actions {
		// Does the action is triggered by any of the fired events?
		if !action.MatchesAnyEvent(events) {
			w.a.V(2).M(host).F().Info("Host hook skipped: events=%v fired=%v", action.Events, events)
			continue
		}

		// Yes, the action is triggered by one of the fired events, so run it.
		err := w.dispatchHostHookAction(ctx, action, host)
		switch {
		case err == nil:
			continue
		case action.ShouldIgnoreFailure():
			w.a.V(1).M(host).F().Warning("Host hook failed (failurePolicy=Ignore, continuing): %v", err)
			continue
		default:
			return err
		}
	}

	// All hook actions on the host completed successfully.
	return nil
}

// dispatchClusterHookAction routes a cluster-level action to its concrete handler.
func (w *worker) dispatchClusterHookAction(ctx context.Context, action *api.HookAction, cluster *api.Cluster) error {
	switch {
	case action.HasSQL():
		return w.runClusterSQLHookAction(ctx, action, cluster)
	case action.HasShell():
		return fmt.Errorf("shell hooks not yet implemented")
	case action.HasHTTP():
		return fmt.Errorf("http hooks not yet implemented")
	default:
		w.a.V(1).F().Info("Empty cluster action specified")
		return nil
	}
}

// dispatchHostHookAction routes a host-level action to its concrete handler.
func (w *worker) dispatchHostHookAction(ctx context.Context, action *api.HookAction, host *api.Host) error {
	switch {
	case action.HasSQL():
		return w.runHostSQLHookAction(ctx, action, host)
	case action.HasShell():
		return fmt.Errorf("shell hooks not yet implemented")
	case action.HasHTTP():
		return fmt.Errorf("http hooks not yet implemented")
	default:
		w.a.V(1).F().Info("Empty host action specified")
		return nil
	}
}

// runHostPreDeleteHooks executes host-level PRE hooks against a host that is about to be
// torn down. There is no post-delete host-level hook — the host's pod is gone after deletion,
// so a host-scope post-hook has nothing to run on. (For cluster-wide cleanup after a host
// is gone, use a cluster-level post-hook on a separate reconcile if needed.)
//
// The host's pod is still up at this call site, so SQL hooks execute against it directly
// via the schemer.
//
// Failure semantics: per-action `failurePolicy` field controls whether errors propagate
// (Fail, default) or are logged-and-ignored (Ignore). What the caller does with the
// returned error differs by call site:
//   - deleteHost (full-CHI delete path): returned error aborts host deletion.
//   - runHostPreDeleteHooksOnRemovedHosts (scale-down + orphan path): returned error
//     is logged per-host; iteration over remaining hosts continues.
//
// Reachability: if the host's pod is unhealthy / unreachable, hooks are skipped with a
// warning instead of returning an error. This avoids a stuck dying host blocking the
// reconcile forever — a host you can't talk to can't be drained gracefully anyway. The
// reachability check overrides failurePolicy: an unreachable host always skips the hook
// with a warning, even with failurePolicy=Fail.
//
// Hook config source: host.GetCluster().GetReconcile().Host.Hooks. For deleted hosts the
// cluster reference is the ANCESTOR cluster (host objects come from the action plan's
// removed-hosts walk), so this naturally reads the hooks that were configured when the
// host existed — even if the user removed the hook field in the current spec.
func (w *worker) runHostPreDeleteHooks(ctx context.Context, host *api.Host) error {
	// Do we have any pre-delete hooks to run? (IsEmpty implies len(Pre)==0 too,
	// but we only care about Pre on this path — Post hooks are not invoked
	// pre-deletion since the pod won't exist post-deletion.)
	hooks := host.GetCluster().GetReconcile().Host.GetHooks()
	if len(hooks.GetPre()) == 0 {
		return nil
	}

	if !w.isHostReachableForHook(ctx, host) {
		w.a.V(1).M(host).F().Warning("Skipping pre-delete hooks: host is not reachable. Host: %s", host.GetName())
		return nil
	}

	return w.runHostHookActions(ctx, hooks.GetPre(), host, w.firedHostDeleteEvents(host))
}

// runHostPreDeleteHooksOnRemovedHosts runs pre-delete hooks against every host going
// away in this reconcile, from TWO sources:
//
//  1. ActionPlan.Removed — the diff between completed and current spec. Catches the
//     planned removals (scale-down, shard removal, cluster removal in spec).
//  2. Orphan StatefulSets — k8s objects with our labels that the current reconcile
//     did not touch. clean()/purge() deletes them after this point. Catches
//     leftover artifacts from operator-restart races, failed prior reconciles, or
//     stale revisions where ActionPlan-based diff misses the host.
//
// Both paths converge on runHostPreDeleteHooks, which gates on pod reachability —
// so a torn-down or unhealthy host silently skips. Hosts already covered by path 1
// are skipped in path 2 (dedup by host name) to avoid double-firing.
//
// Path 2 resolves orphan STSes back to *api.Host structs by reading the STS's
// cluster/shard/replica labels and looking them up in NormalizedCRCompleted. STSes
// that don't map to any host in the completed spec are silently passed over —
// there's no Host context to plug user-defined hooks into.
//
// Note: ActionPlan.WalkRemoved fires the most-specific callback for each diff entry —
// a removed shard fires shardFunc once with the shard, NOT hostFunc once per host
// inside the shard. We expand each removed cluster/shard back into its constituent
// hosts via WalkHosts so the per-host pre-delete hook still fires.
//
// Call site: worker-reconciler-chi.go BEFORE clean(). At that point the removed host's
// pod is still up (clean's purge is what tears it down), so SQL hooks can drain the host.
//
// Errors per host are logged but never aborted — failurePolicy on each individual hook
// already controls Fail vs Ignore for its own SQL error, but a hook error on one removed
// host should not stop other removed hosts from being drained nor block the eventual
// purge from running. (A user who needs scale-down to be aborted on hook failure should
// run the validation as a cluster-level pre-hook instead.)
func (w *worker) runHostPreDeleteHooksOnRemovedHosts(ctx context.Context, cr *api.ClickHouseInstallation) {
	if cr == nil {
		return
	}

	// Dedup state — one host receives at most one pre-delete hook execution.
	// Key is composite (cluster/host) because host.GetName() is short ("0-0") and
	// can collide across clusters of the same CHI; cluster-qualified key is unique.
	// If Address.ClusterName is empty (Address not fully populated), fall back to
	// the FQDN-style HostName which is unique even across clusters.
	handled := map[string]bool{}
	hostKey := func(host *api.Host) string {
		if cn := host.Runtime.Address.ClusterName; cn != "" {
			return cn + "/" + host.GetName()
		}
		return host.Runtime.Address.HostName + "/" + host.GetName()
	}

	// Host function to run the pre-delete hooks on a host.
	runHooksOnHost := func(host *api.Host) error {
		if host == nil {
			return nil
		}
		key := hostKey(host)
		if handled[key] {
			// Already handled this host.
			return nil
		}
		// Mark this host as handled.
		handled[key] = true

		// Run the pre-delete hooks on the host.
		if err := w.runHostPreDeleteHooks(ctx, host); err != nil {
			w.a.V(1).M(host).F().Warning("Pre-delete hook failed on host %s: %v", host.GetName(), err)
		}

		return nil
	}

	// Path 1 — ActionPlan-driven removals (the common case).
	cr.EnsureRuntime().ActionPlan.WalkRemoved(
		func(cluster api.ICluster) {
			cluster.WalkHosts(runHooksOnHost)
		},
		func(shard api.IShard) {
			shard.WalkHosts(runHooksOnHost)
		},
		func(host *api.Host) {
			_ = runHooksOnHost(host)
		},
	)

	// Path 2 — orphan StatefulSets caught by clean()/purge().
	w.walkOrphanHostsFromCompleted(ctx, cr, runHooksOnHost)
}

// walkOrphanHostsFromCompleted invokes f on every host in NormalizedCRCompleted whose
// StatefulSet currently exists in k8s but isn't claimed by the current reconcile —
// i.e. the orphans clean()/purge() is about to delete. STSes with missing or
// unrecognized labels are silently skipped (no Host context to act on).
//
// Best-effort: extra discovery round-trip even when the user has no hooks defined.
// Short-circuit at the call site if you want to avoid that cost.
func (w *worker) walkOrphanHostsFromCompleted(
	ctx context.Context,
	cr *api.ClickHouseInstallation,
	f func(host *api.Host) error,
) {
	if cr == nil {
		return
	}
	completed := cr.GetAncestorT()
	if completed == nil {
		return
	}

	orphans := w.c.discovery(ctx, cr)
	orphans.Subtract(w.task.RegistryReconciled())

	labeler := chiLabeler.New(cr)
	orphans.Walk(func(entityType model.EntityType, m meta.Object) {
		if entityType != model.StatefulSet {
			return
		}
		host := findHostInCompletedFromLabels(completed, labeler, m)
		if host != nil {
			_ = f(host)
		}
	})
}

// findHostInCompletedFromLabels resolves a k8s StatefulSet (by its operator labels)
// back to the *api.Host in the previously-completed CR spec. Returns nil if any
// of the cluster/shard/replica labels are missing or don't match any host in
// completed — covering both "labels stripped" and "true orphan" cases.
func findHostInCompletedFromLabels(
	completed *api.ClickHouseInstallation,
	labeler *chiLabeler.Labeler,
	m meta.Object,
) *api.Host {
	labels := m.GetLabels()
	if labels == nil {
		return nil
	}
	clusterName, ok1 := labels[labeler.Get(commonLabeler.LabelClusterName)]
	shardName, ok2 := labels[labeler.Get(commonLabeler.LabelShardName)]
	replicaName, ok3 := labels[labeler.Get(commonLabeler.LabelReplicaName)]
	if !ok1 || !ok2 || !ok3 {
		return nil
	}
	return completed.FindHost(clusterName, shardName, replicaName)
}

// isHostReachableForHook reports whether SQL can plausibly be executed against this host's
// pod. Used by the pre-delete hook path to avoid blocking deletion of a host whose pod is
// already crashed / gone, and by the cluster-level fan-out targets to avoid failing a
// reconcile over a host that has not been created yet.
func (w *worker) isHostReachableForHook(ctx context.Context, host *api.Host) bool {
	if host == nil {
		return false
	}
	// Reuse the existing pod-OK check: it returns true only when the pod is Running and
	// all containers are Ready. A pre-delete drain hook can't run against anything less.
	return w.isPodOK(ctx, host)
}

// runHostSQLHookAction executes a SQL hook action on a specific host.
func (w *worker) runHostSQLHookAction(ctx context.Context, action *api.HookAction, host *api.Host) error {
	if !action.HasSQL() {
		// Sanity check
		return nil
	}

	sql := action.SQL
	if len(sql.Queries) == 0 {
		// Sanity check
		return nil
	}

	w.a.V(1).M(host).F().Info("Running SQL host hook on %s: %v", host.GetName(), sql.Queries)
	return w.ensureClusterSchemer(host).ExecHost(ctx, host, sql.Queries)
}

// runClusterSQLHookAction executes a SQL hook action at cluster scope, respecting action.Target.
func (w *worker) runClusterSQLHookAction(ctx context.Context, action *api.HookAction, cluster *api.Cluster) error {
	if !action.HasSQL() {
		// Sanity check
		return nil
	}

	sql := action.SQL
	if len(sql.Queries) == 0 {
		// Sanity check
		return nil
	}

	switch action.GetTarget() {
	case api.HookTargetAllHosts:
		w.a.V(1).M(cluster).F().Info("Running SQL cluster hook on all hosts: %v", sql.Queries)
		var hosts []*api.Host
		cluster.WalkHosts(func(host *api.Host) error {
			hosts = append(hosts, host)
			return nil
		})
		if len(hosts) == 0 {
			return fmt.Errorf("cluster %s: %w", cluster.GetName(), errClusterHasNoHostsForHook)
		}
		return w.execHookOnHosts(ctx, sql.Queries, hosts)

	case api.HookTargetAllShards:
		w.a.V(1).M(cluster).F().Info("Running SQL cluster hook on all shards (first replica each): %v", sql.Queries)
		var hosts []*api.Host
		cluster.WalkShards(func(_ int, shard api.IShard) error {
			if chiShard, ok := shard.(*api.ChiShard); ok {
				if host := chiShard.FirstHost(); host != nil {
					hosts = append(hosts, host)
				}
			}
			return nil
		})
		return w.execHookOnHosts(ctx, sql.Queries, hosts)

	default: // HookTargetFirstHost or empty
		firstHost := cluster.FirstHost()
		if firstHost == nil {
			return fmt.Errorf("cluster %s: %w", cluster.GetName(), errClusterHasNoHostsForHook)
		}
		w.a.V(1).M(cluster).F().Info("Running SQL cluster hook on first host %s: %v", firstHost.GetName(), sql.Queries)
		return w.ensureClusterSchemer(firstHost).ExecHost(ctx, firstHost, sql.Queries)
	}
}

// execHookOnHosts runs queries on each host, reporting every host's failure rather than
// only the first, so the caller's failurePolicy decision sees the full blast radius.
//
// A host whose pod cannot serve SQL is skipped, not failed: the host list comes from the
// spec, so during a scale-up it names hosts whose pod does not exist yet, and a pre-hook
// with the default failurePolicy Fail would otherwise abort the reconcile. Skipping them
// all is a different matter - the hook then ran nowhere, which is reported as an error.
func (w *worker) execHookOnHosts(ctx context.Context, queries []string, hosts []*api.Host) error {
	var errs []error
	skipped := 0
	for _, host := range hosts {
		if !w.isHostReachableForHook(ctx, host) {
			// Event, not just a log line: a hook that quietly ran on fewer hosts than asked is
			// the same class of silent no-op this whole path exists to prevent.
			w.a.V(1).M(host).F().
				WithEvent(host.GetCR(), a.EventActionReconcile, a.EventReasonHookSkippedUnreachableHost).
				Warning("Skipping cluster hook on unreachable host: %s", host.GetName())
			skipped++
			continue
		}
		if err := w.ensureClusterSchemer(host).ExecHost(ctx, host, queries); err != nil {
			w.a.V(1).M(host).F().Warning("Cluster hook on host %s failed: %v", host.GetName(), err)
			errs = append(errs, fmt.Errorf("host %s: %w", host.GetName(), err))
		}
	}
	if (len(hosts) > 0) && (skipped == len(hosts)) {
		errs = append(errs, fmt.Errorf("hook did not run: all %d host(s) unreachable", len(hosts)))
	}
	return errors.Join(errs...)
}
