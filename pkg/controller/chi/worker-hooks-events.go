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

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// firedHostEvents returns the set of HookEvents that fired for this host during the
// current reconcile. The result drives whether each `events:`-tagged hook runs:
// MatchesAnyEvent compares the action's declared events against this list.
//
// Multiple events can fire simultaneously — e.g. a config change on a host that is
// also being stopped fires both HostConfigRestart and HostStop, plus the HostShutdown
// aggregate. A host hook listing any of those in its `events:` field will run.
//
// Thin wrapper that resolves all per-host predicates (some need *worker / ctx) and
// hands the resulting bool tuple to computeFiredHostEventsFromState — the pure
// event-firing rules, which are unit-tested directly.
func (w *worker) firedHostEvents(ctx context.Context, host *api.Host) []api.HookEvent {
	if host == nil {
		return nil
	}
	// Cache GetAncestor() — it walks cr.NormalizedCRCompleted.FindCluster.FindShard.FindHost,
	// which is O(clusters × shards × hosts) per call. HasAncestor() calls GetAncestor()
	// internally; calling both would double the cost per host per reconcile.
	ancestor := host.GetAncestor()
	ancestorIsStopped := false
	if ancestor != nil {
		ancestorIsStopped = ancestor.IsStopped()
	}
	return computeFiredHostEventsFromState(hostState{
		hasAncestor:       ancestor != nil,
		ancestorIsStopped: ancestorIsStopped,
		isStopped:         host.IsStopped(),
		forceRestart:      w.shouldForceRestartHost(ctx, host),
		requiresRollout:   hostRequiresStatefulSetRollout(host),
	})
}

// hostState captures the boolean inputs that determine which HookEvents fire for a
// host. Decoupling state from the live *api.Host pointer lets unit tests exercise
// every combination without the cluster/CR plumbing that HasAncestor / IsStopped /
// shouldForceRestartHost transitively need.
type hostState struct {
	hasAncestor       bool // host had prior state (not first creation)
	ancestorIsStopped bool // ancestor host was marked stopped (only meaningful if hasAncestor)
	isStopped         bool // current spec marks the host stopped
	forceRestart      bool // operator decided this host needs an in-place software restart
	requiresRollout   bool // pod-template change forces a StatefulSet rollout
}

// computeFiredHostEventsFromState is the pure event-firing logic for a host. Given
// a hostState tuple it returns the set of HookEvents that fired this reconcile.
//
// HostDelete is NOT emitted from this path — by the time the regular reconcile loop
// reaches a host, that host is part of the current spec. The HostDelete event is
// emitted from the deletion sweep (firedHostDeleteEvents).
func computeFiredHostEventsFromState(s hostState) []api.HookEvent {
	var fired []api.HookEvent

	// Lifecycle: Create when host is new (no ancestor), otherwise Update.
	if s.hasAncestor {
		fired = append(fired, api.HookEventHostUpdate)
	} else {
		fired = append(fired, api.HookEventHostCreate)
	}

	// Stop / Start are independent of the lifecycle events above.
	if s.isStopped {
		fired = append(fired, api.HookEventHostStop)
	}
	if s.hasAncestor && s.ancestorIsStopped && !s.isStopped {
		fired = append(fired, api.HookEventHostStart)
	}

	// Restart-class events: the operator decided to take the pod down for either
	// a software restart or a StatefulSet rollout.
	if s.forceRestart {
		fired = append(fired, api.HookEventHostConfigRestart)
	}
	if s.requiresRollout {
		fired = append(fired, api.HookEventHostRollout)
	}

	// Aggregate: "the pod is going down for any reason this reconcile". Useful for
	// graceful drain / external de-registration patterns. HostDelete intentionally
	// not included — it's emitted from the deletion sweep instead.
	if util.SliceContainsAny(fired,
		api.HookEventHostStop,
		api.HookEventHostConfigRestart,
		api.HookEventHostRollout,
	) {
		fired = append(fired, api.HookEventHostShutdown)
	}

	return fired
}

// firedClusterEvents returns the set of HookEvents that fired for this cluster
// during the current reconcile.
//
// Cluster scope is INDEPENDENT of host scope: cluster-level hooks fire on cluster
// lifecycle events (Create / Reconcile / Delete), not on host events. This is the
// key design split — cluster hooks are not "any host did X" aggregations; they're
// tied to the cluster's own lifecycle.
//
// Rules:
//   - ALL hosts are new (none has an ancestor) → ClusterCreate. The whole cluster
//     is being reconciled for the first time.
//   - At least one host has an ancestor → ClusterReconcile. The cluster existed
//     before; this is an ongoing reconcile (which may include shard/replica
//     scale-ups, where some hosts are new and some are not).
//   - Empty cluster (no hosts at all) → no events.
//   - ClusterDelete is NOT emitted here — it lives in the dedicated cluster delete
//     sweep (parallel to runHostPreDeleteHooks). That wiring is a follow-up.
func (w *worker) firedClusterEvents(cluster *api.Cluster) []api.HookEvent {
	if cluster == nil {
		return nil
	}

	// Whether cluster has any hosts that existed before.
	hasOldHost := false
	// Whether cluster has any hosts in current reconcile.
	hasAnyHost := false
	cluster.WalkHosts(func(host *api.Host) error {
		// Cluster has at least one host in current reconcile.
		hasAnyHost = true
		if host.HasAncestor() {
			// Host has an ancestor — it existed before.
			hasOldHost = true
		}
		return nil
	})

	if !hasAnyHost {
		// Empty cluster — no events to emit.
		return nil
	}

	if !hasOldHost {
		// All hosts are new → cluster is brand new.
		return []api.HookEvent{api.HookEventClusterCreate}
	}

	// At least one host existed before → this is an ongoing reconcile pass over
	// an existing cluster (even if other hosts are being added in a scale-up).
	// The event is named "Reconcile" rather than "Update" because it fires for
	// every operator reconcile cycle — including taskID-only force-reconciles
	// where the cluster spec didn't materially change. It does NOT signal a
	// material spec change.
	return []api.HookEvent{api.HookEventClusterReconcile}
}

// firedHostDeleteEvents returns the set of HookEvents that fire when a host is being
// torn down by the deletion sweep (worker-deleter.go). Distinct from firedHostEvents,
// which covers the regular reconcile pass and never sees deletions.
//
// HostShutdown is included because it's the "pod is going down" aggregate — a hook
// listening to HostShutdown drains on Stop / ConfigRestart / Rollout / Delete alike.
//
// Pure function (no *worker state needed) but kept as a method for symmetry with
// firedHostEvents and to keep all event-set producers grouped on the same receiver.
func (w *worker) firedHostDeleteEvents(host *api.Host) []api.HookEvent {
	// Do we have a host to check firing events for?
	if host == nil {
		return nil
	}

	return []api.HookEvent{api.HookEventHostDelete, api.HookEventHostShutdown}
}

// firedClusterDeleteEvents returns the set of HookEvents that fire when a cluster
// is being torn down. Sibling of firedClusterEvents for the deletion sweep.
//
// Currently emits only ClusterDelete; the deletion-sweep wiring that calls this is
// a follow-up. Defined now so the event-set knowledge stays in this file and the
// future caller has a single place to plug into.
func (w *worker) firedClusterDeleteEvents(cluster *api.Cluster) []api.HookEvent {
	// Do we have a cluster to check firing events for?
	if cluster == nil {
		return nil
	}

	return []api.HookEvent{api.HookEventClusterDelete}
}
