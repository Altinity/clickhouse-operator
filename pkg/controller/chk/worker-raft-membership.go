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

package chk

// Raft-safe CHK rescale — design notes (verified against ClickHouse Keeper
// source, 23.8..master).
//
// Control plane: the operator drives membership through the static
// raft_configuration XML (enable_reconfiguration=false), NOT the client
// `reconfig` command. A versioned `reconfig` CAS does not exist in any Keeper
// version (the version arg is rejected unless -1, and no config version is
// exposed to read), and the client `reconfig` is optimistic/async and not
// leader-forwarded. The XML-diff path, by contrast, is applied by the leader
// only, one server at a time, and serialized by NuRaft — the reliable control
// plane. Existing clusters already run this mode; no migration needed.
//
// The split-brain window this guards: a freshly-started Keeper node loads the
// full XML config and boots as a voter. Two fresh nodes sharing {0,1,2} can
// elect a leader between themselves (2/3) while the original node still commits
// as {0} (1/1) — two quorums. One fresh node alone cannot: it needs an existing
// member's vote, which a healthy member with a live leader refuses. So the
// invariant is: the published XML never contains more than one server that is
// not yet in committed membership (staging, see worker.go), plus
// <start_as_follower> on joiners as a second guard.
//
// Confirmation barrier: `get /keeper/config` returns a node's committed cluster
// config. A node may lag but can never report a not-yet-committed config, so a
// positive match is a safe convergence signal (false-negative only, never
// false-positive). Fail-safe throughout: any barrier timeout / quorum loss /
// unreachable member returns an error and requeues — the operator never deletes
// or mutates on an unconfirmed observation.

import (
	"context"
	"errors"
	"fmt"
	"time"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/keeper"
	"github.com/altinity/clickhouse-operator/pkg/util"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// raftBarrierTimeout bounds a single reconcile's wait for the committed
	// Raft membership to converge to the published XML. On timeout the
	// reconcile fails and is requeued — fail-safe stuck, nothing is deleted.
	raftBarrierTimeout = 5 * time.Minute
	// raftVerifyTimeout is the shorter final-assertion wait used right before
	// marking the CR Completed (per-step barriers have already passed).
	raftVerifyTimeout = 30 * time.Second
	raftPollInterval  = 5 * time.Second
	fourLetterTimeout = 3 * time.Second
)

var errRaftNotConverged = errors.New("raft membership has not converged to the published configuration")

// keeperClientAddr returns the "fqdn:port" ZK-client endpoint of the host —
// the same hostname that is written into the raft XML.
func (w *worker) keeperClientAddr(host *api.Host) string {
	return fmt.Sprintf("%s:%d", w.c.namer.Name(interfaces.NameInstanceHostname, host), host.ZKPort.Value())
}

// publishedMemberIDs returns server ids of hosts currently published in the
// shared raft XML (i.e. not staged out with TagExclude). Server id == replica
// index, mirroring getServerId in pkg/model/chk/config/generator.go.
func publishedMemberIDs(cr api.ICustomResource) map[int]bool {
	ids := map[int]bool{}
	cr.WalkHosts(func(host *api.Host) error {
		if !host.GetReconcileAttributes().IsExclude() {
			ids[host.GetRuntime().GetAddress().GetReplicaIndex()] = true
		}
		return nil
	})
	return ids
}

// membershipObservers returns the hosts whose committed configuration must be
// checked by a barrier: every published host.
func (w *worker) membershipObservers(cr api.ICustomResource) (hosts []*api.Host) {
	cr.WalkHosts(func(host *api.Host) error {
		if !host.GetReconcileAttributes().IsExclude() {
			hosts = append(hosts, host)
		}
		return nil
	})
	return hosts
}

// waitRaftMembershipConverged polls `get /keeper/config` on every observer
// host until each one returns exactly the expected id set. Unreachable
// members or a mismatch keep the barrier closed (fail-safe): a node can
// report a lagging configuration but never a not-yet-committed one.
func (w *worker) waitRaftMembershipConverged(
	ctx context.Context,
	cr api.ICustomResource,
	expected map[int]bool,
	timeout time.Duration,
) error {
	deadline := time.Now().Add(timeout)
	for {
		if util.IsContextDone(ctx) {
			return ctx.Err()
		}
		if w.raftMembershipConverged(ctx, cr, expected) {
			return nil
		}
		if time.Now().After(deadline) {
			return errRaftNotConverged
		}
		util.WaitContextDoneOrTimeout(ctx, raftPollInterval)
	}
}

func (w *worker) raftMembershipConverged(ctx context.Context, cr api.ICustomResource, expected map[int]bool) bool {
	for _, host := range w.membershipObservers(cr) {
		fqdn := w.c.namer.Name(interfaces.NameInstanceHostname, host)
		members, err := keeper.GetCommittedMembership(ctx, fqdn, host.ZKPort.Value())
		if err != nil || !keeper.SameIDs(members, expected) {
			w.a.V(1).M(host).F().Info(
				"raft membership not converged on %s: got %v want %v err: %v",
				fqdn, keeper.MemberIDs(members), expected, err)
			return false
		}
	}
	return true
}

// waitRaftFollowersSynced waits until the leader among published hosts
// reports zk_synced_followers >= expectedVoters-1 (the freshly added member
// has caught up with the log). No-op for single-member clusters.
func (w *worker) waitRaftFollowersSynced(
	ctx context.Context,
	cr api.ICustomResource,
	expectedVoters int,
	timeout time.Duration,
) error {
	if expectedVoters < 2 {
		return nil
	}
	deadline := time.Now().Add(timeout)
	for {
		if util.IsContextDone(ctx) {
			return ctx.Err()
		}
		if w.raftFollowersSynced(ctx, cr, expectedVoters) {
			return nil
		}
		if time.Now().After(deadline) {
			return errRaftNotConverged
		}
		util.WaitContextDoneOrTimeout(ctx, raftPollInterval)
	}
}

func (w *worker) raftFollowersSynced(ctx context.Context, cr api.ICustomResource, expectedVoters int) bool {
	for _, host := range w.membershipObservers(cr) {
		addr := w.keeperClientAddr(host)
		role, err := keeper.GetRole(ctx, addr, fourLetterTimeout)
		if err != nil || role != keeper.RoleLeader {
			continue
		}
		synced, err := keeper.GetSyncedFollowers(ctx, addr, fourLetterTimeout)
		if err != nil {
			return false
		}
		return synced >= expectedVoters-1
	}
	// No leader found among published hosts
	return false
}

// crIsSecureOnlyKeeper reports whether EVERY Keeper host has its plaintext
// client port closed (!IsInsecure()) — the same condition under which
// Generator.getPlaintextListenerRemoval strips the static <tcp_port>. For such a
// CR nothing listens on the plaintext ZKPort, so every new Raft gate (the
// committed-membership barrier, the follower-sync barrier and the purge barrier)
// — all of which read /keeper/config and 4LW over that plaintext port — can
// never connect.
//
// The operator cannot fall back to the secure port either: dialing
// tcp_port_secure with the vendored go-zookeeper client requires a client
// certificate + key (its TLS path is mutual-TLS only), and a CHK's Keeper TLS is
// configured entirely by user-supplied server-side <openSSL> XML from which the
// operator can derive no client identity. No client TLS material is provisioned
// to the controller today (see pkg/controller/chi/worker-zk-integration.go,
// which skips TLS ZK dials for the same reason). Callers therefore detect this
// case and DEGRADE the gates to pre-branch behavior (unverified rescale) rather
// than looping Aborted forever. Real TLS observation is a documented follow-up.
func crIsSecureOnlyKeeper(cr api.ICustomResource) bool {
	anyHost := false
	allPlaintextClosed := true
	cr.WalkHosts(func(host *api.Host) error {
		anyHost = true
		// A nil host carries no posture; treat it conservatively as "not closed"
		// so a phantom host never trips the secure-only degradation.
		if host == nil || host.IsInsecure() {
			allPlaintextClosed = false
		}
		return nil
	})
	return anyHost && allPlaintextClosed
}

// warnRaftGatesSkippedSecureOnly records — loudly — that a Raft
// rescale-safety gate was skipped for a secure-only Keeper. It always emits a
// (k8s-deduplicated) Warning event (visible in `kubectl describe`), and — when
// the operator config has `status.fields.actions` enabled (off by default) —
// also pushes a Status.Actions entry. The Warning event alone guarantees the
// degradation is observable regardless of that config flag.
func (w *worker) warnRaftGatesSkippedSecureOnly(cr api.ICustomResource, gate string) {
	msg := fmt.Sprintf(
		"secure-only Keeper: skipping Raft %s — the operator cannot read /keeper/config or 4LW over the closed plaintext port and has no TLS client material to dial the secure port; rescale-safety is NOT verified (pre-branch behavior). See docs/chk-rescale-raft-safety-v3.md.",
		gate)
	w.a.WithEvent(cr, a.EventActionReconcile, a.EventReasonRaftMembershipUnverified).
		WithActions(cr).
		M(cr).F().Warning(msg)
}

// hostJoinsEstablishedCluster mirrors the staging condition of
// stageNewHostsForRaftJoin: the host is joining a cluster that already existed
// on a prior successful reconcile. Keyed off the CR ancestor (stable across the
// reconcile), NOT the mutable per-host statuses — see api.CRHasEstablishedCluster.
func hostJoinsEstablishedCluster(host *api.Host) bool {
	return api.CRHasEstablishedCluster(host.GetCR())
}

// removedHosts collects hosts being removed by the current action plan
// (their objects come from the ancestor CR, so runtime addresses and names
// are resolvable).
func removedHosts(cr *apiChk.ClickHouseKeeperInstallation) (hosts []*api.Host) {
	ap := cr.EnsureRuntime().ActionPlan
	if ap == nil {
		return nil
	}
	ap.WalkRemoved(
		func(cluster api.ICluster) {
			cluster.WalkHosts(func(host *api.Host) error {
				hosts = append(hosts, host)
				return nil
			})
		},
		func(shard api.IShard) {
			shard.WalkHosts(func(host *api.Host) error {
				hosts = append(hosts, host)
				return nil
			})
		},
		func(host *api.Host) {
			hosts = append(hosts, host)
		},
	)
	return hosts
}

// prepareRaftScaleDown moves Raft leadership off the hosts being removed
// BEFORE the shrunk raft XML is published: `rqld` on the lowest-ordinal
// surviving host (23.8-compatible). Best-effort: Keeper can remove a leader
// on its own (it yields leadership first); the proactive transfer just avoids
// that riskier path, so failures here only log a warning. The actual safety
// fence is the purge barrier in clean().
func (w *worker) prepareRaftScaleDown(ctx context.Context, cr *apiChk.ClickHouseKeeperInstallation) {
	removed := removedHosts(cr)
	if len(removed) == 0 {
		return
	}

	// Is the current leader among the removed hosts?
	leaderRemoved := false
	for _, host := range removed {
		role, err := keeper.GetRole(ctx, w.keeperClientAddr(host), fourLetterTimeout)
		if err == nil && role == keeper.RoleLeader {
			leaderRemoved = true
			break
		}
	}
	if !leaderRemoved {
		return
	}

	// Ask the lowest-ordinal survivor to take leadership over.
	survivors := w.membershipObservers(cr)
	if len(survivors) == 0 {
		return
	}
	target := survivors[0]
	w.a.V(1).M(cr).F().Info("scale-down: transferring raft leadership to surviving host %s", target.GetName())
	if _, err := keeper.RequestLeadership(ctx, w.keeperClientAddr(target), fourLetterTimeout); err != nil {
		w.a.V(1).M(cr).F().Warning("rqld to %s failed: %v", target.GetName(), err)
		return
	}
	// Poll until a survivor is the leader (bounded, best-effort).
	deadline := time.Now().Add(1 * time.Minute)
	for time.Now().Before(deadline) && !util.IsContextDone(ctx) {
		role, err := keeper.GetRole(ctx, w.keeperClientAddr(target), fourLetterTimeout)
		if err == nil && (role == keeper.RoleLeader || role == keeper.RoleStandalone) {
			return
		}
		util.WaitContextDoneOrTimeout(ctx, raftPollInterval)
	}
	w.a.V(1).M(cr).F().Warning("leadership did not move to a survivor in time; relying on Keeper's own leader-removal handling")
}

// raftSafeToPurge is the purge-time safety invariant: a StatefulSet or PVC
// belonging to a Keeper member may only be deleted once every published
// member reports committed Raft membership equal to the published set.
//
// The gate is keyed off what `reg` is actually about to delete, not off the
// transient ActionPlan: on a stopped CR, Task 6's IsStopped short-circuit
// lets finalizeReconcileAndMarkCompleted advance the ancestor before the
// removal is committed, so by the next pass ActionPlan.GetRemovedHostsNum()
// can already read 0 even though the STS/PVC purge is still pending — an
// ActionPlan-keyed trigger would wave that purge through unchecked. Likewise
// after an operator crash/restart mid scale-down, the in-memory ActionPlan is
// gone but an orphaned STS/PVC can still be sitting in `reg`. Keying off the
// purge set itself is correct in both cases.
//
// reg containing no StatefulSet/PVC entries means nothing raft-critical is
// about to be deleted (only ConfigMaps/Services/Secrets/PDBs, say) — those
// purge freely, at no barrier cost on routine reconciles. Otherwise the
// barrier must pass. When the cluster is stopped or unreachable this
// intentionally blocks ALL purge (fail-safe stuck) until the barrier can run
// again and actually observe convergence.
func (w *worker) raftSafeToPurge(ctx context.Context, cr api.ICustomResource, reg *model.Registry) bool {
	raftCritical := false
	reg.Walk(func(entityType model.EntityType, _ meta.Object) {
		if entityType == model.StatefulSet || entityType == model.PVC {
			raftCritical = true
		}
	})
	if !raftCritical {
		return true
	}
	if cr.IsStopped() {
		// A stopped CR has its observers down: convergence is unobservable, so
		// don't burn the full waitRaftMembershipConverged timeout every pass.
		// Fail-safe is preserved (purge stays blocked); on unstop, discovery
		// re-finds the orphan and this gate runs the barrier again for real.
		return false
	}
	if crIsSecureOnlyKeeper(cr) {
		// Secure-only Keeper: the barrier cannot connect (plaintext port closed,
		// no operator TLS client material). Blocking would orphan the STS/PVC
		// forever; degrade to pre-branch behavior (allow the purge, unverified).
		w.warnRaftGatesSkippedSecureOnly(cr, "purge barrier")
		return true
	}
	return w.waitRaftMembershipConverged(ctx, cr, publishedMemberIDs(cr), raftBarrierTimeout) == nil
}
