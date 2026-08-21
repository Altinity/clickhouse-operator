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

import (
	"context"
	"fmt"

	apps "k8s.io/api/apps/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

// Raft / ensemble safety for CHK (#2069).
//
// Live Ready count decides the mode — not CR ancestor / host inventory:
//
//   - Live quorum (Ready members >= majority): rolling recreate of a healthy
//     ensemble — wait Ready before the next host, and refuse to disrupt a host
//     if doing so would drop below quorum.
//   - No live quorum (fresh install, resume-from-stopped, or already broken):
//     bootstrap / recovery — wait Started only so siblings can come up together.
//
// Hooks below stay thin so a fuller Raft-membership barrier (committed
// /keeper/config + mntr, as in PR #2041) can replace verifyHostEnsembleMembership
// without reshaping the reconcile loop.

// chkStatefulSetFallback aborts the reconcile on STS create/update wait failure.
// DefaultFallback returns ErrCRUDIgnore, which lets the host loop recreate the
// next replica while the previous one never rejoined — the #2069 failure mode.
type chkStatefulSetFallback struct{}

func newChkStatefulSetFallback() *chkStatefulSetFallback {
	return &chkStatefulSetFallback{}
}

func (f *chkStatefulSetFallback) OnStatefulSetCreateFailed(ctx context.Context, host *api.Host) common.ErrorCRUD {
	return common.ErrCRUDAbort
}

func (f *chkStatefulSetFallback) OnStatefulSetUpdateFailed(
	ctx context.Context,
	oldStatefulSet *apps.StatefulSet,
	host *api.Host,
	sts interfaces.IKubeSTS,
) common.ErrorCRUD {
	return common.ErrCRUDAbort
}

// raftQuorumSize is Raft majority for an ensemble of n members (n/2 + 1).
func raftQuorumSize(members int) int {
	if members <= 0 {
		return 0
	}
	return members/2 + 1
}

// countReadyEnsembleMembers counts Keeper hosts whose StatefulSet reports
// ReadyReplicas > 0 (each host is typically a 1-replica STS).
//
// countReadyEnsembleMembersFn, when set on the worker, overrides live lookup
// (tests inject fixed Ready counts).
func (w *worker) countReadyEnsembleMembers(ctx context.Context, cr api.ICustomResource) int {
	if w.countReadyEnsembleMembersFn != nil {
		return w.countReadyEnsembleMembersFn(ctx, cr)
	}
	if cr == nil {
		return 0
	}
	ready := 0
	_ = cr.WalkHosts(func(host *api.Host) error {
		sts := host.Runtime.CurStatefulSet
		if sts == nil && w.c != nil {
			sts, _ = w.c.kube.STS().Get(ctx, host)
		}
		if sts != nil && sts.Status.ReadyReplicas > 0 {
			ready++
		}
		return nil
	})
	return ready
}

// ensembleHasLiveQuorum reports whether enough hosts are Ready to form a Raft
// majority. Resume-from-stopped and fresh bootstrap both yield false (0 Ready)
// and therefore take the fast startup path.
func (w *worker) ensembleHasLiveQuorum(ctx context.Context, cr api.ICustomResource) bool {
	if cr == nil {
		return false
	}
	n := cr.HostsCount()
	return w.countReadyEnsembleMembers(ctx, cr) >= raftQuorumSize(n)
}

// hostContributesReady reports whether this host currently counts toward live quorum.
func hostContributesReady(host *api.Host) bool {
	if host == nil || host.Runtime.CurStatefulSet == nil {
		return false
	}
	return host.Runtime.CurStatefulSet.Status.ReadyReplicas > 0
}

// ensureQuorumSafeToDisruptHost refuses to take down a Ready host when the
// remaining Ready members would fall below quorum. No-op when the ensemble
// already lacks live quorum (bootstrap / resume-from-stopped / recovery), or
// when there is only one host (restart is unavoidable — no sibling can hold
// quorum).
func (w *worker) ensureQuorumSafeToDisruptHost(ctx context.Context, host *api.Host) error {
	cr := host.GetCR()
	if cr == nil {
		return nil
	}
	n := cr.HostsCount()
	if n <= 1 {
		return nil
	}
	q := raftQuorumSize(n)
	ready := w.countReadyEnsembleMembers(ctx, cr)
	if ready < q {
		return nil
	}
	remaining := ready
	if hostContributesReady(host) {
		remaining--
	}
	if remaining < q {
		return fmt.Errorf(
			"refusing to disrupt host %s: ready=%d remaining=%d quorum=%d (would drop below Raft quorum)",
			host.GetName(), ready, remaining, q,
		)
	}
	return nil
}

// prepareStsReconcileOptsWaitSection sets STS launch waits for Keeper.
//
// Live quorum present: wait until Ready before moving on.
// No live quorum: wait Started only — Ready would deadlock until siblings exist
// (bootstrap, resume-from-stopped, or recovery).
func (w *worker) prepareStsReconcileOptsWaitSection(ctx context.Context, host *api.Host, opts *statefulset.ReconcileOptions) *statefulset.ReconcileOptions {
	if opts == nil {
		opts = statefulset.NewReconcileStatefulSetOptions()
	}
	probes := host.GetCluster().GetReconcile().Host.Wait.Probes
	liveQuorum := w.ensembleHasLiveQuorum(ctx, host.GetCR())

	if probes.GetStartup().IsTrue() || !liveQuorum {
		opts = opts.SetWaitUntilStarted()
		w.a.V(1).M(host).F().Warning("Setting option SetWaitUntilStarted")
	}

	switch {
	case liveQuorum && !probes.GetReadiness().IsFalse():
		opts = opts.SetWaitUntilReady()
		w.a.V(1).M(host).F().Warning("Setting option SetWaitUntilReady (live Raft quorum)")
	case !liveQuorum:
		w.a.V(1).M(host).F().Info("Skip WaitUntilReady — no live quorum (bootstrap / resume-from-stopped / recovery)")
	}

	return opts
}

// waitHostRaftJoined is the post-host hook for confirming the replica is part of
// the live ensemble before the reconciler advances.
//
// Today a no-op beyond what STS Ready wait already enforced. Replace or extend
// verifyHostEnsembleMembership with committed Raft membership checks as in
// Altinity/clickhouse-operator#2041.
func (w *worker) waitHostRaftJoined(ctx context.Context, host *api.Host) error {
	return w.verifyHostEnsembleMembership(ctx, host)
}

// verifyHostEnsembleMembership is the extension point for Raft membership
// verification. Currently a no-op: STS Ready wait already ran when live quorum
// mode applied. Implement committed-config / leader sync barriers here when
// adopting the fuller rescale design from PR #2041.
func (w *worker) verifyHostEnsembleMembership(ctx context.Context, host *api.Host) error {
	_ = ctx
	_ = host
	return nil
}
