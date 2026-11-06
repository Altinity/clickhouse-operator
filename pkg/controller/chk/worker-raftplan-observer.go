// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package chk

import (
	"context"
	"errors"
	"fmt"

	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	kubeTypes "k8s.io/apimachinery/pkg/types"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/controller/chk/raftplan"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
)

func raftPlanHostID(host *api.Host) raftplan.HostID {
	return raftplan.HostID(host.GetName())
}

// observeRaftPlanHosts returns values captured from live StatefulSet Gets.
// Unlike a cached CurStatefulSet, an API error is explicitly Unknown; callers
// cannot interpret an unreadable member as absent or unready.
// The authoritative admission gate, preliminary staging, and optional shadow
// diagnostics all use this read-only adapter. No resources are mutated.
func (w *worker) observeRaftPlanHosts(
	ctx context.Context,
	cr *apiChk.ClickHouseKeeperInstallation,
	published []raftplan.HostID,
	publishedKnown bool,
) (raftplan.Snapshot, error) {
	if w == nil || w.c == nil || w.c.kube == nil || cr == nil {
		return raftplan.Snapshot{}, fmt.Errorf("raftplan observation requires worker, kube adapter, and CHK")
	}
	snap := raftplan.Snapshot{
		Established:    crHasEstablishedCluster(cr),
		Stopped:        cr.IsStopped(),
		Hosts:          map[raftplan.HostID]raftplan.HostState{},
		PublishedHosts: append([]raftplan.HostID(nil), published...),
		PublishedKnown: publishedKnown,
	}
	// Preserve the same order that the existing host walker uses to admit
	// joiners. Do not sort by StatefulSet creation timestamp.
	if ancestor := cr.GetAncestor(); ancestor != nil {
		_ = ancestor.WalkHosts(func(host *api.Host) error {
			if host != nil {
				snap.AncestorHosts = append(snap.AncestorHosts, raftPlanHostID(host))
			}
			return nil
		})
	}
	existing := make(map[raftplan.HostID]bool, len(snap.AncestorHosts))
	for _, id := range snap.AncestorHosts {
		existing[id] = true
	}

	walkErrors := cr.WalkHosts(func(host *api.Host) error {
		if host == nil {
			return nil
		}
		id := raftPlanHostID(host)
		if _, duplicate := snap.Hosts[id]; duplicate {
			return fmt.Errorf("duplicate Raft planner host ID %q", id)
		}
		snap.DesiredHosts = append(snap.DesiredHosts, id)
		state := raftplan.HostState{
			ID:          id,
			InAncestor:  existing[id],
			StatefulSet: raftplan.PresenceUnknown,
		}
		sts, getErr := w.c.kube.STS().Get(ctx, host)
		switch {
		case apiErrors.IsNotFound(getErr):
			state.StatefulSet = raftplan.PresenceAbsent
			state.ReadyKnown = true
		case getErr != nil:
			snap.Hosts[id] = state
			return fmt.Errorf("read Keeper StatefulSet for %s: %w", id, getErr)
		case sts == nil:
			snap.Hosts[id] = state
			return fmt.Errorf("nil StatefulSet with no error for %s", id)
		default:
			state.StatefulSet = raftplan.PresencePresent
			state.ReadyKnown = true
			state.Ready = sts.Status.ReadyReplicas > 0
			if state.Ready {
				state.PodStarted, state.StartedKnown = true, true
			} else if w.c.kube.Pod() != nil && hostIsStagedScaleUpJoin(host) {
				// A StatefulSet exists before Keeper starts. Observation must
				// distinguish it from a joiner eligible for publication.
				pod, podErr := w.c.kube.Pod().Get(ctx, host)
				switch {
				case apiErrors.IsNotFound(podErr):
					state.StartedKnown = true
				case podErr != nil:
					snap.Hosts[id] = state
					return fmt.Errorf("read Keeper joiner Pod %s: %w", id, podErr)
				case pod == nil:
					snap.Hosts[id] = state
					return fmt.Errorf("nil Keeper Pod %s on successful Get", id)
				default:
					state.StartedKnown = true
					state.PodStarted = k8s.PodHasAllContainersStarted(pod)
				}
			}
		}
		snap.Hosts[id] = state
		return nil
	})
	// WalkHosts aggregates callback errors as []error, not error.
	return snap, errors.Join(walkErrors...)
}

// observeRaftPlanAdmission reads actual published membership from the common
// ConfigMap. It is deliberately separate from the live STS reads above so
// integration tests can compare policy decisions without constructing a CM.
func (w *worker) observeRaftPlanAdmission(
	ctx context.Context, cr *apiChk.ClickHouseKeeperInstallation,
) (raftplan.Snapshot, error) {
	// Test-only seam for published membership. This does not bypass the
	// actual live StatefulSet observer or the planner's safety decisions.
	if w != nil && w.readPublishedRaftMembersFn != nil {
		published, known, err := w.readPublishedRaftMembersFn(ctx, cr)
		if err != nil {
			return raftplan.Snapshot{}, fmt.Errorf("read published Raft membership: %w", err)
		}
		return w.observeRaftPlanHosts(ctx, cr, published, known)
	}
	if w == nil || w.c == nil || w.c.APIReader == nil || w.c.namer == nil || w.task == nil || cr == nil {
		return raftplan.Snapshot{}, fmt.Errorf("raftplan admission observation is unavailable")
	}
	// Use the exact common ConfigMap name produced by the production creator.
	desiredCM := w.task.Creator().CreateConfigMap(interfaces.ConfigMapCommon, w.options())
	current := &core.ConfigMap{}
	key := kubeTypes.NamespacedName{Namespace: desiredCM.Namespace, Name: desiredCM.Name}
	err := w.c.APIReader.Get(ctx, key, current)
	var published []raftplan.HostID
	publishedKnown := false
	switch {
	case apiErrors.IsNotFound(err):
		// A confirmed missing ConfigMap has no published members.
		publishedKnown = true
	case err != nil:
		return raftplan.Snapshot{}, fmt.Errorf("read live Keeper common ConfigMap: %w", err)
	default:
		hostnames, known, parseErr := raftplan.PublishedHostnames(current.Data)
		if parseErr != nil {
			return raftplan.Snapshot{}, parseErr
		}
		publishedKnown = known
		if known {
			// The Raft XML uses instance hostnames, while policy keys are
			// Host.GetName(). Unknown names remain in the published set and
			// therefore cannot be silently dropped by the planner.
			byHostname := make(map[string]raftplan.HostID)
			visit := func(host *api.Host) error {
				if host != nil {
					name := w.c.namer.Name(interfaces.NameInstanceHostname, host)
					byHostname[name] = raftPlanHostID(host)
				}
				return nil
			}
			_ = cr.WalkHosts(visit)
			if ancestor := cr.GetAncestor(); ancestor != nil {
				_ = ancestor.WalkHosts(visit)
			}
			for _, name := range hostnames {
				id, ok := byHostname[name]
				if !ok {
					return raftplan.Snapshot{}, fmt.Errorf("published Raft hostname %q has no known CHK host (observation uncertain)", name)
				}
				published = append(published, id)
			}
		}
	}
	return w.observeRaftPlanHosts(ctx, cr, published, publishedKnown)
}

// raftPlanDisruptionSnapshot adapts the same prepared STS status and frozen
// quorum denominator as the existing quorum gate. It performs no extra API
// reads: the caller must invoke it immediately after its existing fresh
// snapshotHostEnsemble/PrepareHostStatefulSetWithStatus sequence.
func (w *worker) raftPlanDisruptionSnapshot(
	ctx context.Context, host *api.Host,
	opts *statefulset.ReconcileOptions, snap hostEnsembleSnapshot,
) raftplan.Snapshot {
	id := raftPlanHostID(host)
	status := host.GetReconcileAttributes().GetStatus()
	willDisrupt := !host.IsStopped() && !status.Is(types.ObjectStatusRequested) &&
		(!status.Is(types.ObjectStatusSame) || w.shouldForceRestartHost(ctx, host) ||
			(opts != nil && opts.ForceRecreate()))
	return raftplan.Snapshot{
		Stopped:     host.IsStopped(),
		Rolling:     snap.rolling,
		MemberCount: snap.members,
		ReadyCount:  snap.readyCount,
		CountsKnown: true,
		Hosts: map[raftplan.HostID]raftplan.HostState{
			id: {
				ID:           id,
				Ready:        hostContributesReady(host),
				ReadyKnown:   true,
				WouldDisrupt: willDisrupt,
			},
		},
	}
}
