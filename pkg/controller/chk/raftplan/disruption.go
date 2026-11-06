// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package raftplan

// CheckDisruption evaluates an already characterized proposed host mutation.
// Rolling classification and MemberCount/ReadyCount intentionally follow the
// legacy quorumSizingEnsemble policy in this initial, behavior-neutral phase.
// It does not acquire a lock: the worker must serialize disruptions and
// reobserve immediately before applying the decision.
func CheckDisruption(s Snapshot, hostID HostID) DisruptionDecision {
	h, ok := s.Hosts[hostID]
	if !ok {
		return DisruptionDecision{Reason: DisruptionStateUnknown}
	}
	if !h.WouldDisrupt || s.Stopped {
		return DisruptionDecision{Allowed: true, Reason: DisruptionNotNeeded}
	}
	if !s.CountsKnown || !h.ReadyKnown || s.MemberCount < 0 || s.ReadyCount < 0 || s.ReadyCount > s.MemberCount {
		return DisruptionDecision{Reason: DisruptionStateUnknown}
	}

	// These exceptions intentionally mirror the existing 3+ member gate.
	// Protecting a one- or two-member ensemble from any disruption requires
	// an explicit availability policy rather than changing it in this refactor.
	if !s.Rolling || s.MemberCount < 3 || !h.Ready {
		return DisruptionDecision{Allowed: true, Reason: DisruptionSafe}
	}

	if s.ReadyCount-1 < s.MemberCount/2+1 {
		return DisruptionDecision{Reason: DisruptionQuorumUnsafe}
	}
	return DisruptionDecision{Allowed: true, Reason: DisruptionSafe}
}
