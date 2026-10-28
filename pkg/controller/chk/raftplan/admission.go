// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package raftplan

// PlanAdmission determines which Keeper hosts may be published into
// raft_configuration and which missing StatefulSets may be created.
//
// It preserves the legacy staged-admission policy for established ensembles
// with fewer than three ancestor members. Existing published members are
// never removed merely because their StatefulSet has disappeared. Deletion
// and scale-down are outside this policy and require a separate safety gate.
//
// The caller supplies DesiredHosts in stable server-ID order and must
// provide an independently obtained, live snapshot. The function never
// modifies any of its inputs.
func PlanAdmission(s Snapshot) AdmissionPlan {
	plan := AdmissionPlan{
		MayCreate: make(map[HostID]bool),
		Reason:    AdmissionAllowed,
	}

	// Preservation is the safe default. An unreadable ConfigMap cannot be
	// interpreted as an empty member set.
	included := unique(s.PublishedHosts)
	if !s.PublishedKnown {
		plan.Reason = ObservationUnknown
		plan.IncludedHosts = included
		return plan
	}

	// A stopped CHK may reconcile existing resources but must not create a
	// new member. Preserve precisely what was already published.
	if s.Stopped {
		plan.IncludedHosts = included
		plan.Reason = CHKStopped
		return plan
	}

	// Unknown StatefulSet existence for any requested member must not be
	// converted into an admission or a membership removal.
	for _, id := range unique(s.DesiredHosts) {
		host, ok := s.Hosts[id]
		if !ok || host.StatefulSet == PresenceUnknown {
			plan.IncludedHosts = included
			plan.Reason = ObservationUnknown
			return plan
		}
	}

	// Existing voters and members with live StatefulSets are preserved,
	// including after an interrupted creation and when normalizedCompleted
	// hasn't yet caught up with the current live topology.
	included = appendUnique(included, s.AncestorHosts...)
	for _, id := range unique(s.DesiredHosts) {
		if s.Hosts[id].StatefulSet == PresencePresent {
			included = appendUnique(included, id)
		}
	}

	// Fresh bootstrap and 3->N intentionally retain the existing fan-out
	// policy. The safe serial join policy applies only to an established
	// ensemble with fewer than three ancestor members.
	if !s.Established || len(unique(s.AncestorHosts)) >= 3 {
		for _, id := range unique(s.DesiredHosts) {
			included = appendUnique(included, id)
			if s.Hosts[id].StatefulSet == PresenceAbsent {
				plan.MayCreate[id] = true
			}
		}
		return finalizeAdmission(plan, included, s.PublishedHosts)
	}

	ancestor := make(map[HostID]bool, len(s.AncestorHosts))
	for _, id := range s.AncestorHosts {
		ancestor[id] = true
	}

	// A previous joiner with an existing unready StatefulSet blocks the
	// next absent joiner. The existing hosts remain eligible for safe
	// reconciliation so they can adopt the membership configuration.
	for _, id := range unique(s.DesiredHosts) {
		if ancestor[id] {
			continue
		}
		h := s.Hosts[id]
		if h.StatefulSet == PresencePresent && (!h.ReadyKnown || !h.Ready) {
			plan.BlockedBy = id
			plan.Reason = WaitingForJoiner
			return finalizeAdmission(plan, included, s.PublishedHosts)
		}
	}

	// If a previously published joiner has no StatefulSet, repair that
	// member before admitting a different one. Do not retract membership on
	// an ambiguous create response or skip over a stranded admission.
	for _, id := range unique(s.DesiredHosts) {
		if ancestor[id] {
			continue
		}
		if s.Hosts[id].StatefulSet == PresenceAbsent && contains(included, id) {
			plan.MayCreate[id] = true
			return finalizeAdmission(plan, included, s.PublishedHosts)
		}
	}

	// Admit at most one new absent member in stable ID order.
	for _, id := range unique(s.DesiredHosts) {
		if ancestor[id] {
			continue
		}
		if s.Hosts[id].StatefulSet == PresenceAbsent {
			included = appendUnique(included, id)
			plan.MayCreate[id] = true
			break
		}
	}
	return finalizeAdmission(plan, included, s.PublishedHosts)
}

func finalizeAdmission(plan AdmissionPlan, included, published []HostID) AdmissionPlan {
	plan.IncludedHosts = unique(included)
	plan.ShouldPublish = !equalMembers(plan.IncludedHosts, published)
	return plan
}

func contains(ids []HostID, want HostID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func unique(ids []HostID) []HostID {
	seen := make(map[HostID]bool, len(ids))
	result := make([]HostID, 0, len(ids))
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			result = append(result, id)
		}
	}
	return result
}

func appendUnique(ids []HostID, extra ...HostID) []HostID {
	for _, id := range extra {
		if !contains(ids, id) {
			ids = append(ids, id)
		}
	}
	return ids
}

func equalMembers(left, right []HostID) bool {
	left, right = unique(left), unique(right)
	if len(left) != len(right) {
		return false
	}
	for _, id := range left {
		if !contains(right, id) {
			return false
		}
	}
	return true
}
