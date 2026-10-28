// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package raftplan contains deterministic, side-effect-free CHK Raft safety decisions.
//
// Inputs are immutable observations supplied by the CHK worker. The package
// intentionally does not import Kubernetes or the CHK API. In the first
// migration step it is evaluated in shadow mode: existing reconciliation
// remains authoritative until integration parity tests have passed.
package raftplan

type HostID string

type Presence uint8

const (
	PresenceUnknown Presence = iota
	PresenceAbsent
	PresencePresent
)

type HostState struct {
	ID HostID

	// InAncestor means present in the last successfully completed topology.
	InAncestor bool

	StatefulSet Presence
	Ready       bool
	ReadyKnown  bool

	// WouldDisrupt is computed by the adapter from the actual proposed STS
	// mutation, including force-recreate and force-restart options.
	WouldDisrupt bool
}

type Snapshot struct {
	// Host IDs in stable Keeper server-ID order, with no duplicates.
	DesiredHosts  []HostID
	AncestorHosts []HostID
	Established   bool
	Stopped       bool

	Hosts map[HostID]HostState

	// PublishedHosts is what the current common ConfigMap contains. These
	// fields let the planner avoid silently dropping a previously published
	// member when a StatefulSet create result is ambiguous. If the ConfigMap
	// could not be read, new membership publication must not be attempted.
	PublishedHosts []HostID
	PublishedKnown bool

	// Same ensemble denominator as quorumSizingEnsemble in the legacy gate.
	Rolling     bool
	MemberCount int
	ReadyCount  int
	CountsKnown bool
}

type AdmissionReason string

const (
	AdmissionAllowed   AdmissionReason = "Allowed"
	WaitingForJoiner   AdmissionReason = "JoinerNotReady"
	CHKStopped         AdmissionReason = "Stopped"
	ObservationUnknown AdmissionReason = "ObservationUnknown"
)

type AdmissionPlan struct {
	// IncludedHosts is the desired published membership, in stable order.
	IncludedHosts []HostID
	// MayCreate controls newly absent StatefulSets, not existing STS repairs.
	MayCreate map[HostID]bool
	BlockedBy HostID
	Reason    AdmissionReason
	// ShouldPublish is true only when the existing publication is known and
	// differs from IncludedHosts. A false value with ObservationUnknown means
	// do not touch publication, not that it already matches.
	ShouldPublish bool
}

type DisruptionReason string

const (
	DisruptionSafe         DisruptionReason = "Safe"
	DisruptionNotNeeded    DisruptionReason = "NotNeeded"
	DisruptionQuorumUnsafe DisruptionReason = "QuorumUnsafe"
	DisruptionStateUnknown DisruptionReason = "StateUnknown"
)

type DisruptionDecision struct {
	Allowed bool
	Reason  DisruptionReason
}
