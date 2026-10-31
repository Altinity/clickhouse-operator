// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package raftplan

// JoinPhase is reconstructed from live observations after every reconcile
// or controller restart. It is never persisted in memory or the CHK status.
type JoinPhase string

const (
	JoinUnknown    JoinPhase = "Unknown"
	JoinPending    JoinPhase = "Pending"
	JoinStarting   JoinPhase = "Starting"
	JoinPublishing JoinPhase = "Publishing"
	JoinJoining    JoinPhase = "Joining"
	JoinReady      JoinPhase = "Ready"
)

type JoinAction string

const (
	JoinWait      JoinAction = "Wait"
	JoinCreate    JoinAction = "CreateStatefulSet"
	JoinPublish   JoinAction = "PublishMembership"
	JoinVerify    JoinAction = "VerifyMembership"
	JoinAdmitNext JoinAction = "AdmitNext"
)

type JoinObservation struct {
	StatefulSet    Presence
	StartedKnown   bool
	Started        bool
	PublishedKnown bool
	Published      bool
	CommittedKnown bool
	Committed      bool
	ReadyKnown     bool
	Ready          bool
}

type JoinDecision struct {
	Phase  JoinPhase
	Action JoinAction
}

// PlanJoin permits publication only after Pod Started, and admission of the
// next voter only after committed membership and readiness are observed.
func PlanJoin(s JoinObservation) JoinDecision {
	switch {
	case s.StatefulSet == PresenceUnknown:
		return JoinDecision{JoinUnknown, JoinWait}
	case s.StatefulSet == PresenceAbsent:
		return JoinDecision{JoinPending, JoinCreate}
	case !s.StartedKnown:
		return JoinDecision{JoinUnknown, JoinWait}
	case !s.Started:
		return JoinDecision{JoinStarting, JoinWait}
	case !s.PublishedKnown:
		return JoinDecision{JoinUnknown, JoinWait}
	case !s.Published:
		return JoinDecision{JoinPublishing, JoinPublish}
	case !s.CommittedKnown || !s.Committed:
		return JoinDecision{JoinJoining, JoinVerify}
	case !s.ReadyKnown || !s.Ready:
		return JoinDecision{JoinJoining, JoinWait}
	default:
		return JoinDecision{JoinReady, JoinAdmitNext}
	}
}
