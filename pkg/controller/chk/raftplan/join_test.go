// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package raftplan

import "testing"

func TestPlanJoinTransitionsAndRecovery(t *testing.T) {
	cases := []struct {
		name     string
		observed JoinObservation
		want     JoinDecision
	}{
		{"unknown STS", JoinObservation{}, JoinDecision{JoinUnknown, JoinWait}},
		{"pending", JoinObservation{StatefulSet: PresenceAbsent}, JoinDecision{JoinPending, JoinCreate}},
		{"unknown startup", JoinObservation{StatefulSet: PresencePresent}, JoinDecision{JoinUnknown, JoinWait}},
		{"image pull", JoinObservation{StatefulSet: PresencePresent, StartedKnown: true}, JoinDecision{JoinStarting, JoinWait}},
		{"unknown publication", JoinObservation{StatefulSet: PresencePresent, StartedKnown: true, Started: true}, JoinDecision{JoinUnknown, JoinWait}},
		{"started, unpublished", JoinObservation{StatefulSet: PresencePresent, StartedKnown: true, Started: true, PublishedKnown: true}, JoinDecision{JoinPublishing, JoinPublish}},
		{"published, not committed", JoinObservation{StatefulSet: PresencePresent, StartedKnown: true, Started: true, PublishedKnown: true, Published: true, CommittedKnown: true}, JoinDecision{JoinJoining, JoinVerify}},
		{"live membership unknown", JoinObservation{StatefulSet: PresencePresent, StartedKnown: true, Started: true, PublishedKnown: true, Published: true}, JoinDecision{JoinJoining, JoinVerify}},
		{"committed, syncing", JoinObservation{StatefulSet: PresencePresent, StartedKnown: true, Started: true, PublishedKnown: true, Published: true, CommittedKnown: true, Committed: true, ReadyKnown: true}, JoinDecision{JoinJoining, JoinWait}},
		{"ready", JoinObservation{StatefulSet: PresencePresent, StartedKnown: true, Started: true, PublishedKnown: true, Published: true, CommittedKnown: true, Committed: true, ReadyKnown: true, Ready: true}, JoinDecision{JoinReady, JoinAdmitNext}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := PlanJoin(tc.observed)
			if got != tc.want || PlanJoin(tc.observed) != got {
				t.Fatalf("PlanJoin(%+v)=%+v, want %+v", tc.observed, got, tc.want)
			}
		})
	}
}
