// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package raftplan

import "testing"

func TestCheckDisruption(t *testing.T) {
	tests := []struct {
		name         string
		memberCount  int
		readyCount   int
		ready        bool
		readyKnown   bool
		countsKnown  bool
		rolling      bool
		wouldDisrupt bool
		stopped      bool
		wantAllowed  bool
		wantReason   DisruptionReason
	}{
		{"all ready 3 -> allow one", 3, 3, true, true, true, true, true, false, true, DisruptionSafe},
		{"two ready 3 -> refuse", 3, 2, true, true, true, true, true, false, false, DisruptionQuorumUnsafe},
		{"all ready 5 -> allow one", 5, 5, true, true, true, true, true, false, true, DisruptionSafe},
		{"three ready 5 -> refuse", 5, 3, true, true, true, true, true, false, false, DisruptionQuorumUnsafe},
		{"two ready 5, already unready target", 5, 2, false, true, true, true, true, false, true, DisruptionSafe},
		{"existing same", 3, 2, true, true, true, true, false, false, true, DisruptionNotNeeded},
		{"stopped", 3, 2, true, true, true, true, true, true, true, DisruptionNotNeeded},
		{"two-member legacy exception", 2, 2, true, true, true, true, true, false, true, DisruptionSafe},
		{"nonrolling bootstrap", 3, 1, true, true, true, false, true, false, true, DisruptionSafe},
		{"quorum count unknown", 3, 0, true, true, false, true, true, false, false, DisruptionStateUnknown},
		{"target readiness unknown", 3, 3, false, false, true, true, true, false, false, DisruptionStateUnknown},
		{"inconsistent counts unknown", 3, 4, true, true, true, true, true, false, false, DisruptionStateUnknown},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := Snapshot{
				MemberCount: tc.memberCount, ReadyCount: tc.readyCount,
				CountsKnown: tc.countsKnown, Rolling: tc.rolling, Stopped: tc.stopped,
				Hosts: map[HostID]HostState{
					"h0": {ID: "h0", Ready: tc.ready, ReadyKnown: tc.readyKnown, WouldDisrupt: tc.wouldDisrupt},
				},
			}
			got := CheckDisruption(s, "h0")
			if got.Allowed != tc.wantAllowed || got.Reason != tc.wantReason {
				t.Errorf("CheckDisruption=%+v, want allow=%v reason=%s", got, tc.wantAllowed, tc.wantReason)
			}
		})
	}
}

func TestCheckDisruptionMissingHost(t *testing.T) {
	got := CheckDisruption(Snapshot{}, "missing")
	if got.Allowed || got.Reason != DisruptionStateUnknown {
		t.Fatalf("missing host incorrectly allowed: %+v", got)
	}
}
