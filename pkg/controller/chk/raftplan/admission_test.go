// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package raftplan

import (
	"reflect"
	"testing"
)

func baseline(desired, ancestor []HostID) Snapshot {
	s := Snapshot{
		DesiredHosts:   append([]HostID(nil), desired...),
		AncestorHosts:  append([]HostID(nil), ancestor...),
		Established:    len(ancestor) != 0,
		PublishedHosts: append([]HostID(nil), ancestor...),
		PublishedKnown: true,
		Hosts:          make(map[HostID]HostState),
	}
	for _, id := range desired {
		s.Hosts[id] = HostState{ID: id, InAncestor: contains(ancestor, id), StatefulSet: PresenceAbsent, ReadyKnown: true}
	}
	for _, id := range ancestor {
		s.Hosts[id] = HostState{ID: id, InAncestor: true, StatefulSet: PresencePresent, ReadyKnown: true, Ready: true}
	}
	return s
}

func setHost(s *Snapshot, id HostID, presence Presence, ready, readyKnown bool) {
	h := s.Hosts[id]
	h.StatefulSet, h.Ready, h.ReadyKnown = presence, ready, readyKnown
	s.Hosts[id] = h
}

func TestPlanAdmission(t *testing.T) {
	ids := []HostID{"h0", "h1", "h2"}
	tests := []struct {
		name    string
		input   func() Snapshot
		include []HostID
		create  []HostID
		blocked HostID
		reason  AdmissionReason
		publish bool
	}{
		{"fresh bootstrap", func() Snapshot { return baseline(ids, nil) }, ids, []HostID{"h0", "h1", "h2"}, "", AdmissionAllowed, true},
		{"1 to 3 start", func() Snapshot { return baseline(ids, ids[:1]) }, ids[:1], []HostID{"h1"}, "", AdmissionAllowed, false},
		{"1 to 3 pending", func() Snapshot {
			s := baseline(ids, ids[:1])
			s.PublishedHosts = ids[:2]
			setHost(&s, "h1", PresencePresent, false, true)
			return s
		}, ids[:2], nil, "h1", WaitingForJoiner, false},
		{"1 to 3 pending with unknown readiness", func() Snapshot {
			s := baseline(ids, ids[:1])
			s.PublishedHosts = ids[:2]
			setHost(&s, "h1", PresencePresent, false, false)
			return s
		}, ids[:2], nil, "h1", WaitingForJoiner, false},
		{"1 to 3 next admit", func() Snapshot {
			s := baseline(ids, ids[:1])
			s.PublishedHosts = ids[:2]
			setHost(&s, "h1", PresencePresent, true, true)
			return s
		}, ids, []HostID{"h2"}, "", AdmissionAllowed, true},
		{"2 to 3", func() Snapshot { return baseline(ids, ids[:2]) }, ids[:2], []HostID{"h2"}, "", AdmissionAllowed, false},
		{"missing published joiner is repaired first", func() Snapshot {
			s := baseline(ids, ids[:1])
			s.PublishedHosts = ids[:2]
			return s
		}, ids[:2], []HostID{"h1"}, "", AdmissionAllowed, false},
		{"unknown sts blocks admission", func() Snapshot {
			s := baseline(ids, ids[:1])
			setHost(&s, "h1", PresenceUnknown, false, false)
			return s
		}, ids[:1], nil, "", ObservationUnknown, false},
		{"unreadable publication blocks admission", func() Snapshot {
			s := baseline(ids, ids[:1])
			s.PublishedKnown = false
			return s
		}, ids[:1], nil, "", ObservationUnknown, false},
		{"stop retains existing publication", func() Snapshot {
			s := baseline(ids, ids[:1])
			s.Stopped = true
			s.PublishedHosts = ids[:2]
			return s
		}, ids[:2], nil, "", CHKStopped, false},
		{"3 to N remains fanout", func() Snapshot {
			all := []HostID{"h0", "h1", "h2", "h3", "h4"}
			return baseline(all, all[:3])
		}, []HostID{"h0", "h1", "h2", "h3", "h4"}, []HostID{"h3", "h4"}, "", AdmissionAllowed, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.input()
			before := cloneSnapshot(s)
			p := PlanAdmission(s)
			if !reflect.DeepEqual(p.IncludedHosts, tc.include) {
				t.Errorf("IncludedHosts=%v, want %v", p.IncludedHosts, tc.include)
			}
			for id := range p.MayCreate {
				if !contains(tc.create, id) {
					t.Errorf("unexpected creation allowance: %s", id)
				}
			}
			for _, id := range tc.create {
				if !p.MayCreate[id] {
					t.Errorf("missing creation allowance: %s", id)
				}
			}
			if p.BlockedBy != tc.blocked || p.Reason != tc.reason || p.ShouldPublish != tc.publish {
				t.Errorf("plan=%+v, want blocked=%q reason=%q publish=%v", p, tc.blocked, tc.reason, tc.publish)
			}
			if !reflect.DeepEqual(s, before) {
				t.Fatal("planner mutated its input")
			}
			if !reflect.DeepEqual(p, PlanAdmission(s)) {
				t.Fatal("planner output changed without new observations")
			}
		})
	}
}

func TestPlanAdmissionDefersPublicationUntilPodStarted(t *testing.T) {
	s := baseline([]HostID{"h0", "h1", "h2"}, []HostID{"h0"})
	setHost(&s, "h1", PresencePresent, false, true)
	notStarted := PlanAdmission(s)
	if contains(notStarted.IncludedHosts, "h1") || notStarted.ShouldPublish || notStarted.MayCreate["h2"] {
		t.Fatalf("existing but unstarted joiner admitted: %+v", notStarted)
	}
	h := s.Hosts["h1"]
	h.StartedKnown, h.PodStarted = true, true
	s.Hosts["h1"] = h
	started := PlanAdmission(s)
	if !contains(started.IncludedHosts, "h1") || !started.ShouldPublish || started.MayCreate["h2"] {
		t.Fatalf("started joiner not published or next voter admitted early: %+v", started)
	}
}

func TestPlanAdmissionPreservesOutOfDesiredPublishedMember(t *testing.T) {
	s := baseline([]HostID{"h0", "h1"}, []HostID{"h0"})
	s.PublishedHosts = []HostID{"h0", "h3"} // scale-down must not be implicit
	plan := PlanAdmission(s)
	if !contains(plan.IncludedHosts, "h3") {
		t.Fatalf("existing published voter dropped: %v", plan.IncludedHosts)
	}
}

func TestPlanAdmissionOneToFiveSerial(t *testing.T) {
	ids := []HostID{"h0", "h1", "h2", "h3", "h4"}
	s := baseline(ids, ids[:1])
	s.PublishedHosts = ids[:2]
	setHost(&s, "h1", PresencePresent, false, true)
	p := PlanAdmission(s)
	if !reflect.DeepEqual(p.IncludedHosts, ids[:2]) || len(p.MayCreate) != 0 || p.BlockedBy != "h1" {
		t.Fatalf("unexpected additional joiner while h1 blocked: %+v", p)
	}
}

func cloneSnapshot(s Snapshot) Snapshot {
	out := s
	out.DesiredHosts = append([]HostID(nil), s.DesiredHosts...)
	out.AncestorHosts = append([]HostID(nil), s.AncestorHosts...)
	out.PublishedHosts = append([]HostID(nil), s.PublishedHosts...)
	out.Hosts = make(map[HostID]HostState, len(s.Hosts))
	for id, state := range s.Hosts {
		out.Hosts[id] = state
	}
	return out
}
