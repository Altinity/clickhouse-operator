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

package chi

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestPodIsSustainedNotReady covers the pure post-fetch decision used by
// isPodSustainedNotReady.
func TestPodIsSustainedNotReady(t *testing.T) {
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)

	withReady := func(status core.ConditionStatus, transitionOffset time.Duration) *core.Pod {
		return &core.Pod{
			Status: core.PodStatus{
				Conditions: []core.PodCondition{
					{Type: core.PodReady, Status: status,
						LastTransitionTime: meta.NewTime(now.Add(transitionOffset))},
				},
			},
		}
	}

	tests := []struct {
		name      string
		pod       *core.Pod
		threshold time.Duration
		expected  bool
	}{
		{
			name:      "nil pod — never sustained",
			pod:       nil,
			threshold: 5 * time.Minute,
			expected:  false,
		},
		{
			name:      "zero threshold — feature disabled, never fires",
			pod:       withReady(core.ConditionFalse, -30*time.Minute),
			threshold: 0,
			expected:  false,
		},
		{
			name:      "negative threshold — feature disabled, never fires",
			pod:       withReady(core.ConditionFalse, -30*time.Minute),
			threshold: -1 * time.Second,
			expected:  false,
		},
		{
			name: "no PodReady condition — early lifecycle, never sustained",
			pod: &core.Pod{Status: core.PodStatus{Conditions: []core.PodCondition{
				{Type: core.PodInitialized, Status: core.ConditionTrue,
					LastTransitionTime: meta.NewTime(now.Add(-10 * time.Minute))},
			}}},
			threshold: 5 * time.Minute,
			expected:  false,
		},
		{
			name:      "PodReady=True — not sustained even with old LastTransitionTime",
			pod:       withReady(core.ConditionTrue, -30*time.Minute),
			threshold: 5 * time.Minute,
			expected:  false,
		},
		{
			name:      "PodReady=False but only 1m ago — under threshold (transient)",
			pod:       withReady(core.ConditionFalse, -1*time.Minute),
			threshold: 5 * time.Minute,
			expected:  false,
		},
		{
			name:      "PodReady=False for exactly the threshold — fires (>= semantics)",
			pod:       withReady(core.ConditionFalse, -5*time.Minute),
			threshold: 5 * time.Minute,
			expected:  true,
		},
		{
			name:      "PodReady=False for 26h — the production incident, fires",
			pod:       withReady(core.ConditionFalse, -26*time.Hour),
			threshold: 5 * time.Minute,
			expected:  true,
		},
		{
			name:      "PodReady=Unknown for 10m — treated as not-ready, fires",
			pod:       withReady(core.ConditionUnknown, -10*time.Minute),
			threshold: 5 * time.Minute,
			expected:  true,
		},
		{
			name:      "PodReady=False but LastTransitionTime is zero — conservative, don't fire",
			pod:       &core.Pod{Status: core.PodStatus{Conditions: []core.PodCondition{{Type: core.PodReady, Status: core.ConditionFalse}}}},
			threshold: 5 * time.Minute,
			expected:  false,
		},
		{
			name: "multiple PodReady entries — use first match",
			pod: &core.Pod{Status: core.PodStatus{Conditions: []core.PodCondition{
				{Type: core.PodReady, Status: core.ConditionFalse,
					LastTransitionTime: meta.NewTime(now.Add(-10 * time.Minute))},
				{Type: core.PodReady, Status: core.ConditionTrue,
					LastTransitionTime: meta.NewTime(now)},
			}}},
			threshold: 5 * time.Minute,
			expected:  true,
		},
		{
			name: "PodScheduled present alongside PodReady=False — still fires on Ready",
			pod: &core.Pod{Status: core.PodStatus{Conditions: []core.PodCondition{
				{Type: core.PodScheduled, Status: core.ConditionTrue,
					LastTransitionTime: meta.NewTime(now.Add(-1 * time.Hour))},
				{Type: core.PodReady, Status: core.ConditionFalse,
					LastTransitionTime: meta.NewTime(now.Add(-10 * time.Minute))},
			}}},
			threshold: 5 * time.Minute,
			expected:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, podIsSustainedNotReady(tc.pod, tc.threshold, now))
		})
	}
}

// TestPodIsInKubeletFailureMode locks in the kubelet-recovery filter: any pod whose
// failure mode is already being handled by kubelet (image pull errors, crash loops,
// pending, etc.) must NOT trigger the stuck-host recovery path.
func TestPodIsInKubeletFailureMode(t *testing.T) {
	waitingContainer := func(reason string) *core.Pod {
		return &core.Pod{Status: core.PodStatus{
			Phase: core.PodRunning,
			ContainerStatuses: []core.ContainerStatus{
				{Name: "clickhouse", State: core.ContainerState{
					Waiting: &core.ContainerStateWaiting{Reason: reason},
				}},
			},
		}}
	}
	waitingInit := func(reason string) *core.Pod {
		return &core.Pod{Status: core.PodStatus{
			Phase: core.PodRunning,
			InitContainerStatuses: []core.ContainerStatus{
				{Name: "init", State: core.ContainerState{
					Waiting: &core.ContainerStateWaiting{Reason: reason},
				}},
			},
		}}
	}

	tests := []struct {
		name     string
		pod      *core.Pod
		expected bool
	}{
		{"nil pod", nil, false},
		{"no statuses, running phase", &core.Pod{Status: core.PodStatus{Phase: core.PodRunning}}, false},
		{"Pending phase — scheduler/kubelet handling", &core.Pod{Status: core.PodStatus{Phase: core.PodPending}}, true},
		{"ImagePullBackOff — kubelet handling", waitingContainer("ImagePullBackOff"), true},
		{"ErrImagePull — kubelet handling", waitingContainer("ErrImagePull"), true},
		{"InvalidImageName — kubelet handling", waitingContainer("InvalidImageName"), true},
		{"CrashLoopBackOff — kubelet handling", waitingContainer("CrashLoopBackOff"), true},
		{"CreateContainerError — kubelet handling", waitingContainer("CreateContainerError"), true},
		{"RunContainerError — kubelet handling", waitingContainer("RunContainerError"), true},
		{"ContainerCannotRun — kubelet handling", waitingContainer("ContainerCannotRun"), true},
		{"CreateContainerConfigError — kubelet handling", waitingContainer("CreateContainerConfigError"), true},
		{"init container in ImagePullBackOff — kubelet handling", waitingInit("ImagePullBackOff"), true},
		{"ContainerCreating — transient, not kubelet failure", waitingContainer("ContainerCreating"), false},
		{"PodInitializing — transient, not kubelet failure", waitingContainer("PodInitializing"), false},
		{"running container, no waiting state", &core.Pod{Status: core.PodStatus{
			Phase: core.PodRunning,
			ContainerStatuses: []core.ContainerStatus{
				{Name: "clickhouse", Ready: true, State: core.ContainerState{
					Running: &core.ContainerStateRunning{},
				}},
			},
		}}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, podIsInKubeletFailureMode(tc.pod))
		})
	}
}
