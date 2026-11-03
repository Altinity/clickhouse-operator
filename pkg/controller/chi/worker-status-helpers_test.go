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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
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

// TestPodIsTerminating covers the pure post-fetch decision used by isPodTerminating.
//
// The case that matters is the third one. Deleting a pod stamps deletionTimestamp and nothing
// else: the phase stays Running and kubelet keeps the readiness probe going, so a wedged
// ClickHouse still answering /ping reports Ready throughout. Without this predicate such a host
// counts as its shard's healthy peer and the operator will disrupt its last serving sibling.
func TestPodIsTerminating(t *testing.T) {
	deleting := meta.NewTime(time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC))

	tests := []struct {
		name     string
		pod      *core.Pod
		expected bool
	}{
		{
			name:     "nil pod — nothing to be terminating",
			pod:      nil,
			expected: false,
		},
		{
			name:     "live pod — no deletion timestamp",
			pod:      &core.Pod{},
			expected: false,
		},
		{
			name: "terminating pod still Running and Ready — the wedged-shutdown case",
			pod: &core.Pod{
				ObjectMeta: meta.ObjectMeta{DeletionTimestamp: &deleting},
				Status: core.PodStatus{
					Phase:      core.PodRunning,
					Conditions: []core.PodCondition{{Type: core.PodReady, Status: core.ConditionTrue}},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, podIsTerminating(tt.pod))
		})
	}
}

// statusFakePod serves one fixed pod to every Pod().Get. isHostHealthyForReconcile fetches once
// and reads the result four ways, so a single fixture drives the whole conjunction.
type statusFakePod struct {
	interfaces.IKubePod
	pod *core.Pod
}

func (f *statusFakePod) Get(ctx context.Context, params ...any) (*core.Pod, error) {
	return f.pod, nil
}

// statusFakeKube exposes Pod() only. IKube is embedded as a nil interface, so reaching any other
// accessor panics - a guard that the predicate under test consults nothing else.
type statusFakeKube struct {
	interfaces.IKube
	pod interfaces.IKubePod
}

func (k *statusFakeKube) Pod() interfaces.IKubePod { return k.pod }

// newHealthyPodWorker wires a worker whose Pod().Get always answers with pod, plus the host it is
// asked about. The host needs a CR because IsStopped/IsTroubleshoot dereference it.
func newHealthyPodWorker(pod *core.Pod) (*worker, *api.Host) {
	host := &api.Host{Name: "h0"}
	host.Runtime.SetCR(&api.ClickHouseInstallation{})
	w := &worker{c: &Controller{kube: &statusFakeKube{pod: &statusFakePod{pod: pod}}}}
	return w, host
}

// TestIsHostHealthyForReconcileTreatsTerminatingPodAsUnhealthy pins the WIRING, not just the
// predicate. isHostHealthyForReconcile is what shard-safety consults before disrupting a host's
// sibling, and a terminating pod keeps reporting Running and Ready for as long as its ClickHouse
// stays wedged - deleting a pod only stamps deletionTimestamp, and kubelet keeps the readiness
// probe running even though it stops liveness and startup. Without the terminating conjunct the
// operator counts such a host as its shard's healthy peer and takes down the last serving replica.
//
// The live-pod case is the control: same fixture, same phase, same ready containers, only the
// deletionTimestamp differs - so the terminating case's false cannot come from anywhere else.
func TestIsHostHealthyForReconcileTreatsTerminatingPodAsUnhealthy(t *testing.T) {
	deleting := meta.NewTime(time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC))

	// Running, single container Ready, not crashing - healthy by every other predicate.
	newPod := func(deletionTimestamp *meta.Time) *core.Pod {
		return &core.Pod{
			ObjectMeta: meta.ObjectMeta{DeletionTimestamp: deletionTimestamp},
			Status: core.PodStatus{
				Phase:             core.PodRunning,
				ContainerStatuses: []core.ContainerStatus{{Ready: true}},
			},
		}
	}

	t.Run("live pod is healthy - the control", func(t *testing.T) {
		w, host := newHealthyPodWorker(newPod(nil))
		require.True(t, w.isHostHealthyForReconcile(context.Background(), host))
	})

	t.Run("terminating pod is NOT a healthy peer", func(t *testing.T) {
		w, host := newHealthyPodWorker(newPod(&deleting))
		require.False(t, w.isHostHealthyForReconcile(context.Background(), host),
			"a pod with deletionTimestamp must never count as a shard's healthy peer")
	})
}
