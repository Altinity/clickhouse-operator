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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// pod is a small builder for a Pod with one container and the given readiness flag.
func pod(ready bool) *core.Pod {
	return &core.Pod{
		Status: core.PodStatus{
			ContainerStatuses: []core.ContainerStatus{
				{Name: "clickhouse", Ready: ready},
			},
		},
	}
}

// multiContainerPod is a builder for a Pod with multiple containers, each with
// individually controllable readiness. The pod is "ready" only if all containers are.
// Container names use strconv.Itoa so an arbitrary number of containers is supported.
func multiContainerPod(readiness ...bool) *core.Pod {
	p := &core.Pod{Status: core.PodStatus{}}
	for i, r := range readiness {
		p.Status.ContainerStatuses = append(p.Status.ContainerStatuses, core.ContainerStatus{
			Name:  "c" + strconv.Itoa(i),
			Ready: r,
		})
	}
	return p
}

// TestIsPodNotReadyToReadyTransition verifies the pure transition-detection logic
// used by recoverAbortedReconcileOnPodReady.
func TestIsPodNotReadyToReadyTransition(t *testing.T) {
	tests := []struct {
		name     string
		old, new *core.Pod
		expected bool
	}{
		{"nil old", nil, pod(true), false},
		{"nil new", pod(false), nil, false},
		{"both nil", nil, nil, false},
		{"not ready → ready (the target case)", pod(false), pod(true), true},
		{"ready → ready (no transition)", pod(true), pod(true), false},
		{"ready → not ready (wrong direction)", pod(true), pod(false), false},
		{"not ready → not ready", pod(false), pod(false), false},
		{"multi-container: one not ready → all ready", multiContainerPod(true, false), multiContainerPod(true, true), true},
		{"multi-container: all ready → one not ready", multiContainerPod(true, true), multiContainerPod(false, true), false},
		{"multi-container: all ready → all ready", multiContainerPod(true, true), multiContainerPod(true, true), false},
		// Edge case: PodHasNotReadyContainers returns false for an empty ContainerStatuses
		// slice (zero-length loop). So a pod with no statuses yet is effectively treated as
		// "ready". When the first pod event we see already has ready statuses, the transition
		// does NOT fire — we only react to observed NotReady→Ready flips, not to pods that
		// were already ready when we started observing them.
		{"empty container statuses → ready (startup edge, no transition)", &core.Pod{}, pod(true), false},
		// Many containers — exercises strconv-based naming in the builder.
		{"12-container pod: last flips to ready",
			multiContainerPod(true, true, true, true, true, true, true, true, true, true, true, false),
			multiContainerPod(true, true, true, true, true, true, true, true, true, true, true, true),
			true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isPodNotReadyToReadyTransition(tc.old, tc.new)
			require.Equal(t, tc.expected, got)
		})
	}
}

// TestShouldTriggerAutoRecovery verifies the CR-state gate used by
// recoverAbortedReconcileOnPodReady.
func TestShouldTriggerAutoRecovery(t *testing.T) {
	// Minimal CHI builder. The Status struct has no setter — field is set directly.
	makeCR := func(status string, deleting bool) *api.ClickHouseInstallation {
		cr := &api.ClickHouseInstallation{
			ObjectMeta: meta.ObjectMeta{Name: "chi", Namespace: "ns"},
		}
		cr.EnsureStatus().Status = status
		if deleting {
			now := meta.NewTime(time.Now())
			cr.ObjectMeta.DeletionTimestamp = &now
		}
		return cr
	}

	// withError builds an Aborted CR carrying a reason-tagged latest error.
	withError := func(reason, msg string) *api.ClickHouseInstallation {
		cr := makeCR(api.StatusAborted, false)
		cr.EnsureStatus().Errors = []string{"[" + reason + "] " + msg}
		return cr
	}

	tests := []struct {
		name     string
		cr       *api.ClickHouseInstallation
		expected bool
	}{
		{"nil CR — reject", nil, false},
		{"Aborted, not deleting — accept (the target case)", makeCR(api.StatusAborted, false), true},
		{"Completed — reject (nothing to recover)", makeCR(api.StatusCompleted, false), false},
		{"InProgress — reject (reconcile already running)", makeCR(api.StatusInProgress, false), false},
		{"Terminating — reject", makeCR(api.StatusTerminating, false), false},
		{"Aborted but being deleted — reject", makeCR(api.StatusAborted, true), false},
		{"empty status (fresh CR) — reject", makeCR("", false), false},
		// Normalize-time abort reasons: pod-Ready flips can't recover these.
		{"Aborted with FIPSValidationFailed — reject (spec edit required)",
			withError(api.StatusReasonFIPSValidationFailed, "ZK plain-text rejected"), false},
		{"Aborted with RootCAConflict — reject",
			withError(api.StatusReasonRootCAConflict, "rootCA and rootCASecretRef both set"), false},
		{"Aborted with RootCASecretUnresolved — reject",
			withError(api.StatusReasonRootCASecretUnresolved, "secret/key not found"), false},
		{"Aborted with FIPSImagePolicyViolation — reject",
			withError(api.StatusReasonFIPSImagePolicyViolation, "image lacks fips marker"), false},
		// Generic Aborted with an unrecognized reason tag — still a recovery target.
		{"Aborted with unrecognized reason — accept",
			withError("SomeOtherReason", "generic abort"), true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, shouldTriggerAutoRecovery(tc.cr))
		})
	}
}

// TestIsPodReadyToNotReadyTransition verifies the dual of isPodNotReadyToReadyTransition:
// fires only on Ready→NotReady, mirrors the same nil/edge-case handling.
func TestIsPodReadyToNotReadyTransition(t *testing.T) {
	tests := []struct {
		name     string
		old, new *core.Pod
		expected bool
	}{
		{"nil old", nil, pod(false), false},
		{"nil new", pod(true), nil, false},
		{"both nil", nil, nil, false},
		{"ready → not ready (the target case)", pod(true), pod(false), true},
		{"ready → ready (no transition)", pod(true), pod(true), false},
		{"not ready → ready (wrong direction, handled by sibling)", pod(false), pod(true), false},
		{"not ready → not ready", pod(false), pod(false), false},
		{"multi-container: all ready → one not ready", multiContainerPod(true, true), multiContainerPod(false, true), true},
		{"multi-container: one not ready → all ready", multiContainerPod(true, false), multiContainerPod(true, true), false},
		{"multi-container: all ready → all ready", multiContainerPod(true, true), multiContainerPod(true, true), false},
		{"empty statuses → not ready (fires; empty counts as ready)",
			&core.Pod{}, pod(false), true},
		{"12-container pod: last flips to not ready",
			multiContainerPod(true, true, true, true, true, true, true, true, true, true, true, true),
			multiContainerPod(true, true, true, true, true, true, true, true, true, true, true, false),
			true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isPodReadyToNotReadyTransition(tc.old, tc.new)
			require.Equal(t, tc.expected, got)
		})
	}
}

// TestShouldTriggerStuckHostRecovery verifies the CR-state gate used by
// recoverCompletedReconcileOnPodNotReady.
func TestShouldTriggerStuckHostRecovery(t *testing.T) {
	makeCR := func(status string, deleting bool) *api.ClickHouseInstallation {
		cr := &api.ClickHouseInstallation{
			ObjectMeta: meta.ObjectMeta{Name: "chi", Namespace: "ns"},
		}
		cr.EnsureStatus().Status = status
		if deleting {
			now := meta.NewTime(time.Now())
			cr.ObjectMeta.DeletionTimestamp = &now
		}
		return cr
	}

	tests := []struct {
		name     string
		cr       *api.ClickHouseInstallation
		expected bool
	}{
		{"nil CR — reject", nil, false},
		// The target case: Completed CHI whose host has just regressed.
		{"Completed, not deleting — accept (the target case)", makeCR(api.StatusCompleted, false), true},
		// Aborted is the sibling path's responsibility; firing stuck-host recovery on it
		// would double-enqueue with recoverAbortedReconcileOnPodReady once the pod
		// eventually becomes Ready again.
		{"Aborted — reject (handled by sibling recoverAbortedReconcileOnPodReady path)",
			makeCR(api.StatusAborted, false), false},
		// InProgress means a reconcile is already in flight; let it observe the pod state
		// on its own rather than racing another enqueue.
		{"InProgress — reject (reconcile already running)", makeCR(api.StatusInProgress, false), false},
		{"Terminating — reject", makeCR(api.StatusTerminating, false), false},
		{"Completed but being deleted — reject", makeCR(api.StatusCompleted, true), false},
		// Fresh CR with no status field set yet — happens between Create and the first
		// status update by the operator.
		{"empty status (fresh CR) — reject", makeCR("", false), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, shouldTriggerStuckHostRecovery(tc.cr))
		})
	}
}

// TestStuckHostScheduleDelay verifies the delay computation for the deferred re-enqueue.
// The helper is pure (clock + threshold injected as args), so we can exercise the
// boundary conditions without time-mocking the rest of the controller.
func TestStuckHostScheduleDelay(t *testing.T) {
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)

	// podWithReadyTransition builds a pod whose PodReady condition transitioned at the
	// given offset from "now". Negative offset means the transition is in the past.
	podWithReadyTransition := func(offset time.Duration) *core.Pod {
		return &core.Pod{
			Status: core.PodStatus{
				Conditions: []core.PodCondition{
					{Type: core.PodReady, Status: core.ConditionFalse,
						LastTransitionTime: meta.NewTime(now.Add(offset))},
				},
			},
		}
	}

	tests := []struct {
		name      string
		pod       *core.Pod
		threshold time.Duration
		// expected delay must satisfy lo <= got <= hi (small tolerance for arithmetic).
		expectMin time.Duration
		expectMax time.Duration
	}{
		{
			// Fresh transition: full threshold + small extra padding for apiserver
			// catch-up. With threshold=5m and elapsed=0, delay should be ~5m02s.
			name:      "fresh transition: schedule full threshold + extra",
			pod:       podWithReadyTransition(0),
			threshold: 5 * time.Minute,
			expectMin: 5*time.Minute + stuckHostExtraDelay - time.Second,
			expectMax: 5*time.Minute + stuckHostExtraDelay + time.Second,
		},
		{
			// Already half-elapsed: remaining ~2.5m + extra.
			name:      "half-elapsed: schedule the remainder",
			pod:       podWithReadyTransition(-150 * time.Second),
			threshold: 5 * time.Minute,
			expectMin: 150*time.Second + stuckHostExtraDelay - time.Second,
			expectMax: 150*time.Second + stuckHostExtraDelay + time.Second,
		},
		{
			// Threshold already past at schedule time (e.g. operator restart after
			// long outage): clamp to stuckHostMinDelay rather than firing instantly,
			// so a single quick flap doesn't produce an immediate restart.
			name:      "threshold already past: clamp to minDelay",
			pod:       podWithReadyTransition(-10 * time.Minute),
			threshold: 5 * time.Minute,
			expectMin: stuckHostMinDelay,
			expectMax: stuckHostMinDelay,
		},
		{
			// Nil pod: no LastTransitionTime info → treat as elapsed=0 → full threshold.
			name:      "nil pod: full threshold",
			pod:       nil,
			threshold: 5 * time.Minute,
			expectMin: 5*time.Minute + stuckHostExtraDelay,
			expectMax: 5*time.Minute + stuckHostExtraDelay,
		},
		{
			// Pod has no PodReady condition (very early in lifecycle): elapsed=0.
			name:      "pod missing PodReady condition: full threshold",
			pod:       &core.Pod{Status: core.PodStatus{Conditions: []core.PodCondition{}}},
			threshold: 5 * time.Minute,
			expectMin: 5*time.Minute + stuckHostExtraDelay,
			expectMax: 5*time.Minute + stuckHostExtraDelay,
		},
		{
			// Zero LastTransitionTime (apiserver hasn't stamped it yet): treat as
			// elapsed=0, schedule full threshold.
			name: "zero LastTransitionTime: full threshold",
			pod: &core.Pod{
				Status: core.PodStatus{
					Conditions: []core.PodCondition{
						{Type: core.PodReady, Status: core.ConditionFalse},
					},
				},
			},
			threshold: 5 * time.Minute,
			expectMin: 5*time.Minute + stuckHostExtraDelay,
			expectMax: 5*time.Minute + stuckHostExtraDelay,
		},
		{
			// Threshold smaller than minDelay: minDelay still floors the result.
			name:      "tiny threshold: clamp to minDelay",
			pod:       podWithReadyTransition(0),
			threshold: 1 * time.Second,
			expectMin: stuckHostMinDelay,
			expectMax: stuckHostMinDelay,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := stuckHostScheduleDelay(tc.pod, tc.threshold, now)
			require.GreaterOrEqual(t, got, tc.expectMin, "delay below expected minimum")
			require.LessOrEqual(t, got, tc.expectMax, "delay above expected maximum")
		})
	}
}
