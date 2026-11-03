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

package v1

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// TestHookActionMatchesAnyEvent exercises the matcher that decides whether a hook should
// fire given the events emitted by the reconcile classifier.
func TestHookActionMatchesAnyEvent(t *testing.T) {
	t.Run("nil action does not match", func(t *testing.T) {
		var a *HookAction
		require.False(t, a.MatchesAnyEvent([]HookEvent{HookEventHostUpdate}))
	})

	t.Run("empty On list does not match (invalid input — schema enforces non-empty)", func(t *testing.T) {
		a := &HookAction{}
		require.False(t, a.MatchesAnyEvent([]HookEvent{HookEventHostUpdate}))
	})

	t.Run("Any matches everything, including empty fired set", func(t *testing.T) {
		a := &HookAction{Events: []HookEvent{HookEventAny}}
		require.True(t, a.MatchesAnyEvent(nil))
		require.True(t, a.MatchesAnyEvent([]HookEvent{HookEventHostUpdate}))
		require.True(t, a.MatchesAnyEvent([]HookEvent{HookEventHostShutdown, HookEventHostStop}))
	})

	t.Run("Any matches case-insensitively", func(t *testing.T) {
		// User wrote "any" lowercase in YAML; runtime should still treat it as wildcard.
		a := &HookAction{Events: []HookEvent{"any"}}
		require.True(t, a.MatchesAnyEvent([]HookEvent{HookEventHostUpdate}))
	})

	t.Run("specific event matches case-insensitively", func(t *testing.T) {
		// User wrote "hostupdate" lowercase; should still match the canonical fired event.
		a := &HookAction{Events: []HookEvent{"hostupdate"}}
		require.True(t, a.MatchesAnyEvent([]HookEvent{HookEventHostUpdate}))
		require.False(t, a.MatchesAnyEvent([]HookEvent{HookEventHostCreate}))
	})

	t.Run("specific event matches when fired", func(t *testing.T) {
		a := &HookAction{Events: []HookEvent{HookEventHostCreate}}
		require.True(t, a.MatchesAnyEvent([]HookEvent{HookEventHostCreate}))
		require.False(t, a.MatchesAnyEvent([]HookEvent{HookEventHostUpdate}))
	})

	t.Run("any of several listed events matching is enough", func(t *testing.T) {
		a := &HookAction{Events: []HookEvent{HookEventHostStop, HookEventHostRollout}}
		// Fired set includes HostRollout — second item in On matches.
		require.True(t, a.MatchesAnyEvent([]HookEvent{HookEventHostUpdate, HookEventHostRollout}))
	})

	t.Run("aggregate HostShutdown is just another event — matched if classifier emits it", func(t *testing.T) {
		a := &HookAction{Events: []HookEvent{HookEventHostShutdown}}
		// classifier emits HostShutdown alongside any of Stop/ConfigRestart/Rollout
		require.True(t, a.MatchesAnyEvent([]HookEvent{HookEventHostConfigRestart, HookEventHostShutdown}))
		// no shutdown-class event in the fired set
		require.False(t, a.MatchesAnyEvent([]HookEvent{HookEventHostUpdate}))
	})

	t.Run("no overlap fires nothing", func(t *testing.T) {
		a := &HookAction{Events: []HookEvent{HookEventHostCreate}}
		require.False(t, a.MatchesAnyEvent([]HookEvent{HookEventHostUpdate, HookEventHostStop}))
	})
}

// TestGetTargetNormalization locks in case-insensitive normalization of the Target
// field — users may write "FirstHost" / "firsthost" / "FIRSTHOST" in YAML and the
// runtime must converge them to the same canonical constant so callers can switch
// on the typed value with == comparison.
func TestGetTargetNormalization(t *testing.T) {
	cases := []struct {
		in   string
		want HookTarget
	}{
		{"FirstHost", HookTargetFirstHost},
		{"firstHost", HookTargetFirstHost},
		{"firsthost", HookTargetFirstHost},
		{"FIRSTHOST", HookTargetFirstHost},
		{"AllHosts", HookTargetAllHosts},
		{"allhosts", HookTargetAllHosts},
		{"AllShards", HookTargetAllShards},
		{"allshards", HookTargetAllShards},
	}
	for _, c := range cases {
		c := c
		t.Run("case "+c.in, func(t *testing.T) {
			a := &HookAction{Target: types.NewString(c.in)}
			require.Equal(t, string(c.want), string(a.GetTarget()))
		})
	}

	t.Run("unset Target falls back to FirstHost", func(t *testing.T) {
		a := &HookAction{}
		require.Equal(t, string(HookTargetFirstHost), string(a.GetTarget()))
	})

	t.Run("nil receiver falls back to FirstHost", func(t *testing.T) {
		var a *HookAction
		require.Equal(t, string(HookTargetFirstHost), string(a.GetTarget()))
	})

	t.Run("unknown value passes through unchanged", func(t *testing.T) {
		// normalize is best-effort; an unrecognized value isn't silently coerced
		// to the default, so the runtime can surface a clear "unknown target" error.
		a := &HookAction{Target: types.NewString("Mystery")}
		require.Equal(t, "Mystery", string(a.GetTarget()))
	})
}

// TestGetFailurePolicyNormalization mirrors TestGetTargetNormalization for the
// FailurePolicy field.
func TestGetFailurePolicyNormalization(t *testing.T) {
	cases := []struct {
		in   string
		want HookFailurePolicy
	}{
		{"Fail", HookFailurePolicyFail},
		{"fail", HookFailurePolicyFail},
		{"FAIL", HookFailurePolicyFail},
		{"Ignore", HookFailurePolicyIgnore},
		{"ignore", HookFailurePolicyIgnore},
		{"IGNORE", HookFailurePolicyIgnore},
	}
	for _, c := range cases {
		c := c
		t.Run("case "+c.in, func(t *testing.T) {
			a := &HookAction{FailurePolicy: types.NewString(c.in)}
			require.Equal(t, string(c.want), string(a.GetFailurePolicy()))
		})
	}

	t.Run("unset FailurePolicy falls back to Fail", func(t *testing.T) {
		a := &HookAction{}
		require.Equal(t, string(HookFailurePolicyFail), string(a.GetFailurePolicy()))
	})

	t.Run("ShouldIgnoreFailure honors case-insensitive value", func(t *testing.T) {
		require.True(t, (&HookAction{FailurePolicy: types.NewString("Ignore")}).ShouldIgnoreFailure())
		require.True(t, (&HookAction{FailurePolicy: types.NewString("ignore")}).ShouldIgnoreFailure())
		require.False(t, (&HookAction{FailurePolicy: types.NewString("Fail")}).ShouldIgnoreFailure())
		require.False(t, (&HookAction{FailurePolicy: types.NewString("fail")}).ShouldIgnoreFailure())
		require.False(t, (&HookAction{}).ShouldIgnoreFailure())
	})
}
