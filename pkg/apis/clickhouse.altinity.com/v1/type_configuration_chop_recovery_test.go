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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// TestShouldRecoverAbortedOnPodReady verifies the accessor's behavior across the
// full matrix of possible values for reconcile.recovery.from.aborted.onPodReady.
func TestShouldRecoverAbortedOnPodReady(t *testing.T) {
	tests := []struct {
		name     string
		onReady  *types.String
		expected bool
	}{
		{"nil defaults to retry", nil, true},
		{"empty string defaults to retry", types.NewString(""), true},
		{"retry lowercase", types.NewString("retry"), true},
		{"Retry mixed case", types.NewString("Retry"), true},
		{"RETRY upper case", types.NewString("RETRY"), true},
		{"none lowercase", types.NewString("none"), false},
		{"None mixed case", types.NewString("None"), false},
		{"NONE upper case", types.NewString("NONE"), false},
		{"unknown value treated as no-retry", types.NewString("bogus"), false},
		{"whitespace-only treated as no-retry", types.NewString("  "), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &OperatorConfig{}
			c.Reconcile.Recovery.From.Aborted.OnPodReady = tc.onReady
			require.Equal(t, tc.expected, c.ShouldRecoverAbortedOnPodReady())
		})
	}
}

// TestRecoveryActionConstants documents the stable enum values published in the CRD.
// Changes here would break users' CHOPCONF CRs.
func TestRecoveryActionConstants(t *testing.T) {
	require.Equal(t, "none", RecoveryActionNone)
	require.Equal(t, "retry", RecoveryActionRetry)
}

// TestShouldRecoverCompletedOnPodNotReady verifies the accessor's behavior across the
// full matrix of possible values for reconcile.recovery.from.completed.onPodNotReady.
// Mirrors TestShouldRecoverAbortedOnPodReady so symmetric config keys behave identically.
func TestShouldRecoverCompletedOnPodNotReady(t *testing.T) {
	tests := []struct {
		name        string
		onPodNotRdy *types.String
		expected    bool
	}{
		{"nil defaults to off (opt-in only — destructive recreate)", nil, false},
		{"empty string defaults to off", types.NewString(""), false},
		{"retry lowercase", types.NewString("retry"), true},
		{"Retry mixed case", types.NewString("Retry"), true},
		{"RETRY upper case", types.NewString("RETRY"), true},
		{"none lowercase — opt-out", types.NewString("none"), false},
		{"None mixed case", types.NewString("None"), false},
		{"NONE upper case", types.NewString("NONE"), false},
		{"unknown value treated as no-retry (fail safe)", types.NewString("bogus"), false},
		{"whitespace-only treated as no-retry", types.NewString("  "), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &OperatorConfig{}
			c.Reconcile.Recovery.From.Completed.OnPodNotReady = tc.onPodNotRdy
			require.Equal(t, tc.expected, c.ShouldRecoverCompletedOnPodNotReady())
		})
	}
}

// TestCompletedOnPodNotReadyThreshold verifies the threshold parser. Unparseable,
// empty, and non-positive values must fall back to the package default — operators
// who *really* want to disable the safety net should use onPodNotReady=none, not
// pass a malformed duration.
func TestCompletedOnPodNotReadyThreshold(t *testing.T) {
	tests := []struct {
		name     string
		raw      *types.String
		expected time.Duration
	}{
		{"nil falls back to default", nil, defaultCompletedOnPodNotReadyThreshold},
		{"empty falls back to default", types.NewString(""), defaultCompletedOnPodNotReadyThreshold},
		{"whitespace falls back to default", types.NewString("   "), defaultCompletedOnPodNotReadyThreshold},
		{"unparseable falls back to default", types.NewString("five minutes"), defaultCompletedOnPodNotReadyThreshold},
		{"zero falls back to default (don't accidentally disable)",
			types.NewString("0s"), defaultCompletedOnPodNotReadyThreshold},
		{"negative falls back to default", types.NewString("-30s"), defaultCompletedOnPodNotReadyThreshold},
		{"30 seconds — aggressive", types.NewString("30s"), 30 * time.Second},
		{"5 minutes — the documented default in string form",
			types.NewString("5m"), 5 * time.Minute},
		{"1 hour — conservative", types.NewString("1h"), time.Hour},
		{"complex duration: 1h30m", types.NewString("1h30m"), 90 * time.Minute},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &OperatorConfig{}
			c.Reconcile.Recovery.From.Completed.OnPodNotReadyThreshold = tc.raw
			require.Equal(t, tc.expected, c.CompletedOnPodNotReadyThreshold())
		})
	}
}
