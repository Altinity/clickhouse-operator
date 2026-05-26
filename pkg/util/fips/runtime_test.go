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

package fips

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRuntime_Stubbable verifies the indirection — production callers see
// crypto/fips140.{Enabled,Enforced}; tests swap the function-pointer pair to
// drive the gate decision matrix without depending on the actual Go toolchain
// FIPS posture (which a unit-test runner cannot control).
func TestRuntime_Stubbable(t *testing.T) {
	origEnabled, origEnforced := Enabled, Enforced
	t.Cleanup(func() { Enabled, Enforced = origEnabled, origEnforced })

	for _, want := range []bool{true, false} {
		Enabled = func() bool { return want }
		Enforced = func() bool { return !want }
		require.Equal(t, want, Enabled(), "Enabled stub")
		require.Equal(t, !want, Enforced(), "Enforced stub")
	}
}

// TestGODEBUGRaw_ReadsEnv confirms the helper returns the GODEBUG env var
// verbatim, including multi-key payloads and empty after unset.
func TestGODEBUGRaw_ReadsEnv(t *testing.T) {
	const payload = "fips140=on,other=foo"
	t.Setenv("GODEBUG", payload)
	require.Equal(t, payload, GODEBUGRaw(), "GODEBUGRaw should mirror env verbatim")

	t.Setenv("GODEBUG", "")
	require.Equal(t, "", GODEBUGRaw(), "GODEBUGRaw should be empty after unset")
}

// TestGODEBUGRaw_EmptyWhenUnset asserts the empty-env case explicitly so
// the banner code can rely on "" meaning "GODEBUG not set".
func TestGODEBUGRaw_EmptyWhenUnset(t *testing.T) {
	t.Setenv("GODEBUG", "")
	require.Equal(t, "", GODEBUGRaw())
}

// TestBuildSetting_ReturnsExistingKey uses GOOS because every Go binary
// (including the test binary) has a GOOS build setting populated by the
// toolchain via runtime/debug.ReadBuildInfo().
func TestBuildSetting_ReturnsExistingKey(t *testing.T) {
	require.NotEmpty(t, BuildSetting("GOOS"), "GOOS build setting must be populated for any Go binary")
}

// TestBuildSetting_EmptyForUnknownKey confirms the absent-key contract —
// banner code uses "" as the "setting not present" signal.
func TestBuildSetting_EmptyForUnknownKey(t *testing.T) {
	require.Equal(t, "", BuildSetting("NotARealSetting"))
}

// TestBuildSetting_GOFIPS140 is diagnostic only: the test toolchain may or
// may not stamp GOFIPS140 depending on how `go test` was invoked. Logging
// the value helps debug FIPS banner output across different test environments.
func TestBuildSetting_GOFIPS140(t *testing.T) {
	t.Logf("GOFIPS140 build setting = %q", BuildSetting("GOFIPS140"))
}

// TestGODEBUGRaw_Mockable confirms the `var func` indirection pattern allows
// tests to swap the helper out for a stub — same seam as Enabled/Enforced.
func TestGODEBUGRaw_Mockable(t *testing.T) {
	orig := GODEBUGRaw
	defer func() { GODEBUGRaw = orig }()
	GODEBUGRaw = func() string { return "test-injected" }
	require.Equal(t, "test-injected", GODEBUGRaw())
}
