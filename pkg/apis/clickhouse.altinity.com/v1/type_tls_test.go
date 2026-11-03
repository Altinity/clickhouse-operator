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
)

// TestTLSVerifyNormalization mirrors TestGetFailurePolicyNormalization shape:
// case-insensitive PascalCase normalization, unknown values pass through.
func TestTLSVerifyNormalization(t *testing.T) {
	cases := []struct {
		in   string
		want TLSVerify
	}{
		{"Strict", TLSVerifyStrict},
		{"strict", TLSVerifyStrict},
		{"STRICT", TLSVerifyStrict},
		{"None", TLSVerifyNone},
		{"none", TLSVerifyNone},
		{"NONE", TLSVerifyNone},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got := normalizeTLSVerify(NewTLSVerify(c.in))
			require.Equal(t, string(c.want), string(got))
		})
	}
	t.Run("unknown passes through", func(t *testing.T) {
		got := normalizeTLSVerify(NewTLSVerify("Weird"))
		require.Equal(t, "Weird", string(got))
	})
}

func TestTLSMinVersionNormalization(t *testing.T) {
	require.Equal(t, string(TLSMinVersion12), string(normalizeTLSMinVersion(NewTLSMinVersion("1.2"))))
	require.Equal(t, string(TLSMinVersion13), string(normalizeTLSMinVersion(NewTLSMinVersion("1.3"))))
	require.Equal(t, "1.1", string(normalizeTLSMinVersion(NewTLSMinVersion("1.1")))) // unknown passes through
}

func TestIPCModeNormalization(t *testing.T) {
	require.Equal(t, string(IPCModePlain), string(normalizeIPCMode(NewIPCMode("Plain"))))
	require.Equal(t, string(IPCModePlain), string(normalizeIPCMode(NewIPCMode("plain"))))
	require.Equal(t, string(IPCModeSecure), string(normalizeIPCMode(NewIPCMode("Secure"))))
	require.Equal(t, string(IPCModeSecure), string(normalizeIPCMode(NewIPCMode("SECURE"))))
	require.Equal(t, "Strict", string(normalizeIPCMode(NewIPCMode("Strict")))) // unknown
}

func TestFIPSImagePolicyNormalization(t *testing.T) {
	// Wire value for FIPSImagePolicyRequired is "FIPSRequired" (renamed from
	// the pre-release bare "Required"). normalizeFIPSImagePolicy accepts the
	// new spelling plus a defensive legacy alias for stale chopconfs.
	cases := []struct {
		in   string
		want FIPSImagePolicy
	}{
		{"Permissive", FIPSImagePolicyPermissive},
		{"permissive", FIPSImagePolicyPermissive},
		{"PERMISSIVE", FIPSImagePolicyPermissive},
		// New canonical wire value.
		{"FIPSRequired", FIPSImagePolicyRequired},
		{"fipsrequired", FIPSImagePolicyRequired},
		{"FIPSREQUIRED", FIPSImagePolicyRequired},
		// Defensive legacy alias for pre-release chopconfs — must still resolve
		// to FIPSImagePolicyRequired so stale configs abort on FIPS-incompatible
		// images rather than silently downgrading to Permissive.
		{"Required", FIPSImagePolicyRequired},
		{"required", FIPSImagePolicyRequired},
		{"REQUIRED", FIPSImagePolicyRequired},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got := normalizeFIPSImagePolicy(NewFIPSImagePolicy(c.in))
			require.Equal(t, string(c.want), string(got))
		})
	}
	t.Run("canonical wire value is FIPSRequired", func(t *testing.T) {
		// Pin the wire-value rename so a future revert to "Required" trips this
		// assertion before it silently breaks serialized chopconfs in flight.
		require.Equal(t, "FIPSRequired", string(FIPSImagePolicyRequired))
	})
	t.Run("unknown passes through", func(t *testing.T) {
		got := normalizeFIPSImagePolicy(NewFIPSImagePolicy("Audit"))
		require.Equal(t, "Audit", string(got))
	})
}
