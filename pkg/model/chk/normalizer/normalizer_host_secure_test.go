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

package normalizer

import (
	"testing"

	"github.com/stretchr/testify/require"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// TestHostEnsurePortValuesFromSettings_SecureGate pins the byte-stability
// contract for the settings-driven secure-port harvest: when a CHK has
// `keeper_server/tcp_port_secure` set in spec.configuration.settings but no
// host or cluster opts into secure mode, the operator MUST NOT propagate the
// secure port onto host.ZKPortSecure. Without this gate the per-CR Service
// would silently surface zk-secure:2281 for non-adopters and break their
// upgrade path.
//
// host.IsSecure() short-circuits on host.Secure.HasValue() before consulting
// the cluster (cf. type_host.go:499-516), so we set host.Secure directly to
// decouple the test from CHK runtime back-pointer wiring.
func TestHostEnsurePortValuesFromSettings_SecureGate(t *testing.T) {
	// Use a non-default port (32281) to distinguish "setting was harvested"
	// from "default fallback was applied" — otherwise a regression that drops
	// the setting and falls through to the 2281 default would still pass.
	const customSecurePort = int32(32281)
	secureSetting := chi.NewSettings().Set(
		"keeper_server/tcp_port_secure",
		chi.MustNewSettingScalarFromAny(customSecurePort),
	)

	t.Run("insecure host ignores stray tcp_port_secure setting", func(t *testing.T) {
		h := &chi.Host{HostSecure: chi.HostSecure{Secure: types.NewStringBool(false)}}
		hostEnsurePortValuesFromSettings(h, secureSetting, true)
		require.False(t, h.ZKPortSecure.HasValue(),
			"non-secure host must not adopt stray tcp_port_secure from settings; got %v", h.ZKPortSecure)
		require.True(t, h.ZKPort.HasValue(), "plain ZKPort must still be populated by fallback")
		require.Equal(t, int32(chi.KpDefaultZKPortNumber), h.ZKPort.Value())
	})

	t.Run("secure host adopts tcp_port_secure setting", func(t *testing.T) {
		h := &chi.Host{HostSecure: chi.HostSecure{Secure: types.NewStringBool(true)}}
		hostEnsurePortValuesFromSettings(h, secureSetting, true)
		require.True(t, h.ZKPortSecure.HasValue(),
			"secure host must adopt tcp_port_secure from settings")
		require.Equal(t, customSecurePort, h.ZKPortSecure.Value(),
			"secure host must adopt the SETTING value, not the default fallback")
	})

	t.Run("secure host without setting falls back to default 2281", func(t *testing.T) {
		h := &chi.Host{HostSecure: chi.HostSecure{Secure: types.NewStringBool(true)}}
		hostEnsurePortValuesFromSettings(h, chi.NewSettings(), true)
		require.True(t, h.ZKPortSecure.HasValue())
		require.Equal(t, int32(chi.KpDefaultZKSecurePortNumber), h.ZKPortSecure.Value())
	})

	t.Run("two-pass: host-settings (final=false) then common-settings (final=true)", func(t *testing.T) {
		// Production calls the function twice — once with the host's own
		// settings and final=false, then with the cluster's common settings
		// and final=true. The first pass may or may not populate ZKPortSecure;
		// the second pass MUST converge to the correct final state.
		h := &chi.Host{HostSecure: chi.HostSecure{Secure: types.NewStringBool(true)}}
		hostEnsurePortValuesFromSettings(h, secureSetting, false)    // host-personal
		hostEnsurePortValuesFromSettings(h, chi.NewSettings(), true) // common
		require.True(t, h.ZKPortSecure.HasValue(),
			"after two-pass the host-personal setting must persist into the final state")
		require.Equal(t, customSecurePort, h.ZKPortSecure.Value())
	})

	t.Run("invariant: non-secure host with explicit ZKPortSecure is cleared on final pass", func(t *testing.T) {
		// User-supplied per-host `zkPortSecure` on a non-secure cluster
		// must not leak onto the Service. The final pass enforces the
		// invariant ZKPortSecure.HasValue() iff IsSecure().
		h := &chi.Host{
			HostSecure: chi.HostSecure{Secure: types.NewStringBool(false)},
			HostPorts:  chi.HostPorts{ZKPortSecure: types.NewInt32(customSecurePort)},
		}
		hostEnsurePortValuesFromSettings(h, chi.NewSettings(), true)
		require.False(t, h.ZKPortSecure.HasValue(),
			"non-secure host must have ZKPortSecure cleared on final pass; got %v", h.ZKPortSecure)
	})
}
