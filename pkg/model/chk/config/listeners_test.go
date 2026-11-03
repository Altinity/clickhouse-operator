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

package config

import (
	"testing"

	"github.com/stretchr/testify/require"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// fakeCR is a minimal ICustomResource stub exercising the WalkHosts-based
// getPlaintextListenerRemoval. Only WalkHosts is needed; the embedded interface
// satisfies the type at compile time and panics on any unexpected call.
type fakeCR struct {
	chi.ICustomResource
	hosts []*chi.Host
}

func (f *fakeCR) WalkHosts(fn func(host *chi.Host) error) []error {
	var errs []error
	for _, h := range f.hosts {
		errs = append(errs, fn(h))
	}
	return errs
}

func secureHost(securePort int32) *chi.Host {
	return &chi.Host{
		HostSecure: chi.HostSecure{
			Insecure: types.NewStringBool(false),
			Secure:   types.NewStringBool(true),
		},
		HostPorts: chi.HostPorts{ZKPortSecure: types.NewInt32(securePort)},
	}
}

// insecureHost mirrors the resolved legacy posture (plaintext exposed, secure
// absent) with explicit personal values, so IsSecure()/IsInsecure()
// short-circuit on the host and do not dereference an unset cluster/runtime
// (the production path always has a cluster; this keeps the unit test
// self-contained).
func insecureHost() *chi.Host {
	return &chi.Host{HostSecure: chi.HostSecure{
		Insecure: types.NewStringBool(true),
		Secure:   types.NewStringBool(false),
	}}
}

// TestGetHostListenersOverride pins the per-host (conf.d) overlay: it opens the
// secure port and MUST NOT carry the plaintext-port removal (which has to live
// in the common dir to win the merge — see getPlaintextListenerRemoval).
func TestGetHostListenersOverride(t *testing.T) {
	g := &Generator{}

	t.Run("legacy default host -> empty (byte-identity on upgrade)", func(t *testing.T) {
		// Resolved legacy posture: IsSecure()==false, IsInsecure()==true.
		require.Equal(t, "", g.getHostListenersOverride(insecureHost()))
	})

	t.Run("nil host -> empty", func(t *testing.T) {
		require.Equal(t, "", g.getHostListenersOverride(nil))
	})

	t.Run("secure host -> opens secure port, no plaintext removal", func(t *testing.T) {
		xml := g.getHostListenersOverride(secureHost(9281))
		require.Contains(t, xml, "<tcp_port_secure>9281</tcp_port_secure>")
		require.NotContains(t, xml, `remove="1"`,
			"the plaintext-port removal must NOT be emitted into the per-host conf.d overlay; it belongs in the common group")
	})

	t.Run("secure host without resolved secure port -> empty", func(t *testing.T) {
		h := &chi.Host{HostSecure: chi.HostSecure{
			Insecure: types.NewStringBool(false),
			Secure:   types.NewStringBool(true),
		}}
		require.Equal(t, "", g.getHostListenersOverride(h))
	})
}

// TestGetPlaintextListenerRemoval pins the common-group (keeper_config.d)
// overlay that deletes the static <tcp_port>2181</tcp_port>. It must fire only
// when EVERY host has its plaintext port closed, and stay empty for any legacy
// or mixed posture so non-adopters keep byte-identical configmaps.
func TestGetPlaintextListenerRemoval(t *testing.T) {
	cases := []struct {
		name      string
		hosts     []*chi.Host
		wantEmpty bool
	}{
		{
			name:      "no hosts -> empty",
			hosts:     nil,
			wantEmpty: true,
		},
		{
			name:      "nil host -> empty (conservative)",
			hosts:     []*chi.Host{nil},
			wantEmpty: true,
		},
		{
			name:      "legacy default host -> empty",
			hosts:     []*chi.Host{insecureHost()},
			wantEmpty: true,
		},
		{
			name:      "all hosts secure (plaintext closed) -> remove emitted",
			hosts:     []*chi.Host{secureHost(9281), secureHost(9281)},
			wantEmpty: false,
		},
		{
			name:      "mixed: one host still insecure -> empty (do not strip)",
			hosts:     []*chi.Host{secureHost(9281), insecureHost()},
			wantEmpty: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := &Generator{cr: &fakeCR{hosts: tc.hosts}}
			xml := g.getPlaintextListenerRemoval()
			if tc.wantEmpty {
				require.Equal(t, "", xml)
				return
			}
			require.Contains(t, xml, `<tcp_port remove="1"/>`)
			require.NotContains(t, xml, "tcp_port_secure",
				"the common removal overlay carries only the plaintext-port deletion")
		})
	}
}
