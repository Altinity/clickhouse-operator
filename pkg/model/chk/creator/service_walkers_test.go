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

package creator

import (
	"testing"

	"github.com/stretchr/testify/require"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// fakeCR is a minimal ICustomResource stub for exercising the WalkHosts-based
// helpers. We do not need any of the dozens of other interface methods, only
// WalkHosts — so the embedded interface satisfies the type at compile time and
// panics on any unexpected call (which would surface as a missing-method bug
// rather than a silent fallthrough).
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

// TestCRExposesInsecureZK pins the three nil-safety branches the pure
// buildZKServicePorts unit test cannot reach: nil CR, hostless CR, and a CR
// whose WalkHosts visits a nil host pointer. The hostless and nil-CR cases
// return true to preserve legacy emission (the operator defaults insecure ON
// for non-adopters); a host with no ZKPort set contributes "false".
func TestCRExposesInsecureZK(t *testing.T) {
	t.Run("nil CR returns true (legacy default)", func(t *testing.T) {
		require.True(t, crExposesInsecureZK(nil))
	})
	t.Run("hostless CR returns true (legacy default)", func(t *testing.T) {
		require.True(t, crExposesInsecureZK(&fakeCR{}))
	})
	t.Run("WalkHosts visits nil host pointer", func(t *testing.T) {
		// A CR that walks a nil host must not panic. With only a nil host
		// the walker sees no port set, so exposed stays false; the hasHosts
		// branch fires though, so the return is false (NOT the legacy true).
		require.False(t, crExposesInsecureZK(&fakeCR{hosts: []*chi.Host{nil}}))
	})
	t.Run("host with ZKPort and explicit Insecure=true returns true", func(t *testing.T) {
		// Explicit Insecure=true short-circuits IsInsecure() at the host level
		// so the test does not require a wired-up Cluster back-pointer. The
		// "default-true via cluster chain" path is exercised by the e2e suite
		// (a real CHK reconcile wires up GetCluster()).
		h := &chi.Host{
			HostSecure: chi.HostSecure{Insecure: types.NewStringBool(true)},
			HostPorts:  chi.HostPorts{ZKPort: types.NewInt32(2181)},
		}
		require.True(t, crExposesInsecureZK(&fakeCR{hosts: []*chi.Host{h}}))
	})
	t.Run("host without ZKPort returns false (Insecure=true)", func(t *testing.T) {
		h := &chi.Host{HostSecure: chi.HostSecure{Insecure: types.NewStringBool(true)}}
		require.False(t, crExposesInsecureZK(&fakeCR{hosts: []*chi.Host{h}}))
	})
	t.Run("host opts out via Insecure=false drops zk port", func(t *testing.T) {
		// The plaintext zk:2181 must NOT be advertised on the Service when
		// the host (or its cluster) explicitly sets insecure=no, even though
		// host.ZKPort is still populated for loopback probe use.
		h := &chi.Host{
			HostSecure: chi.HostSecure{Insecure: types.NewStringBool(false)},
			HostPorts:  chi.HostPorts{ZKPort: types.NewInt32(2181)},
		}
		require.False(t, crExposesInsecureZK(&fakeCR{hosts: []*chi.Host{h}}))
	})
	t.Run("host with Insecure=true keeps zk port", func(t *testing.T) {
		h := &chi.Host{
			HostSecure: chi.HostSecure{Insecure: types.NewStringBool(true)},
			HostPorts:  chi.HostPorts{ZKPort: types.NewInt32(2181)},
		}
		require.True(t, crExposesInsecureZK(&fakeCR{hosts: []*chi.Host{h}}))
	})
}

// TestCRExposesSecureZK mirrors TestCRExposesInsecureZK but pins the secure-is-
// opt-in semantic: nil/hostless/nil-host all return false (secure must be
// explicitly requested). A host with ZKPortSecure set returns true.
func TestCRExposesSecureZK(t *testing.T) {
	t.Run("nil CR returns false (secure is opt-in)", func(t *testing.T) {
		require.False(t, crExposesSecureZK(nil))
	})
	t.Run("hostless CR returns false (secure is opt-in)", func(t *testing.T) {
		require.False(t, crExposesSecureZK(&fakeCR{}))
	})
	t.Run("WalkHosts visits nil host pointer", func(t *testing.T) {
		require.False(t, crExposesSecureZK(&fakeCR{hosts: []*chi.Host{nil}}))
	})
	t.Run("host with ZKPortSecure set returns true", func(t *testing.T) {
		h := &chi.Host{HostPorts: chi.HostPorts{ZKPortSecure: types.NewInt32(2281)}}
		require.True(t, crExposesSecureZK(&fakeCR{hosts: []*chi.Host{h}}))
	})
	t.Run("host without ZKPortSecure returns false", func(t *testing.T) {
		require.False(t, crExposesSecureZK(&fakeCR{hosts: []*chi.Host{{}}}))
	})
}
