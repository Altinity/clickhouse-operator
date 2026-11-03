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
	core "k8s.io/api/core/v1"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// TestClusterSecurityClickHouseTLSMergeFromFillsEmpty verifies that MergeFrom fills empty
// fields only — the precedent for 3-level inheritance (CHOP-config → CHI → cluster).
func TestClusterSecurityClickHouseTLSMergeFromFillsEmpty(t *testing.T) {
	cluster := &ClusterSecurityClickHouseTLS{
		Verify: types.NewString("Strict"), // cluster-level set
	}
	source := &ClusterSecurityClickHouseTLS{
		Verify:     types.NewString("None"), // should NOT overwrite cluster's "Strict"
		MinVersion: types.NewString("1.3"),  // should fill (cluster's was nil)
		ServerName: "ch.example.com",        // should fill
		RootCA:     "ca-pem",                // should fill
	}
	merged := cluster.MergeFrom(source, MergeTypeFillEmptyValues)
	require.NotNil(t, merged)
	require.Equal(t, "Strict", merged.Verify.Value(), "explicit cluster-level Verify must NOT be overwritten")
	require.Equal(t, "1.3", merged.MinVersion.Value(), "empty MinVersion must fill from source")
	require.Equal(t, "ch.example.com", merged.ServerName)
	require.Equal(t, "ca-pem", merged.RootCA)
}

func TestClusterSecurityClickHouseTLSMergeFromNilReceiver(t *testing.T) {
	var nilTLS *ClusterSecurityClickHouseTLS
	src := &ClusterSecurityClickHouseTLS{Verify: types.NewString("Strict")}
	merged := nilTLS.MergeFrom(src, MergeTypeFillEmptyValues)
	require.NotNil(t, merged, "nil receiver + non-nil src must allocate")
	require.Equal(t, "Strict", merged.Verify.Value())
}

func TestClusterSecurityClickHouseTLSMergeFromNilSource(t *testing.T) {
	original := &ClusterSecurityClickHouseTLS{Verify: types.NewString("None")}
	merged := original.MergeFrom(nil, MergeTypeFillEmptyValues)
	require.Same(t, original, merged, "nil src must return receiver unchanged")
	require.Equal(t, "None", merged.Verify.Value())
}

// TestClusterSecurityNilSafeGetters verifies that the GetX/Get* chain tolerates
// nil receivers — required so we can use `chi.Spec.Security.GetClickHouse().GetTLS()`
// from the normalizer without pyramid nil checks (per the nil-safe-getter-chain
// feedback).
func TestClusterSecurityNilSafeGetters(t *testing.T) {
	var s *ClusterSecurity
	require.Nil(t, s.GetClickHouse())
	require.Nil(t, s.GetZookeeper())

	var ch *ClusterSecurityClickHouse
	require.Nil(t, ch.GetTLS())

	var zoo *ClusterSecurityZookeeper
	require.Nil(t, zoo.GetTLS())

	var t1 *ClusterSecurityClickHouseTLS
	require.Equal(t, "", string(t1.GetVerify()))
	require.Equal(t, "", string(t1.GetMinVersion()))
	require.Equal(t, "", t1.GetServerName())
	require.Equal(t, "", t1.GetRootCA())

	var z *ClusterSecurityZookeeperTLS
	require.Equal(t, "", string(z.GetVerify()))
	require.Equal(t, "", string(z.GetMinVersion()))
}

// TestClusterSecurityKubernetesTLSVerifyDefault locks in the back-compat
// invariant: nil/empty defaults to permissive (the gate does NOT fire on
// startup with no security block). Polarity discipline: assert positive
// equality with each enumerated value, not inequality against the relaxed
// value, so a wrong-cell rewrite fails loudly.
func TestClusterSecurityKubernetesTLSVerifyDefault(t *testing.T) {
	// nil leaf → empty (permissive default — kubeconfig wins)
	require.Equal(t, TLSVerify(""), (&ClusterSecurityKubernetesTLS{}).GetVerify())
	// Explicit Strict — refuses insecure kubeconfig at startup
	strict := types.NewString(string(TLSVerifyStrict))
	require.Equal(t, TLSVerifyStrict, (&ClusterSecurityKubernetesTLS{Verify: strict}).GetVerify())
	// Explicit None — explicit opt-in to permit insecure kubeconfig
	none := types.NewString(string(TLSVerifyNone))
	require.Equal(t, TLSVerifyNone, (&ClusterSecurityKubernetesTLS{Verify: none}).GetVerify())
}

func TestOperatorConfigIPCMode(t *testing.T) {
	var nilIPC *OperatorConfigSecurityIPC
	require.Equal(t, string(IPCModePlain), string(nilIPC.GetMode()), "nil IPC defaults to Plain")

	plainIPC := &OperatorConfigSecurityIPC{Mode: types.NewString("Plain")}
	require.Equal(t, string(IPCModePlain), string(plainIPC.GetMode()))

	secureIPC := &OperatorConfigSecurityIPC{Mode: types.NewString("secure")} // case-insensitive
	require.Equal(t, string(IPCModeSecure), string(secureIPC.GetMode()))
}

// TestClusterSecurityClickHouseTLSMergeFromAtomicRootCAPair pins the invariant
// that RootCA (inline PEM) and RootCASecretRef (Secret pointer) propagate
// through MergeFrom as an atomic pair. Mixing inline-from-one-level with
// ref-from-another would trip the RootCAConflict gate at normalize time, so
// MergeFrom must never produce that mixed state.
func TestClusterSecurityClickHouseTLSMergeFromAtomicRootCAPair(t *testing.T) {
	secretRef := func(name string) *core.SecretKeySelector {
		return &core.SecretKeySelector{
			LocalObjectReference: core.LocalObjectReference{Name: name},
			Key:                  "ca.crt",
		}
	}

	t.Run("FillEmpty: receiver has inline, src has ref — neither inherited", func(t *testing.T) {
		dst := &ClusterSecurityClickHouseTLS{RootCA: "dst-inline"}
		src := &ClusterSecurityClickHouseTLS{RootCASecretRef: secretRef("src-secret")}
		merged := dst.MergeFrom(src, MergeTypeFillEmptyValues)
		require.Equal(t, "dst-inline", merged.RootCA, "receiver inline must be preserved")
		require.Nil(t, merged.RootCASecretRef, "src ref must NOT be inherited when receiver already has inline")
	})

	t.Run("FillEmpty: receiver has ref, src has inline — neither inherited", func(t *testing.T) {
		dst := &ClusterSecurityClickHouseTLS{RootCASecretRef: secretRef("dst-secret")}
		src := &ClusterSecurityClickHouseTLS{RootCA: "src-inline"}
		merged := dst.MergeFrom(src, MergeTypeFillEmptyValues)
		require.Equal(t, "", merged.RootCA, "src inline must NOT be inherited when receiver already has ref")
		require.NotNil(t, merged.RootCASecretRef)
		require.Equal(t, "dst-secret", merged.RootCASecretRef.Name, "receiver ref must be preserved")
	})

	t.Run("FillEmpty: receiver empty, src has inline — inline inherited", func(t *testing.T) {
		dst := &ClusterSecurityClickHouseTLS{}
		src := &ClusterSecurityClickHouseTLS{RootCA: "src-inline"}
		merged := dst.MergeFrom(src, MergeTypeFillEmptyValues)
		require.Equal(t, "src-inline", merged.RootCA)
		require.Nil(t, merged.RootCASecretRef)
	})

	t.Run("FillEmpty: receiver empty, src has ref — ref inherited", func(t *testing.T) {
		dst := &ClusterSecurityClickHouseTLS{}
		src := &ClusterSecurityClickHouseTLS{RootCASecretRef: secretRef("src-secret")}
		merged := dst.MergeFrom(src, MergeTypeFillEmptyValues)
		require.Equal(t, "", merged.RootCA)
		require.NotNil(t, merged.RootCASecretRef)
		require.Equal(t, "src-secret", merged.RootCASecretRef.Name)
	})

	t.Run("Override: receiver has ref, src has inline — both replaced (ref cleared, inline set)", func(t *testing.T) {
		dst := &ClusterSecurityClickHouseTLS{RootCASecretRef: secretRef("dst-secret")}
		src := &ClusterSecurityClickHouseTLS{RootCA: "src-inline"}
		merged := dst.MergeFrom(src, MergeTypeOverrideByNonEmptyValues)
		require.Equal(t, "src-inline", merged.RootCA, "src inline must replace receiver")
		require.Nil(t, merged.RootCASecretRef, "receiver ref must be cleared atomically")
	})

	t.Run("Override: receiver has inline, src has ref — both replaced (inline cleared, ref set)", func(t *testing.T) {
		dst := &ClusterSecurityClickHouseTLS{RootCA: "dst-inline"}
		src := &ClusterSecurityClickHouseTLS{RootCASecretRef: secretRef("src-secret")}
		merged := dst.MergeFrom(src, MergeTypeOverrideByNonEmptyValues)
		require.Equal(t, "", merged.RootCA, "receiver inline must be cleared atomically")
		require.NotNil(t, merged.RootCASecretRef)
		require.Equal(t, "src-secret", merged.RootCASecretRef.Name)
	})

	t.Run("Override: src has neither — receiver pair unchanged", func(t *testing.T) {
		dst := &ClusterSecurityClickHouseTLS{RootCA: "dst-inline"}
		src := &ClusterSecurityClickHouseTLS{}
		merged := dst.MergeFrom(src, MergeTypeOverrideByNonEmptyValues)
		require.Equal(t, "dst-inline", merged.RootCA, "Override with empty src must not touch receiver")
		require.Nil(t, merged.RootCASecretRef)
	})

	t.Run("FillEmpty: receiver has empty-name sentinel ref — src inline inherited", func(t *testing.T) {
		// rootCASecretRef: {name: ""} is the documented "explicit not-used"
		// sentinel. It must NOT block inheritance of the parent's inline RootCA
		// through the atomic-pair guard, or strict-TLS users that authored the
		// sentinel would silently lose their inherited CA.
		dst := &ClusterSecurityClickHouseTLS{RootCASecretRef: secretRef("")}
		src := &ClusterSecurityClickHouseTLS{RootCA: "parent-inline"}
		merged := dst.MergeFrom(src, MergeTypeFillEmptyValues)
		require.Equal(t, "parent-inline", merged.RootCA, "sentinel ref must not block parent inline inheritance")
	})

	t.Run("Override: src has empty-name sentinel ref — receiver pair unchanged", func(t *testing.T) {
		// Override symmetry: an empty-name ref on src counts as "src has no
		// ref", so the override gate stays closed when src brings only the
		// sentinel. Receiver's inline must survive.
		dst := &ClusterSecurityClickHouseTLS{RootCA: "dst-inline"}
		src := &ClusterSecurityClickHouseTLS{RootCASecretRef: secretRef("")}
		merged := dst.MergeFrom(src, MergeTypeOverrideByNonEmptyValues)
		require.Equal(t, "dst-inline", merged.RootCA, "sentinel-only src must not override receiver inline")
	})
}
