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

package chop

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// TestErrInsecureKubeconfigRejected_IsSentinel verifies the K8s-insecure-gate
// sentinel error is exported and errors.Is-compatible. Callers (and a future
// startup-test) need to identify this specific failure mode without parsing
// the message string.
func TestErrInsecureKubeconfigRejected_IsSentinel(t *testing.T) {
	require.NotNil(t, ErrInsecureKubeconfigRejected)
	require.True(t, errors.Is(ErrInsecureKubeconfigRejected, ErrInsecureKubeconfigRejected))
	// Sentinel must NOT match an unrelated error.
	require.False(t, errors.Is(errors.New("unrelated"), ErrInsecureKubeconfigRejected))
}

// TestK8sInsecureGate_Policy tables the gate predicate exercised by
// ConfigManager.Init via OperatorConfig.RequiresStrictK8sTLS(). The predicate
// must fire when ANY of these are true:
//   - security.kubernetes.tls.verify is Strict (explicit per-component setting)
//   - security.policy is Enforced (master coercion to Strict)
//   - security.fips.enforced is true (FIPS master switch — also one-way coerces
//     verify to Strict via applyEnforcedHardening)
//
// The gate cannot wait for applyEnforcedHardening to run on the file-based
// config, so the predicate must see both switches directly.
//
// Each case asserts the rejection outcome directly against the inputs, so a
// wrong-cell rewrite (e.g. swapping TLSVerifyStrict and TLSVerifyNone in the
// predicate, or forgetting either the Policy or FIPS branch) fails loudly.
func TestK8sInsecureGate_Policy(t *testing.T) {
	strict := types.NewString(string(api.TLSVerifyStrict))
	none := types.NewString(string(api.TLSVerifyNone))
	enforced := types.NewString(string(api.SecurityPolicyEnforced))
	fipsOn := &api.OperatorConfigSecurityFIPS{Enforced: types.NewStringBool(true)}
	cases := []struct {
		name           string
		kubeInsecure   bool
		verify         *types.String
		policy         *types.String
		fips           *api.OperatorConfigSecurityFIPS
		expectRejected bool
	}{
		{"safe kubeconfig + nil verify + Permissive", false, nil, nil, nil, false},
		{"safe kubeconfig + verify=Strict + Permissive", false, strict, nil, nil, false},
		{"safe kubeconfig + Enforced", false, nil, enforced, nil, false},
		{"safe kubeconfig + FIPS.enforced", false, nil, nil, fipsOn, false},
		{"insecure kubeconfig + nil verify + Permissive (default)", true, nil, nil, nil, false},
		{"insecure kubeconfig + verify=None + Permissive (explicit permit)", true, none, nil, nil, false},
		{"insecure kubeconfig + verify=Strict + Permissive (reject)", true, strict, nil, nil, true},
		{"insecure kubeconfig + nil verify + Enforced (reject — Policy coerces verify to Strict)", true, nil, enforced, nil, true},
		{"insecure kubeconfig + verify=None + Enforced (reject — Policy overrides)", true, none, enforced, nil, true},
		// FIPS branch — applyEnforcedHardening also fires on fips.enforced.
		{"insecure kubeconfig + nil verify + FIPS.enforced (reject — FIPS coerces verify to Strict)", true, nil, nil, fipsOn, true},
		{"insecure kubeconfig + verify=None + FIPS.enforced (reject — FIPS overrides)", true, none, nil, fipsOn, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := &api.OperatorConfig{
				Security: api.OperatorConfigSecurity{
					Kubernetes: &api.ClusterSecurityKubernetes{TLS: &api.ClusterSecurityKubernetesTLS{Verify: c.verify}},
					Policy:     c.policy,
					FIPS:       c.fips,
				},
			}
			rejected := c.kubeInsecure && cfg.RequiresStrictK8sTLS()
			require.Equal(t, c.expectRejected, rejected)
		})
	}
}

// TestFetchSecurityRootCAResolve_ClearOnFailure exercises every branch of the
// chopconf-level rootCASecretRef resolver. The key invariant: every terminal
// path (success OR failure) ends with tls.RootCASecretRef == nil so the CHI
// normalizer doesn't later try to resolve a stale ref against per-CHI namespaces.
func TestFetchSecurityRootCAResolve_ClearOnFailure(t *testing.T) {
	// Convenience builders.
	tlsWithRef := func(name, key, inline string) *api.ClusterSecurityClickHouseTLS {
		return &api.ClusterSecurityClickHouseTLS{
			RootCA:          inline,
			RootCASecretRef: &core.SecretKeySelector{LocalObjectReference: core.LocalObjectReference{Name: name}, Key: key},
		}
	}
	fakeGet := func(want map[string][]byte, err error) secretDataGetter {
		return func(ns, name string) (map[string][]byte, error) {
			return want, err
		}
	}

	tests := []struct {
		name       string
		tls        *api.ClusterSecurityClickHouseTLS
		operatorNs string
		get        secretDataGetter
		wantRootCA string
		wantRefNil bool
	}{
		{
			name:       "nil tls — no-op",
			tls:        nil,
			operatorNs: "op-ns",
			wantRefNil: true, // n/a; tls is nil
		},
		{
			name:       "no ref set — no-op",
			tls:        &api.ClusterSecurityClickHouseTLS{},
			operatorNs: "op-ns",
			wantRefNil: true,
		},
		{
			name:       "empty Name sentinel — clear without warning",
			tls:        tlsWithRef("", "", ""),
			operatorNs: "op-ns",
			wantRefNil: true,
		},
		{
			name:       "nil getter — clear (defensive guard against panic)",
			tls:        tlsWithRef("my-ca", "", ""),
			operatorNs: "op-ns",
			get:        nil,
			wantRefNil: true,
		},
		{
			name:       "inline rootCA + ref set — inline wins, ref cleared",
			tls:        tlsWithRef("my-ca", "", "INLINE-PEM"),
			operatorNs: "op-ns",
			wantRootCA: "INLINE-PEM",
			wantRefNil: true,
		},
		{
			name:       "operator namespace unknown — clear",
			tls:        tlsWithRef("my-ca", "", ""),
			operatorNs: "",
			wantRefNil: true,
		},
		{
			name:       "fetch error — clear",
			tls:        tlsWithRef("my-ca", "", ""),
			operatorNs: "op-ns",
			get:        fakeGet(nil, errors.New("not found")),
			wantRefNil: true,
		},
		{
			name:       "success ca.crt fallback (Key empty)",
			tls:        tlsWithRef("my-ca", "", ""),
			operatorNs: "op-ns",
			get:        fakeGet(map[string][]byte{"ca.crt": []byte("CA-PEM")}, nil),
			wantRootCA: "CA-PEM",
			wantRefNil: true,
		},
		{
			name:       "success tls.crt fallback (Key empty, no ca.crt)",
			tls:        tlsWithRef("my-ca", "", ""),
			operatorNs: "op-ns",
			get:        fakeGet(map[string][]byte{"tls.crt": []byte("TLS-PEM")}, nil),
			wantRootCA: "TLS-PEM",
			wantRefNil: true,
		},
		{
			name:       "success explicit Key",
			tls:        tlsWithRef("my-ca", "custom", ""),
			operatorNs: "op-ns",
			get:        fakeGet(map[string][]byte{"custom": []byte("CUSTOM-PEM"), "ca.crt": []byte("WRONG")}, nil),
			wantRootCA: "CUSTOM-PEM",
			wantRefNil: true,
		},
		{
			name:       "key missing — clear",
			tls:        tlsWithRef("my-ca", "", ""),
			operatorNs: "op-ns",
			get:        fakeGet(map[string][]byte{"other": []byte("...")}, nil),
			wantRefNil: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fetchSecurityRootCAResolve(tc.tls, tc.operatorNs, tc.get)
			if tc.tls == nil {
				return
			}
			require.Equal(t, tc.wantRootCA, tc.tls.RootCA, "rootCA")
			if tc.wantRefNil {
				require.Nil(t, tc.tls.RootCASecretRef, "ref must be cleared")
			}
		})
	}
}
