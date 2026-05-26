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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// TestSecurity_IsEnforced_NilSafe verifies the policy predicate at every
// reachable nil-shape: nil receiver, nil Policy pointer, unset value, explicit
// Permissive, explicit Enforced.
func TestSecurity_IsEnforced_NilSafe(t *testing.T) {
	require.False(t, (*OperatorConfigSecurity)(nil).IsEnforced(), "nil receiver must report false")
	require.False(t, (&OperatorConfigSecurity{}).IsEnforced(), "unset Policy must report false (defaults to Permissive)")
	require.False(t, (&OperatorConfigSecurity{Policy: types.NewString(string(SecurityPolicyPermissive))}).IsEnforced())
	require.True(t, (&OperatorConfigSecurity{Policy: types.NewString(string(SecurityPolicyEnforced))}).IsEnforced())
}

// TestSecurity_GetFIPS_IsEnforced_NilSafe verifies the fips-enforced predicate
// across every reachable nil shape: nil parent receiver, nil FIPS sub-struct,
// nil Enforced StringBool, explicit false, explicit true. Default is FALSE so
// pre-0.27.1 chopconfs (no fips block at all) preserve their behavior.
func TestSecurity_GetFIPS_IsEnforced_NilSafe(t *testing.T) {
	// Nil parent receiver — GetFIPS returns nil, IsEnforced on nil reports false.
	require.False(t, (*OperatorConfigSecurity)(nil).GetFIPS().IsEnforced(), "nil parent receiver must report false")
	// Parent set, FIPS sub-struct unset — IsEnforced on nil receiver reports false.
	require.False(t, (&OperatorConfigSecurity{}).GetFIPS().IsEnforced(), "nil FIPS sub-struct must report false")
	// FIPS sub-struct allocated, Enforced unset — must default to false (back-compat).
	require.False(t, (&OperatorConfigSecurity{FIPS: &OperatorConfigSecurityFIPS{}}).GetFIPS().IsEnforced(), "unset Enforced must default to false")
	// FIPS.Enforced explicitly false — false.
	require.False(t, (&OperatorConfigSecurity{FIPS: &OperatorConfigSecurityFIPS{Enforced: types.NewStringBool(false)}}).GetFIPS().IsEnforced())
	// FIPS.Enforced explicitly true — true. This is the one cell that triggers
	// the FIPS-runtime gate in cmd/.../app/fips_gate.go.
	require.True(t, (&OperatorConfigSecurity{FIPS: &OperatorConfigSecurityFIPS{Enforced: types.NewStringBool(true)}}).GetFIPS().IsEnforced())
}

// TestSecurity_RequiresHardening_NilSafe verifies the union accessor used to
// gate per-CR security checks (plain-text ZK rejection, FIPS-bypass rejection,
// ZK digest-auth rejection). Fires when EITHER security.policy=Enforced OR
// security.fips.enforced=true — confirming the orthogonal switches both
// independently and jointly tighten the per-CR posture. Without this either-
// switch union the narrower fips.enforced=true would be strictly weaker than
// the broader policy=Enforced at the per-CR level.
func TestSecurity_RequiresHardening_NilSafe(t *testing.T) {
	require.False(t, (*OperatorConfigSecurity)(nil).RequiresHardening(), "nil receiver must report false")
	require.False(t, (&OperatorConfigSecurity{}).RequiresHardening(), "neither switch set must report false")

	// Policy alone — true.
	require.True(t,
		(&OperatorConfigSecurity{Policy: types.NewString(string(SecurityPolicyEnforced))}).RequiresHardening(),
		"policy=Enforced alone must report true")
	// fips.enforced alone — true. This is the regression the accessor closes:
	// before its introduction, four downstream gates checked only IsEnforced(),
	// so fips.enforced=true silently kept digest-auth + plain-text-ZK paths open.
	require.True(t,
		(&OperatorConfigSecurity{FIPS: &OperatorConfigSecurityFIPS{Enforced: types.NewStringBool(true)}}).RequiresHardening(),
		"fips.enforced=true alone must report true")
	// Both — true. Idempotent union semantics.
	require.True(t,
		(&OperatorConfigSecurity{
			Policy: types.NewString(string(SecurityPolicyEnforced)),
			FIPS:   &OperatorConfigSecurityFIPS{Enforced: types.NewStringBool(true)},
		}).RequiresHardening())
	// Explicit Permissive + Enforced=false — false (degenerate case).
	require.False(t,
		(&OperatorConfigSecurity{
			Policy: types.NewString(string(SecurityPolicyPermissive)),
			FIPS:   &OperatorConfigSecurityFIPS{Enforced: types.NewStringBool(false)},
		}).RequiresHardening())
}

// TestSecurity_GetPolicy_NilSafe verifies the accessor returns the documented
// default for nil-shaped inputs and round-trips known values through normalize.
func TestSecurity_GetPolicy_NilSafe(t *testing.T) {
	require.Equal(t, SecurityPolicyPermissive, (*OperatorConfigSecurity)(nil).GetPolicy(), "nil receiver must default to Permissive")
	require.Equal(t, SecurityPolicyPermissive, (&OperatorConfigSecurity{}).GetPolicy(), "unset Policy must default to Permissive")

	s := &OperatorConfigSecurity{Policy: types.NewString("ENFORCED")}
	require.Equal(t, SecurityPolicyEnforced, s.GetPolicy(), "case-insensitive normalize must map ENFORCED → Enforced")
}

// TestApplyFIPSStrict_NoOpWhenDisabled is the zero-regression guard. With
// security.policy unset/Permissive AND security.fips.enforced unset/false,
// normalize() must not touch any per-component knob — that is the entire
// back-compat contract. Both switches must be off for the no-op behavior;
// EITHER one being on triggers coercion (covered in CoerceTable).
func TestApplyFIPSStrict_NoOpWhenDisabled(t *testing.T) {
	cases := []struct {
		name string
		s    OperatorConfigSecurity
	}{
		{"unset Policy / unset FIPS", OperatorConfigSecurity{}},
		{"Policy=Permissive / unset FIPS", OperatorConfigSecurity{Policy: types.NewString(string(SecurityPolicyPermissive))}},
		{"unset Policy / FIPS.enforced=false (explicit)", OperatorConfigSecurity{FIPS: &OperatorConfigSecurityFIPS{Enforced: types.NewStringBool(false)}}},
		{"Policy=Permissive / FIPS.enforced=false (explicit)", OperatorConfigSecurity{Policy: types.NewString(string(SecurityPolicyPermissive)), FIPS: &OperatorConfigSecurityFIPS{Enforced: types.NewStringBool(false)}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := &OperatorConfig{Security: c.s}
			cfg.applyEnforcedHardening()
			require.Nil(t, cfg.Security.ClickHouse, "ClickHouse sub-struct must remain nil")
			require.Nil(t, cfg.Security.Zookeeper, "Zookeeper sub-struct must remain nil")
			require.Nil(t, cfg.Security.Kubernetes, "Kubernetes sub-struct must remain nil")
			require.Nil(t, cfg.Security.IPC, "IPC must remain nil")
		})
	}
}

// TestApplyFIPSStrict_CoerceTable verifies every per-component knob the master
// switch is supposed to tighten. The 4-axis matrix (policy={off,on} ×
// fips.enforced={off,on}) is covered: coercion fires when EITHER switch is on,
// stays no-op when both are off. Each enabled-row pre-sets the OPPOSITE of the
// strict value for every knob to prove coercion is one-way tightening that
// overrides user-set values too.
func TestApplyFIPSStrict_CoerceTable(t *testing.T) {
	// relaxedSecurity returns a security block with every per-component knob
	// pre-set to the OPPOSITE of the strict target. Caller plugs in Policy /
	// FIPS to choose which switch (or both) drives the coercion.
	relaxedSecurity := func() OperatorConfigSecurity {
		return OperatorConfigSecurity{
			ClickHouse: &ClusterSecurityClickHouse{
				TLS: &ClusterSecurityClickHouseTLS{
					Verify:     types.NewString(string(TLSVerifyNone)),
					MinVersion: types.NewString(string(TLSMinVersion12)),
				},
			},
			Zookeeper: &ClusterSecurityZookeeper{
				TLS: &ClusterSecurityZookeeperTLS{
					Verify:     types.NewString(string(TLSVerifyNone)),
					MinVersion: types.NewString(string(TLSMinVersion12)),
				},
			},
			Kubernetes: &ClusterSecurityKubernetes{
				TLS: &ClusterSecurityKubernetesTLS{
					Verify:     types.NewString(string(TLSVerifyNone)),
					MinVersion: types.NewString(string(TLSMinVersion12)),
				},
			},
			IPC: &OperatorConfigSecurityIPC{Mode: types.NewString(string(IPCModePlain))},
		}
	}

	// 4-axis matrix: every combination of {policy enforced, fips.enforced}.
	// "wantCoerce=true" rows MUST flip every relaxed knob to its strict target;
	// the no-coerce row is the back-compat baseline. The no-op-when-disabled
	// guard is covered separately in TestApplyFIPSStrict_NoOpWhenDisabled.
	cases := []struct {
		name       string
		policy     *types.String
		fips       *OperatorConfigSecurityFIPS
		wantCoerce bool
	}{
		{
			name:       "policy off + fips off → no coercion",
			policy:     nil,
			fips:       nil,
			wantCoerce: false,
		},
		{
			name:       "policy on + fips off → coercion",
			policy:     types.NewString(string(SecurityPolicyEnforced)),
			fips:       nil,
			wantCoerce: true,
		},
		{
			name:       "policy off + fips on → coercion",
			policy:     nil,
			fips:       &OperatorConfigSecurityFIPS{Enforced: types.NewStringBool(true)},
			wantCoerce: true,
		},
		{
			name:       "policy on + fips on → coercion (both switches)",
			policy:     types.NewString(string(SecurityPolicyEnforced)),
			fips:       &OperatorConfigSecurityFIPS{Enforced: types.NewStringBool(true)},
			wantCoerce: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sec := relaxedSecurity()
			sec.Policy = c.policy
			sec.FIPS = c.fips
			cfg := &OperatorConfig{Security: sec}

			cfg.applyEnforcedHardening()

			s := cfg.Security
			if !c.wantCoerce {
				// Polarity guard: the relaxed values must round-trip unchanged.
				require.Equal(t, TLSVerifyNone, s.ClickHouse.GetTLS().GetVerify(), "no coercion expected")
				require.Equal(t, IPCModePlain, s.IPC.GetMode(), "no coercion expected")
				return
			}
			require.Equal(t, TLSVerifyStrict, s.ClickHouse.GetTLS().GetVerify(), "ClickHouse.TLS.Verify must coerce to Strict")
			require.Equal(t, TLSMinVersion13, s.ClickHouse.GetTLS().GetMinVersion(), "ClickHouse.TLS.MinVersion must coerce to 1.3")
			require.Equal(t, TLSVerifyStrict, s.Zookeeper.GetTLS().GetVerify(), "Zookeeper.TLS.Verify must coerce to Strict")
			require.Equal(t, TLSMinVersion13, s.Zookeeper.GetTLS().GetMinVersion(), "Zookeeper.TLS.MinVersion must coerce to 1.3")
			// Assert positive equality with the strict target, not inequality
			// against the relaxed one — a wrong-cell rewrite (Strict → None
			// typo in applyEnforcedHardening) must fail this assertion.
			require.Equal(t, TLSVerifyStrict, s.Kubernetes.GetTLS().GetVerify(), "Kubernetes.TLS.Verify must coerce to Strict")
			require.Equal(t, TLSMinVersion13, s.Kubernetes.GetTLS().GetMinVersion(), "Kubernetes.TLS.MinVersion must coerce to 1.3")
			require.Equal(t, IPCModeSecure, s.IPC.GetMode(), "IPC.Mode must coerce to Secure")
		})
	}
}

// TestApplyFIPSStrict_AllocatesMissingSubstructs covers the case where the
// master switch fires but the user never set any per-component sub-struct —
// applyEnforcedHardening must allocate them so the coerced state is observable
// to downstream code (otherwise a nil TLS sub-struct would silently keep legacy
// behavior). Both Policy=Enforced and FIPS.enforced=true must trigger allocation
// since either alone is sufficient to drive coercion.
func TestApplyFIPSStrict_AllocatesMissingSubstructs(t *testing.T) {
	cases := []struct {
		name string
		s    OperatorConfigSecurity
	}{
		{"Policy=Enforced alone", OperatorConfigSecurity{Policy: types.NewString(string(SecurityPolicyEnforced))}},
		{"FIPS.enforced=true alone", OperatorConfigSecurity{FIPS: &OperatorConfigSecurityFIPS{Enforced: types.NewStringBool(true)}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := &OperatorConfig{Security: c.s}

			cfg.applyEnforcedHardening()

			s := cfg.Security
			require.NotNil(t, s.ClickHouse, "ClickHouse sub-struct must be allocated")
			require.NotNil(t, s.ClickHouse.TLS, "ClickHouse.TLS leaf must be allocated")
			require.Equal(t, TLSVerifyStrict, s.ClickHouse.TLS.GetVerify())
			require.NotNil(t, s.Zookeeper)
			require.NotNil(t, s.Zookeeper.TLS)
			require.NotNil(t, s.Kubernetes)
			require.NotNil(t, s.Kubernetes.TLS, "Kubernetes.TLS leaf must be allocated")
			require.Equal(t, TLSVerifyStrict, s.Kubernetes.TLS.GetVerify())
			require.Equal(t, TLSMinVersion13, s.Kubernetes.TLS.GetMinVersion())
			require.NotNil(t, s.IPC)
			require.Equal(t, IPCModeSecure, s.IPC.GetMode())
		})
	}
}

// TestApplyFIPSStrict_CoercesHTTPSchemeUnderFIPS pins the HTTP-scheme
// coercion in applyEnforcedHardening. The rule is asymmetric on purpose: only
// an explicit "http" is flipped (case-insensitive, since user edits chopconf),
// because `auto` resolves per-host downstream and forcing HTTPS where no TLS
// port exists would break pre-0.27.1 back-compat. With both switches off the
// coercion must not fire at all; with EITHER policy=Enforced OR fips.enforced
// the coercion fires (same gate as the other knobs in applyEnforcedHardening).
func TestApplyFIPSStrict_CoercesHTTPSchemeUnderFIPS(t *testing.T) {
	// trigger captures which of the two switches is on (or both/neither). Both
	// trigger paths must produce the same coercion outcome.
	type trigger struct {
		policyEnforced bool
		fipsEnforced   bool
	}
	tests := []struct {
		name    string
		trigger trigger
		scheme  string
		want    string
	}{
		{name: "Policy=Enforced + http lowercase → coerced to https", trigger: trigger{policyEnforced: true}, scheme: ChSchemeHTTP, want: ChSchemeHTTPS},
		{name: "Policy=Enforced + HTTP uppercase → coerced (EqualFold)", trigger: trigger{policyEnforced: true}, scheme: "HTTP", want: ChSchemeHTTPS},
		{name: "Policy=Enforced + https → unchanged", trigger: trigger{policyEnforced: true}, scheme: ChSchemeHTTPS, want: ChSchemeHTTPS},
		{name: "Policy=Enforced + auto → preserved (back-compat)", trigger: trigger{policyEnforced: true}, scheme: ChSchemeAuto, want: ChSchemeAuto},
		// FIPS.enforced=true alone must also coerce — same gate.
		{name: "FIPS.enforced + http → coerced to https", trigger: trigger{fipsEnforced: true}, scheme: ChSchemeHTTP, want: ChSchemeHTTPS},
		{name: "FIPS.enforced + auto → preserved", trigger: trigger{fipsEnforced: true}, scheme: ChSchemeAuto, want: ChSchemeAuto},
		// Both switches on — outcome must match either alone.
		{name: "Policy=Enforced + FIPS.enforced + http → coerced", trigger: trigger{policyEnforced: true, fipsEnforced: true}, scheme: ChSchemeHTTP, want: ChSchemeHTTPS},
		// Both off — no coercion.
		{name: "Permissive + FIPS off + http → no coercion", trigger: trigger{}, scheme: ChSchemeHTTP, want: ChSchemeHTTP},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &OperatorConfig{}
			cfg.ClickHouse.Access.Scheme = tt.scheme
			if tt.trigger.policyEnforced {
				cfg.Security.Policy = types.NewString(string(SecurityPolicyEnforced))
			}
			if tt.trigger.fipsEnforced {
				cfg.Security.FIPS = &OperatorConfigSecurityFIPS{Enforced: types.NewStringBool(true)}
			}
			cfg.applyEnforcedHardening()
			require.Equal(t, tt.want, cfg.ClickHouse.Access.Scheme)
		})
	}
}

// TestStatusReasonFIPSValidationFailed verifies the reason constant value (used
// as a grep target by operators and dashboards) and that ReconcileAbortWithReason
// formats the error stream as `[reason] msg`. The reason MUST appear at the
// START of the error string — the auto-recovery skip in shouldTriggerAutoRecovery
// relies on a HasPrefix match. If the format ever grows a timestamp prefix or
// similar, this assertion fires before the skip silently breaks.
func TestStatusReasonFIPSValidationFailed(t *testing.T) {
	require.Equal(t, "FIPSValidationFailed", StatusReasonFIPSValidationFailed)

	s := &Status{}
	s.ReconcileAbortWithReason(StatusReasonFIPSValidationFailed, "ZooKeeper node zk-0:2181 must have secure=true")

	require.Equal(t, StatusAborted, s.Status)
	require.NotEmpty(t, s.Errors)
	require.True(t, strings.HasPrefix(s.Errors[0], "[FIPSValidationFailed] "),
		"reason tag must be at the START of the error (auto-recovery skip prefix-matches): got %q", s.Errors[0])
	require.Contains(t, s.Errors[0], "ZooKeeper")
}
