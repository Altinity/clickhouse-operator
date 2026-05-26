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
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// TLSVerify controls peer-certificate verification for outbound TLS connections
// established by the operator (ClickHouse client, ZooKeeper/Keeper client).
// Alias of types.String so existing pointer/Value()/MergeFrom semantics work unchanged.
//
// Valid values: TLSVerifyStrict (default), TLSVerifyNone.
// Case-insensitive at runtime: "Strict" and "strict" both normalize to
// TLSVerifyStrict; "None" and "none" both to TLSVerifyNone.
//
// Codegen note: gengo v2 cannot emit deepcopy for fields typed as a Go alias, so
// struct fields are declared as *types.String (the underlying type) rather than
// *TLSVerify. Both types are interchangeable at the language level.
type TLSVerify = types.String

// TLSMinVersion declares the minimum TLS protocol version the operator will
// negotiate for outbound TLS connections. Same aliasing convention as TLSVerify.
//
// Valid values: TLSMinVersion12 (default — matches Go's default), TLSMinVersion13.
// Case-insensitive at runtime.
type TLSMinVersion = types.String

// IPCMode selects the wire protocol for the operator↔metrics-exporter REST channel
// (`/chi` on port 8888). Same aliasing convention as TLSVerify.
//
// Valid values: IPCModePlain (default — listen on all interfaces, no auth, identical
// to today's behavior), IPCModeSecure (loopback-only callers + require a shared
// bearer token header on every request).
//
// IPC is operator-internal (operator↔exporter run in the same Pod), so this knob
// lives only at the CHOP-config level — there is no per-CHI override.
type IPCMode = types.String

// FIPSImagePolicy gates whether the operator refuses to deploy CRs whose
// ClickHouse / Keeper container images don't carry a FIPS marker. Same
// aliasing convention as TLSVerify.
//
// Valid values: FIPSImagePolicyPermissive ("Permissive", default — any image
// accepted), FIPSImagePolicyRequired ("FIPSRequired" — every CH/Keeper
// container image MUST have the "fips" substring in its tag, AND the running
// binary's `SELECT version()` reply MUST also carry "fips" once the pod is
// Ready).
//
// Operator-process-scoped (CHOP-config only) — there is no CHI/cluster
// override. Orthogonal to security.policy: a deployment may run with
// permissive TLS but strict image policy, or vice versa.
type FIPSImagePolicy = types.String

// SecurityPolicy is the operator-wide master switch governing TLS/IPC
// coercion and per-CR rejection of FIPS-incompatible postures. Replaces the
// previous boolean toggle (never released). Same aliasing convention as
// TLSVerify.
//
// Valid values: SecurityPolicyPermissive (default — preserves 0.27.0
// behavior across every per-component knob) and SecurityPolicyEnforced
// (coerces TLS verify/minVersion to Strict/1.3 across ClickHouse, ZooKeeper
// and Kubernetes clients; coerces IPC to Secure; coerces clickhouse.access.scheme
// http→https; rejects CHIs that reference plain-text external ZooKeeper or
// otherwise downgrade per-cluster security; re-registers the legacy
// ClickHouse TLS config to verifying mode).
//
// Operator-process-scoped (CHOP-config only); no CHI/cluster override.
// Orthogonal to security.images.policy (FIPS image-tag gate).
type SecurityPolicy = types.String

const (
	// TLSVerifyStrict verifies the peer certificate chain and hostname against
	// configured RootCAs / ServerName. The audit-safe default for FIPS deployments.
	TLSVerifyStrict TLSVerify = "Strict"
	// TLSVerifyNone disables peer-certificate and hostname verification (the
	// underlying tls.Config sets InsecureSkipVerify=true). Useful for local debug
	// against self-signed certs; never use in FIPS-compatible production.
	TLSVerifyNone TLSVerify = "None"

	// TLSMinVersion12 floors TLS at 1.2. The Go stdlib default; matches the
	// FIPS 140-3 approved baseline for AES-GCM cipher suites.
	TLSMinVersion12 TLSMinVersion = "1.2"
	// TLSMinVersion13 floors TLS at 1.3. Stricter than the FIPS baseline but
	// preferred where the peer supports it.
	TLSMinVersion13 TLSMinVersion = "1.3"

	// IPCModePlain preserves today's behavior: server binds all interfaces on :8888,
	// no auth. Selected by default to keep upgrades zero-regression.
	IPCModePlain IPCMode = "Plain"
	// IPCModeSecure rejects non-loopback callers at the /chi handler AND requires
	// every request to present an X-CHOP-Token header matching a shared bearer
	// token. The token is a Pod-lifetime random value (32 bytes from crypto/rand,
	// hex-encoded) provisioned by the operator into a shared emptyDir volume.
	IPCModeSecure IPCMode = "Secure"

	// FIPSImagePolicyPermissive accepts any ClickHouse / Keeper image regardless
	// of FIPS marker. Default — preserves today's behavior.
	FIPSImagePolicyPermissive FIPSImagePolicy = "Permissive"
	// FIPSImagePolicyRequired refuses every CR whose CH/Keeper container image
	// lacks the "fips" substring in its tag, AND aborts a running CR whose
	// `SELECT version()` reply lacks "fips" once the pod is Ready.
	// Wire value "FIPSRequired" disambiguates from the older bare "Required"
	// spelling (still accepted by normalizeFIPSImagePolicy as a defensive alias).
	FIPSImagePolicyRequired FIPSImagePolicy = "FIPSRequired"

	// SecurityPolicyPermissive disables all operator-wide coercion. Per-component
	// security knobs retain whatever the user (or chopconf default) set;
	// CHIs/CHKs reconcile regardless of FIPS posture. Default — preserves 0.27.0
	// behavior on upgrade.
	SecurityPolicyPermissive SecurityPolicy = "Permissive"
	// SecurityPolicyEnforced coerces every per-component security toggle to its
	// Strict position at startup, re-registers the legacy ClickHouse TLS config
	// to verifying mode, and aborts CHIs/CHKs that cannot be served in a
	// FIPS-compatible posture (e.g. plain-text external ZooKeeper, cluster-level
	// verify=None overrides). Also coerces clickhouse.access.scheme http→https.
	SecurityPolicyEnforced SecurityPolicy = "Enforced"
)

// NewTLSVerify builds a TLSVerify value from a plain string. Sibling of
// NewHookFailurePolicy — see that function for rationale.
func NewTLSVerify(s string) TLSVerify {
	return TLSVerify(s)
}

// NewTLSMinVersion builds a TLSMinVersion value from a plain string.
func NewTLSMinVersion(s string) TLSMinVersion {
	return TLSMinVersion(s)
}

// NewIPCMode builds an IPCMode value from a plain string.
func NewIPCMode(s string) IPCMode {
	return IPCMode(s)
}

// NewFIPSImagePolicy builds a FIPSImagePolicy value from a plain string.
func NewFIPSImagePolicy(s string) FIPSImagePolicy {
	return FIPSImagePolicy(s)
}

// NewSecurityPolicy builds a SecurityPolicy value from a plain string.
func NewSecurityPolicy(s string) SecurityPolicy {
	return SecurityPolicy(s)
}

// IPCDefaultTokenPath is the default shared-volume path the operator writes the
// IPC token to and the metrics-exporter reads from. Lives here (rather than in
// pkg/chop or pkg/metrics/clickhouse) so both sides can reference the same
// constant without a chop↔clickhouse import cycle.
const IPCDefaultTokenPath = "/etc/clickhouse-operator-ipc/token"

// IPCDefaultBindHost is the loopback default when IPCMode=Secure and BindHost
// is unset. Shared by both the listener and any client that wants to assemble
// the loopback URL without hardcoding the literal.
const IPCDefaultBindHost = "127.0.0.1"

// IPCHeaderToken is the HTTP header carrying the operator↔exporter shared
// bearer token in IPCModeSecure.
const IPCHeaderToken = "X-CHOP-Token"

// normalizeTLSVerify maps an arbitrarily-cased TLSVerify to the normalized
// PascalCase form. Unknown values pass through unchanged so the runtime can
// surface a clear "unknown verify" error rather than silently coercing to the
// default.
func normalizeTLSVerify(v TLSVerify) TLSVerify {
	for _, normalized := range []TLSVerify{
		TLSVerifyStrict,
		TLSVerifyNone,
	} {
		if v.EqualFold(&normalized) {
			return normalized
		}
	}
	return v
}

// normalizeTLSMinVersion maps an arbitrarily-cased TLSMinVersion to normalized form.
func normalizeTLSMinVersion(v TLSMinVersion) TLSMinVersion {
	for _, normalized := range []TLSMinVersion{
		TLSMinVersion12,
		TLSMinVersion13,
	} {
		if v.EqualFold(&normalized) {
			return normalized
		}
	}
	return v
}

// normalizeIPCMode maps an arbitrarily-cased IPCMode to normalized form.
func normalizeIPCMode(m IPCMode) IPCMode {
	for _, normalized := range []IPCMode{
		IPCModePlain,
		IPCModeSecure,
	} {
		if m.EqualFold(&normalized) {
			return normalized
		}
	}
	return m
}

// normalizeFIPSImagePolicy maps an arbitrarily-cased FIPSImagePolicy to normalized form.
// The current wire value for "required" is "FIPSRequired"; the older bare
// "Required" spelling (used by pre-release chopconfs on this branch) is also
// accepted as a defensive alias so a stale config aborts on FIPS-incompatible
// images rather than silently downgrading to Permissive.
func normalizeFIPSImagePolicy(p FIPSImagePolicy) FIPSImagePolicy {
	for _, normalized := range []FIPSImagePolicy{
		FIPSImagePolicyPermissive,
		FIPSImagePolicyRequired,
	} {
		if p.EqualFold(&normalized) {
			return normalized
		}
	}
	// Defensive legacy alias: "required" / "Required" / "REQUIRED" → FIPSImagePolicyRequired.
	legacyRequired := FIPSImagePolicy("Required")
	if p.EqualFold(&legacyRequired) {
		return FIPSImagePolicyRequired
	}
	return p
}

// normalizeSecurityPolicy maps an arbitrarily-cased SecurityPolicy to normalized form.
func normalizeSecurityPolicy(p SecurityPolicy) SecurityPolicy {
	for _, normalized := range []SecurityPolicy{
		SecurityPolicyPermissive,
		SecurityPolicyEnforced,
	} {
		if p.EqualFold(&normalized) {
			return normalized
		}
	}
	return p
}
