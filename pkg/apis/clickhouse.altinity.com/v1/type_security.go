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
	core "k8s.io/api/core/v1"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// ClusterSecurity groups per-cluster security knobs for outbound connections the
// operator establishes on behalf of a ClickHouseInstallation.
//
// All fields default to nil; nil means "fall through to CHOP-config default,
// which itself defaults to current behavior". This guarantees zero regression
// on upgrade for users who do not set any field.
//
// Shape is target-scoped: security.<target>.<protocol>.<knob>, e.g.
//
//	security.clickhouse.tls.verify
//	security.zookeeper.tls.minVersion
//
// The per-target wrappers leave room to add non-TLS knobs later without
// re-shuffling the YAML surface.
//
// Kubernetes-API-client TLS is operator-process-scoped (one kubeconfig per
// operator pod, gated at startup against the file-based chopconf) and therefore
// lives on OperatorConfigSecurity only — sibling pattern with IPC and FIPS.
type ClusterSecurity struct {
	ClickHouse *ClusterSecurityClickHouse `json:"clickhouse,omitempty" yaml:"clickhouse,omitempty"`
	Zookeeper  *ClusterSecurityZookeeper  `json:"zookeeper,omitempty"  yaml:"zookeeper,omitempty"`
}

// ClusterSecurityClickHouse is the wrapper for ClickHouse-target knobs. Today
// it carries only TLS; the wrapper exists so we can grow the surface (e.g.
// authentication policy) without breaking the YAML shape.
type ClusterSecurityClickHouse struct {
	TLS *ClusterSecurityClickHouseTLS `json:"tls,omitempty" yaml:"tls,omitempty"`
}

// ClusterSecurityZookeeper is the wrapper for ZooKeeper / Keeper target knobs.
type ClusterSecurityZookeeper struct {
	TLS *ClusterSecurityZookeeperTLS `json:"tls,omitempty" yaml:"tls,omitempty"`
}

// ClusterSecurityKubernetes is the wrapper for Kubernetes-target knobs.
// Today it carries only TLS; the wrapper exists for shape symmetry with the
// ClickHouse and Zookeeper targets and to leave room for non-TLS knobs.
type ClusterSecurityKubernetes struct {
	TLS *ClusterSecurityKubernetesTLS `json:"tls,omitempty" yaml:"tls,omitempty"`
}

// ClusterSecurityKubernetesTLS holds knobs for the operator's outbound
// Kubernetes API client. The k8s client-go respects whatever's in the
// kubeconfig; the operator never builds the kubeconfig's tls.Config itself,
// so these knobs are evaluated as a LOAD-TIME GATE — the operator refuses
// to start with a kubeconfig that doesn't meet the requested posture.
type ClusterSecurityKubernetesTLS struct {
	// Verify gates startup against the kubeconfig's TLSClientConfig.Insecure
	// field. Valid values are TLSVerifyStrict and TLSVerifyNone.
	//   Strict — refuse a kubeconfig with Insecure=true (fail-fast at startup).
	//   None   — explicitly permit an insecure kubeconfig.
	//   Nil    — preserve current behavior (permissive: kubeconfig wins).
	// Unlike ClickHouse/Zookeeper Verify, Strict here does NOT actively
	// override the kubeconfig; it only refuses to load an insecure one.
	Verify *types.String `json:"verify,omitempty"     yaml:"verify,omitempty"`
	// MinVersion floors TLS at the named protocol version. Valid values are
	// TLSMinVersion12 and TLSMinVersion13. Nil = Go stdlib default.
	//
	// Declared for shape symmetry with ClickHouse/Zookeeper and coerced under
	// FIPS, but not yet enforced on the operator's K8s API transport — a future
	// enhancement will wire it into rest.Config when the operator wraps the
	// kubeconfig with stricter TLS settings.
	MinVersion *types.String `json:"minVersion,omitempty" yaml:"minVersion,omitempty"`
}

// ClusterSecurityClickHouseTLS holds knobs for the operator's outbound
// ClickHouse client TLS connections.
type ClusterSecurityClickHouseTLS struct {
	// Verify gates peer-certificate + hostname verification. Valid values are
	// TLSVerifyStrict and TLSVerifyNone (case-insensitive at apply time).
	// Nil = preserve current behavior (InsecureSkipVerify=true).
	Verify *types.String `json:"verify,omitempty"     yaml:"verify,omitempty"`
	// MinVersion sets the minimum TLS protocol version. Valid values are
	// TLSMinVersion12 and TLSMinVersion13. Nil = Go stdlib default.
	MinVersion *types.String `json:"minVersion,omitempty" yaml:"minVersion,omitempty"`
	// ServerName overrides the default ServerName used in TLS handshake (defaults
	// to the dial host). Useful for certs that don't carry the operator-visible FQDN.
	ServerName string `json:"serverName,omitempty" yaml:"serverName,omitempty"`
	// RootCA carries the inline CA bundle used to validate the ClickHouse server
	// cert when Verify=Strict. Accepts raw PEM or a base64-wrapped PEM payload
	// (auto-detected). Mutually exclusive with RootCASecretRef below — setting
	// both at the same level aborts the CR with reason RootCAConflict.
	//
	// Empty + Verify=Strict falls back to the operator pod's system trust store;
	// if the server cert is not chained to a system-trusted root the handshake
	// will fail with a clear cert error (no silent downgrade).
	RootCA string `json:"rootCA,omitempty"     yaml:"rootCA,omitempty"`
	// RootCASecretRef points to a Kubernetes Secret holding the PEM CA bundle.
	// Mutually exclusive with the inline RootCA above. Empty `name` is the
	// "not used" sentinel — the ref is cleared and no resolution attempted.
	//
	// Key defaulting: when SecretKeyRef.Key is empty, the operator looks for
	// "ca.crt" (cert-manager / kubernetes.io/tls convention) first, then
	// "tls.crt" as a fallback. Set Key explicitly to override.
	//
	// Namespace: SecretKeySelector has no namespace field. Resolution behavior
	// depends on where the ref was authored:
	//   - CHI / cluster level: resolved at CHI-normalize time against the CHI's
	//     own namespace. Failure (Secret missing, key missing, both inline +
	//     ref set) surfaces as CR Aborted with status reason
	//     RootCASecretUnresolved or RootCAConflict respectively. No silent
	//     fallback to system roots, since that would mask a security-posture
	//     regression on the per-CHI path.
	//   - Chopconf level: resolved at operator config-load time against the
	//     operator's pod namespace. On any failure (Secret missing, key
	//     missing, API error, or both inline + ref set) the ref is CLEARED in
	//     memory and a Warning is logged — there is no per-CHI abort, since
	//     the operator could not later determine which namespace the ref
	//     originally referred to after MergeFrom propagation. Fix the Secret
	//     and restart the operator to retry.
	RootCASecretRef *core.SecretKeySelector `json:"rootCASecretRef,omitempty" yaml:"rootCASecretRef,omitempty"`
}

// ClusterSecurityZookeeperTLS holds knobs for the operator's ZooKeeper /
// Keeper client. The existing ZK TLS plumbing already loads cert/key/CA +
// ServerName; only MinVersion is missing today.
type ClusterSecurityZookeeperTLS struct {
	// Verify gates peer-certificate verification. Valid values are TLSVerifyStrict
	// and TLSVerifyNone. Nil = current behavior.
	Verify *types.String `json:"verify,omitempty"     yaml:"verify,omitempty"`
	// MinVersion floors TLS at the named protocol version. Valid values are
	// TLSMinVersion12 and TLSMinVersion13. Nil = Go stdlib default.
	MinVersion *types.String `json:"minVersion,omitempty" yaml:"minVersion,omitempty"`
}

// MergeFrom merges src into c honoring _type. Nil-safe on both receiver and src.
// MergeTypeFillEmptyValues preserves receiver values; MergeTypeOverrideByNonEmptyValues
// overwrites with any field src explicitly sets.
func (c *ClusterSecurityClickHouse) MergeFrom(src *ClusterSecurityClickHouse, _type MergeType) *ClusterSecurityClickHouse {
	if src == nil {
		return c
	}
	if c == nil {
		c = &ClusterSecurityClickHouse{}
	}
	c.TLS = c.TLS.MergeFrom(src.TLS, _type)
	return c
}

// MergeFrom merges src into z honoring _type. Nil-safe on both receiver and src.
// MergeTypeFillEmptyValues preserves receiver values; MergeTypeOverrideByNonEmptyValues
// overwrites with any field src explicitly sets.
func (z *ClusterSecurityZookeeper) MergeFrom(src *ClusterSecurityZookeeper, _type MergeType) *ClusterSecurityZookeeper {
	if src == nil {
		return z
	}
	if z == nil {
		z = &ClusterSecurityZookeeper{}
	}
	z.TLS = z.TLS.MergeFrom(src.TLS, _type)
	return z
}

// MergeFrom merges src into k honoring _type. Nil-safe on both receiver and src.
// MergeTypeFillEmptyValues preserves receiver values; MergeTypeOverrideByNonEmptyValues
// overwrites with any field src explicitly sets.
func (k *ClusterSecurityKubernetes) MergeFrom(src *ClusterSecurityKubernetes, _type MergeType) *ClusterSecurityKubernetes {
	if src == nil {
		return k
	}
	if k == nil {
		k = &ClusterSecurityKubernetes{}
	}
	k.TLS = k.TLS.MergeFrom(src.TLS, _type)
	return k
}

// MergeFrom merges src into t honoring _type. Nil-safe on both receiver and src.
// MergeTypeFillEmptyValues preserves receiver values; MergeTypeOverrideByNonEmptyValues
// overwrites with any field src explicitly sets.
func (t *ClusterSecurityKubernetesTLS) MergeFrom(src *ClusterSecurityKubernetesTLS, _type MergeType) *ClusterSecurityKubernetesTLS {
	if src == nil {
		return t
	}
	if t == nil {
		t = &ClusterSecurityKubernetesTLS{}
	}
	switch _type {
	case MergeTypeFillEmptyValues:
		t.Verify = t.Verify.MergeFrom(src.Verify)
		t.MinVersion = t.MinVersion.MergeFrom(src.MinVersion)
	case MergeTypeOverrideByNonEmptyValues:
		if src.Verify.HasValue() {
			t.Verify = src.Verify
		}
		if src.MinVersion.HasValue() {
			t.MinVersion = src.MinVersion
		}
	}
	return t
}

// MergeFrom merges src into t honoring _type. Nil-safe on both receiver and src.
// MergeTypeFillEmptyValues preserves receiver values; MergeTypeOverrideByNonEmptyValues
// overwrites with any field src explicitly sets.
// isRootCAPairEmpty reports whether neither half of the (RootCA, RootCASecretRef)
// pair is set on tls. An empty-name ref (`rootCASecretRef: {name: ""}`) is the
// documented "explicit not-used" sentinel — it counts as unset here so that an
// atomic-pair MergeFrom (FillEmpty or Override) does not treat a sentinel as a
// blocker for inheritance or as a non-empty source.
func isRootCAPairEmpty(tls *ClusterSecurityClickHouseTLS) bool {
	if tls == nil {
		return true
	}
	if tls.RootCA != "" {
		return false
	}
	if (tls.RootCASecretRef != nil) && (tls.RootCASecretRef.Name != "") {
		return false
	}
	return true
}

func (t *ClusterSecurityClickHouseTLS) MergeFrom(src *ClusterSecurityClickHouseTLS, _type MergeType) *ClusterSecurityClickHouseTLS {
	if src == nil {
		return t
	}
	if t == nil {
		t = &ClusterSecurityClickHouseTLS{}
	}
	switch _type {
	case MergeTypeFillEmptyValues:
		t.Verify = t.Verify.MergeFrom(src.Verify)
		t.MinVersion = t.MinVersion.MergeFrom(src.MinVersion)
		if t.ServerName == "" {
			t.ServerName = src.ServerName
		}
		// RootCA and RootCASecretRef are mutually exclusive — propagate them
		// as an atomic pair. If the receiver has neither, adopt src's pair;
		// otherwise leave receiver alone. Filling each half independently
		// (the prior behavior) could graft src's ref onto a receiver that
		// already had inline RootCA, synthesizing a RootCAConflict on
		// subsequent normalize (ancestor re-normalize via inheritance).
		// An empty-name ref (`rootCASecretRef: {name: ""}`) is the documented
		// "explicit not-used" sentinel — treat it as unset here so a child
		// using the sentinel still inherits the parent's inline RootCA.
		if isRootCAPairEmpty(t) {
			t.RootCA = src.RootCA
			t.RootCASecretRef = src.RootCASecretRef
		}
	case MergeTypeOverrideByNonEmptyValues:
		if src.Verify.HasValue() {
			t.Verify = src.Verify
		}
		if src.MinVersion.HasValue() {
			t.MinVersion = src.MinVersion
		}
		if src.ServerName != "" {
			t.ServerName = src.ServerName
		}
		// Atomic-pair override: if src sets either half, both halves on the
		// receiver are replaced together (the empty side of src clears the
		// receiver's value). Replacing each half independently would let a
		// receiver-inlined RootCA coexist with a src-supplied ref (or vice
		// versa) and trip RootCAConflict downstream. An empty-name ref on
		// src counts as "src has no ref" (sentinel semantics).
		if !isRootCAPairEmpty(src) {
			t.RootCA = src.RootCA
			t.RootCASecretRef = src.RootCASecretRef
		}
	}
	return t
}

// MergeFrom merges src into z honoring _type. Nil-safe on both receiver and src.
// MergeTypeFillEmptyValues preserves receiver values; MergeTypeOverrideByNonEmptyValues
// overwrites with any field src explicitly sets.
func (z *ClusterSecurityZookeeperTLS) MergeFrom(src *ClusterSecurityZookeeperTLS, _type MergeType) *ClusterSecurityZookeeperTLS {
	if src == nil {
		return z
	}
	if z == nil {
		z = &ClusterSecurityZookeeperTLS{}
	}
	switch _type {
	case MergeTypeFillEmptyValues:
		z.Verify = z.Verify.MergeFrom(src.Verify)
		z.MinVersion = z.MinVersion.MergeFrom(src.MinVersion)
	case MergeTypeOverrideByNonEmptyValues:
		if src.Verify.HasValue() {
			z.Verify = src.Verify
		}
		if src.MinVersion.HasValue() {
			z.MinVersion = src.MinVersion
		}
	}
	return z
}

// GetClickHouse returns the ClickHouse sub-struct, nil-safe.
func (s *ClusterSecurity) GetClickHouse() *ClusterSecurityClickHouse {
	if s == nil {
		return nil
	}
	return s.ClickHouse
}

// GetZookeeper returns the Zookeeper sub-struct, nil-safe.
func (s *ClusterSecurity) GetZookeeper() *ClusterSecurityZookeeper {
	if s == nil {
		return nil
	}
	return s.Zookeeper
}

// GetTLS returns the TLS leaf under ClickHouse, nil-safe.
func (c *ClusterSecurityClickHouse) GetTLS() *ClusterSecurityClickHouseTLS {
	if c == nil {
		return nil
	}
	return c.TLS
}

// GetTLS returns the TLS leaf under Zookeeper, nil-safe.
func (z *ClusterSecurityZookeeper) GetTLS() *ClusterSecurityZookeeperTLS {
	if z == nil {
		return nil
	}
	return z.TLS
}

// GetVerify returns the resolved TLSVerify for ClickHouse-client TLS: the explicit
// value if set, otherwise nil (caller decides default). Nil-safe.
func (t *ClusterSecurityClickHouseTLS) GetVerify() TLSVerify {
	if (t == nil) || (t.Verify == nil) || !t.Verify.HasValue() {
		return TLSVerify("")
	}
	return normalizeTLSVerify(NewTLSVerify(t.Verify.Value()))
}

// GetMinVersion returns the resolved TLSMinVersion for ClickHouse-client TLS.
// Nil-safe; returns empty value when unset (caller decides default).
func (t *ClusterSecurityClickHouseTLS) GetMinVersion() TLSMinVersion {
	if (t == nil) || (t.MinVersion == nil) || !t.MinVersion.HasValue() {
		return TLSMinVersion("")
	}
	return normalizeTLSMinVersion(NewTLSMinVersion(t.MinVersion.Value()))
}

// GetServerName returns the configured ServerName, or empty string when unset.
func (t *ClusterSecurityClickHouseTLS) GetServerName() string {
	if t == nil {
		return ""
	}
	return t.ServerName
}

// GetRootCA returns the configured RootCA PEM/base64 payload, or empty string.
func (t *ClusterSecurityClickHouseTLS) GetRootCA() string {
	if t == nil {
		return ""
	}
	return t.RootCA
}

// GetRootCASecretRef returns the configured Secret reference for RootCA, nil-safe.
func (t *ClusterSecurityClickHouseTLS) GetRootCASecretRef() *core.SecretKeySelector {
	if t == nil {
		return nil
	}
	return t.RootCASecretRef
}

// GetVerify returns the resolved TLSVerify for ZK-client TLS.
func (z *ClusterSecurityZookeeperTLS) GetVerify() TLSVerify {
	if (z == nil) || (z.Verify == nil) || !z.Verify.HasValue() {
		return TLSVerify("")
	}
	return normalizeTLSVerify(NewTLSVerify(z.Verify.Value()))
}

// GetMinVersion returns the resolved TLSMinVersion for ZK-client TLS.
// Nil-safe; returns empty value when unset (caller decides default).
func (z *ClusterSecurityZookeeperTLS) GetMinVersion() TLSMinVersion {
	if (z == nil) || (z.MinVersion == nil) || !z.MinVersion.HasValue() {
		return TLSMinVersion("")
	}
	return normalizeTLSMinVersion(NewTLSMinVersion(z.MinVersion.Value()))
}

// GetTLS returns the TLS leaf under Kubernetes, nil-safe.
func (k *ClusterSecurityKubernetes) GetTLS() *ClusterSecurityKubernetesTLS {
	if k == nil {
		return nil
	}
	return k.TLS
}

// GetVerify returns the resolved TLSVerify for the operator's K8s-client gate.
// Nil/unset returns empty value — the gate treats empty as permissive
// (kubeconfig wins) to preserve pre-rename behavior.
func (t *ClusterSecurityKubernetesTLS) GetVerify() TLSVerify {
	if (t == nil) || (t.Verify == nil) || !t.Verify.HasValue() {
		return TLSVerify("")
	}
	return normalizeTLSVerify(NewTLSVerify(t.Verify.Value()))
}

// GetMinVersion returns the resolved TLSMinVersion for the operator's K8s client.
// Nil-safe; returns empty value when unset. Declared for shape consistency and
// FIPS coercion uniformity — not yet wired into the K8s API transport.
func (t *ClusterSecurityKubernetesTLS) GetMinVersion() TLSMinVersion {
	if (t == nil) || (t.MinVersion == nil) || !t.MinVersion.HasValue() {
		return TLSMinVersion("")
	}
	return normalizeTLSMinVersion(NewTLSMinVersion(t.MinVersion.Value()))
}

// OperatorConfigSecurity is the CHOP-config-level security section. ClickHouse
// and Zookeeper sub-structs serve as operator-wide defaults inherited into CHIs
// and clusters (3-level chopconf → CHI spec → cluster). Kubernetes, IPC, and
// FIPS are operator-process-scoped — they have no CHI/cluster override because
// they govern singletons inside the operator pod (the K8s API client, the
// operator↔exporter loopback IPC, and the master FIPS switch).
type OperatorConfigSecurity struct {
	ClickHouse *ClusterSecurityClickHouse    `json:"clickhouse,omitempty" yaml:"clickhouse,omitempty"`
	Zookeeper  *ClusterSecurityZookeeper     `json:"zookeeper,omitempty"  yaml:"zookeeper,omitempty"`
	Kubernetes *ClusterSecurityKubernetes    `json:"kubernetes,omitempty" yaml:"kubernetes,omitempty"`
	IPC        *OperatorConfigSecurityIPC    `json:"ipc,omitempty"        yaml:"ipc,omitempty"`
	Policy     *types.String                 `json:"policy,omitempty"     yaml:"policy,omitempty"`
	Images     *OperatorConfigSecurityImages `json:"images,omitempty"     yaml:"images,omitempty"`
	FIPS       *OperatorConfigSecurityFIPS   `json:"fips,omitempty"       yaml:"fips,omitempty"`
}

// OperatorConfigSecurityImages gates the operator against CHs/Keepers whose
// container images aren't FIPS-built. Orthogonal to Policy — users may run
// permissive TLS but strict image policy, or vice versa.
type OperatorConfigSecurityImages struct {
	// Policy selects Permissive (default) or Required. Valid values are
	// FIPSImagePolicyPermissive and FIPSImagePolicyRequired.
	Policy *types.String `json:"policy,omitempty" yaml:"policy,omitempty"`
}

// OperatorConfigSecurityFIPS controls FIPS cryptographic-module enforcement.
// Orthogonal to security.policy (which governs transport hardening). When
// Enforced is true, the operator Fatals at startup unless the binary was
// built with GOFIPS140 and crypto/fips140 reports Enabled. Strict-runtime
// disagreement (GODEBUG=fips140=only set, chopconf disabled) produces a
// startup Warning, not a Fatal — see EvaluateGate.
type OperatorConfigSecurityFIPS struct {
	Enforced *types.StringBool `json:"enforced,omitempty" yaml:"enforced,omitempty"`
}

// OperatorConfigSecurityIPC controls the operator↔metrics-exporter REST
// channel hardening. CHOP-config-only; no CHI override.
//
// Mode=Plain (default): server binds all interfaces on :8888, no auth — identical
// to today's behavior, preserves upgrade compatibility.
//
// Mode=Secure: server binds BindHost:port (default loopback), and every request
// must carry an X-CHOP-Token header matching the token read from TokenPath. The
// operator generates the token at startup (32 random bytes via crypto/rand) and
// writes it to a shared emptyDir volume mounted into both operator and exporter
// containers.
type OperatorConfigSecurityIPC struct {
	// Mode selects Plain (default) or Secure. Valid values are IPCModePlain and
	// IPCModeSecure.
	Mode *types.String `json:"mode,omitempty" yaml:"mode,omitempty"`
	// BindHost defaults to IPCDefaultBindHost when Mode=Secure and unset.
	BindHost string `json:"bindHost,omitempty" yaml:"bindHost,omitempty"`
	// TokenPath defaults to IPCDefaultTokenPath when Mode=Secure and unset.
	TokenPath string `json:"tokenPath,omitempty" yaml:"tokenPath,omitempty"`
}

// GetClickHouse returns the ClickHouse sub-struct, nil-safe.
func (s *OperatorConfigSecurity) GetClickHouse() *ClusterSecurityClickHouse {
	if s == nil {
		return nil
	}
	return s.ClickHouse
}

// GetZookeeper returns the Zookeeper sub-struct, nil-safe.
func (s *OperatorConfigSecurity) GetZookeeper() *ClusterSecurityZookeeper {
	if s == nil {
		return nil
	}
	return s.Zookeeper
}

// GetKubernetes returns the Kubernetes sub-struct, nil-safe.
func (s *OperatorConfigSecurity) GetKubernetes() *ClusterSecurityKubernetes {
	if s == nil {
		return nil
	}
	return s.Kubernetes
}

// GetIPC returns the IPC sub-struct, nil-safe.
func (s *OperatorConfigSecurity) GetIPC() *OperatorConfigSecurityIPC {
	if s == nil {
		return nil
	}
	return s.IPC
}

// GetPolicy returns the resolved SecurityPolicy. Defaults to
// SecurityPolicyPermissive when unset so upgrades preserve 0.27.0 behavior.
// Nil-safe.
func (s *OperatorConfigSecurity) GetPolicy() SecurityPolicy {
	if (s == nil) || (s.Policy == nil) || !s.Policy.HasValue() {
		return SecurityPolicyPermissive
	}
	return normalizeSecurityPolicy(NewSecurityPolicy(s.Policy.Value()))
}

// IsEnforced reports whether security.policy is Enforced. Nil-safe.
func (s *OperatorConfigSecurity) IsEnforced() bool {
	return s.GetPolicy() == SecurityPolicyEnforced
}

// RequiresHardening returns true if any operator-wide hardening switch is on
// (either security.policy=Enforced OR security.fips.enforced=true). Used to
// gate per-CR security checks (plain-text ZK rejection, FIPS-bypass rejection,
// ZK digest-auth rejection) so they fire under EITHER switch — the two
// switches are orthogonal but the gates they trigger are the union (either
// switch tightens the posture; neither should leave a documented gate open).
// Nil-safe.
func (s *OperatorConfigSecurity) RequiresHardening() bool {
	if s == nil {
		return false
	}
	return s.IsEnforced() || s.GetFIPS().IsEnforced()
}

// GetImages returns the Images sub-struct, nil-safe.
func (s *OperatorConfigSecurity) GetImages() *OperatorConfigSecurityImages {
	if s == nil {
		return nil
	}
	return s.Images
}

// GetFIPS returns the FIPS sub-struct, nil-safe.
func (s *OperatorConfigSecurity) GetFIPS() *OperatorConfigSecurityFIPS {
	if s == nil {
		return nil
	}
	return s.FIPS
}

// IsEnforced reports whether security.fips.enforced=true is configured.
func (f *OperatorConfigSecurityFIPS) IsEnforced() bool {
	if f == nil {
		return false
	}
	return f.Enforced.IsTrue()
}

// GetPolicy returns the resolved FIPSImagePolicy. Defaults to
// FIPSImagePolicyPermissive when unset so upgrades preserve current behavior.
func (i *OperatorConfigSecurityImages) GetPolicy() FIPSImagePolicy {
	if (i == nil) || (i.Policy == nil) || !i.Policy.HasValue() {
		return FIPSImagePolicyPermissive
	}
	return normalizeFIPSImagePolicy(NewFIPSImagePolicy(i.Policy.Value()))
}

// IsRequired reports whether the image policy is Required. Nil-safe: nil
// receiver returns false (default Permissive).
func (i *OperatorConfigSecurityImages) IsRequired() bool {
	return i.GetPolicy() == FIPSImagePolicyRequired
}

// GetMode returns the resolved IPCMode. Defaults to IPCModePlain when unset so
// upgrades from older chopconfs keep working unchanged.
func (i *OperatorConfigSecurityIPC) GetMode() IPCMode {
	if (i == nil) || (i.Mode == nil) || !i.Mode.HasValue() {
		return IPCModePlain
	}
	return normalizeIPCMode(NewIPCMode(i.Mode.Value()))
}

// GetBindHost returns the configured BindHost. When Mode=Secure and BindHost is
// empty, callers should treat that as "127.0.0.1" (the documented default).
func (i *OperatorConfigSecurityIPC) GetBindHost() string {
	if i == nil {
		return ""
	}
	return i.BindHost
}

// GetTokenPath returns the configured TokenPath, or empty string when unset.
func (i *OperatorConfigSecurityIPC) GetTokenPath() string {
	if i == nil {
		return ""
	}
	return i.TokenPath
}
