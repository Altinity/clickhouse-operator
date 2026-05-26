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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer/subst"
)

// init wires the chop singleton with a default OperatorConfig so chop.Config()
// is non-nil inside the gate. Each test mutates Security in-place and restores
// it via t.Cleanup so the package-global doesn't leak across cases.
func init() {
	chop.New(nil, nil, "")
}

// withFIPSImagePolicy temporarily sets chop.Config().Security.Images.Policy
// to the requested value, restoring the previous Security block on test cleanup.
// Set policy="" to clear (Permissive default).
func withFIPSImagePolicy(t *testing.T, policy string) {
	t.Helper()
	cfg := chop.Config()
	prev := cfg.Security
	t.Cleanup(func() { cfg.Security = prev })

	cfg.Security = chi.OperatorConfigSecurity{}
	if policy != "" {
		cfg.Security.Images = &chi.OperatorConfigSecurityImages{
			Policy: types.NewString(policy),
		}
	}
}

// buildCRWithHosts builds a minimal ClickHouseInstallation with one cluster
// containing the requested host configurations. Each `hostImage` entry adds a
// shard with one replica. Empty image string means "no PodTemplate" — the host
// resolves to the operator default image (which lacks the fips marker).
//
// The CR back-pointer (host.Runtime.cr) is wired so resolveClickHouseImage's
// host.GetCR() lookup works. PodTemplates are appended in one pass and the
// index is populated in a second pass — a single-pass `&slice[len-1]` would
// alias the pre-realloc backing array on subsequent appends.
func buildCRWithHosts(name, namespace string, hostImages ...string) *chi.ClickHouseInstallation {
	cr := &chi.ClickHouseInstallation{}
	cr.Name = name
	cr.Namespace = namespace
	cr.Spec.Templates = &chi.Templates{PodTemplatesIndex: chi.NewPodTemplatesIndex()}
	cr.Spec.Configuration = &chi.Configuration{}
	cluster := &chi.Cluster{Name: "c", Layout: &chi.ChiClusterLayout{}}

	for i, image := range hostImages {
		host := &chi.Host{Templates: &chi.TemplatesList{}}
		host.Name = hostName(i)
		if image != "" {
			ptName := "pt-" + hostName(i)
			cr.Spec.Templates.PodTemplates = append(cr.Spec.Templates.PodTemplates, chi.PodTemplate{
				Name: ptName,
				Spec: core.PodSpec{Containers: []core.Container{{Name: config.ClickHouseContainerName, Image: image}}},
			})
			host.Templates.PodTemplate = ptName
		}
		host.SetCR(cr)
		shard := &chi.ChiShard{Hosts: []*chi.Host{host}}
		cluster.Layout.Shards = append(cluster.Layout.Shards, shard)
	}
	// Second pass: now that PodTemplates is fully built, index by stable pointer.
	for i := range cr.Spec.Templates.PodTemplates {
		pt := &cr.Spec.Templates.PodTemplates[i]
		cr.Spec.Templates.PodTemplatesIndex.Set(pt.Name, pt)
	}
	cr.Spec.Configuration.Clusters = []*chi.Cluster{cluster}
	return cr
}

// hostName returns a stable name for the i-th host. Uses fmt.Sprintf rather
// than rune arithmetic so i >= 10 still produces a clean digit-formatted name.
func hostName(i int) string { return fmt.Sprintf("h%d", i) }

// callGate constructs a minimal Normalizer with the given target and invokes
// enforceFIPSImagePolicy. Returns the target so the test can inspect its status.
func callGate(target *chi.ClickHouseInstallation) *chi.ClickHouseInstallation {
	n := New(nil)
	n.req = NewRequest(nil)
	n.req.SetTarget(target)
	n.enforceFIPSImagePolicy()
	return target
}

// TestEnforceFIPSImagePolicy exercises the admission-time gate in CHI
// normalizer.go. Covers the three policy/image cells the runtime actually
// hits: Required+non-fips aborts, Required+fips passes, Permissive ignored.
func TestEnforceFIPSImagePolicy(t *testing.T) {
	const fipsImage = "altinity/clickhouse-server:25.3.fips"
	const stockImage = "clickhouse/clickhouse-server:25.3"

	t.Run("Required + non-fips image aborts with FIPSImagePolicyViolation", func(t *testing.T) {
		withFIPSImagePolicy(t, string(chi.FIPSImagePolicyRequired))
		target := callGate(buildCRWithHosts("chi", "ns", stockImage))

		s := target.EnsureStatus()
		require.Equal(t, chi.StatusAborted, s.Status, "non-fips image under Required must abort")
		require.NotEmpty(t, s.Errors)
		require.True(t, strings.HasPrefix(s.Errors[0], "["+chi.StatusReasonFIPSImagePolicyViolation+"] "),
			"reason tag must lead the error string for auto-recovery prefix-match: got %q", s.Errors[0])
		require.Contains(t, s.Errors[0], stockImage, "abort message must name the offending image")
	})

	t.Run("Required + fips image does not abort (default PodTemplate image)", func(t *testing.T) {
		withFIPSImagePolicy(t, string(chi.FIPSImagePolicyRequired))
		target := callGate(buildCRWithHosts("chi", "ns", fipsImage))

		s := target.EnsureStatus()
		require.Empty(t, s.Status, "fips-tagged image must pass admission without setting status")
		require.Empty(t, s.Errors, "no error should be appended for a compliant CR")
	})

	t.Run("Permissive (default) ignores non-fips image", func(t *testing.T) {
		withFIPSImagePolicy(t, "") // Permissive default
		target := callGate(buildCRWithHosts("chi", "ns", stockImage))

		s := target.EnsureStatus()
		require.Empty(t, s.Status, "Permissive policy must not touch CR status")
		require.Empty(t, s.Errors)
	})

	// WalkHosts ignores callback errors, so the gate uses a local `aborted`
	// flag to fire ReconcileAbortWithReason exactly once. If a future refactor
	// drops the flag, every non-fips host would append a duplicate error —
	// this guard fires on the first regression.
	t.Run("Required + multiple non-fips hosts appends exactly one error", func(t *testing.T) {
		withFIPSImagePolicy(t, string(chi.FIPSImagePolicyRequired))
		target := callGate(buildCRWithHosts("chi", "ns", stockImage, stockImage, stockImage))

		s := target.EnsureStatus()
		require.Equal(t, chi.StatusAborted, s.Status)
		require.Len(t, s.Errors, 1, "WalkHosts gate must short-circuit after the first violation")
	})

	t.Run("nil target — no panic, no-op", func(t *testing.T) {
		withFIPSImagePolicy(t, string(chi.FIPSImagePolicyRequired))
		n := New(nil)
		n.req = NewRequest(nil)
		n.req.SetTarget((*chi.ClickHouseInstallation)(nil))
		require.NotPanics(t, n.enforceFIPSImagePolicy)
	})
}

// TestRejectFIPSBypass exercises the cluster-level bypass detector that fires
// under FIPS strict mode. Cluster-level Verify=None / MinVersion=1.2 / unset
// (only when the field is set explicitly) is a downgrade attempt that must
// abort the CR with `FIPSValidationFailed`. The first violation short-circuits
// — subsequent fields are not evaluated in the same call.
//
// Scope note: this test deliberately covers only the two targets reachable
// from chi.ClusterSecurity — clickhouse.tls.{verify,minVersion} and
// zookeeper.tls.{verify,minVersion}. The remaining FIPS-relevant knobs are
// out of scope at cluster level by design and are covered elsewhere:
//   - kubernetes.tls.verify is operator-process-scoped (one kubeconfig per
//     operator pod) and has no per-CR/per-cluster field on ClusterSecurity;
//     enforcement lives on OperatorConfigSecurity.Kubernetes and is exercised
//     by pkg/chop/config_manager_test.go::TestK8sInsecureGate_Policy.
//   - clickhouse.access.scheme=http is a CHOP-config-only coercion (Path-A
//     defaults flip http→https when FIPS strict is on) with no cluster-scope
//     override surface; coverage is in
//     pkg/apis/clickhouse.altinity.com/v1/type_security_fips_test.go::
//     TestApplyFIPSStrict_CoercesHTTPSchemeUnderFIPS.
//
// Adding rows for those fields here would assert against struct shape that
// the CRD does not expose, so the gap is intentional rather than a coverage
// miss.
func TestRejectFIPSBypass(t *testing.T) {
	build := func(ch *chi.ClusterSecurityClickHouseTLS, zk *chi.ClusterSecurityZookeeperTLS) *chi.ClusterSecurity {
		s := &chi.ClusterSecurity{}
		if ch != nil {
			s.ClickHouse = &chi.ClusterSecurityClickHouse{TLS: ch}
		}
		if zk != nil {
			s.Zookeeper = &chi.ClusterSecurityZookeeper{TLS: zk}
		}
		return s
	}
	chTLS := func(verify, minVersion string) *chi.ClusterSecurityClickHouseTLS {
		t := &chi.ClusterSecurityClickHouseTLS{}
		if verify != "" {
			t.Verify = types.NewString(verify)
		}
		if minVersion != "" {
			t.MinVersion = types.NewString(minVersion)
		}
		return t
	}
	zkTLS := func(verify, minVersion string) *chi.ClusterSecurityZookeeperTLS {
		t := &chi.ClusterSecurityZookeeperTLS{}
		if verify != "" {
			t.Verify = types.NewString(verify)
		}
		if minVersion != "" {
			t.MinVersion = types.NewString(minVersion)
		}
		return t
	}

	callBypass := func(security *chi.ClusterSecurity) *chi.ClickHouseInstallation {
		target := &chi.ClickHouseInstallation{}
		target.Name = "chi"
		target.Namespace = "ns"
		n := New(nil)
		n.req = NewRequest(nil)
		n.req.SetTarget(target)
		n.rejectFIPSBypass(security)
		return target
	}

	cases := []struct {
		name      string
		security  *chi.ClusterSecurity
		wantAbort bool
		wantField string
	}{
		{"all-Strict / no abort", build(chTLS(string(chi.TLSVerifyStrict), string(chi.TLSMinVersion12)), zkTLS(string(chi.TLSVerifyStrict), string(chi.TLSMinVersion12))), false, ""},
		{"unset ClickHouse verify / abort (gate-contract guard; chopconf merge prevents this in production but the gate's polarity must still hold)", build(chTLS("", ""), nil), true, "clickhouse.tls.verify"},
		{"ClickHouse verify=None / abort", build(chTLS(string(chi.TLSVerifyNone), ""), nil), true, "clickhouse.tls.verify"},
		{"ClickHouse verify=Strict, minVersion=garbage / abort", build(chTLS(string(chi.TLSVerifyStrict), "9.9"), nil), true, "clickhouse.tls.minVersion"},
		{"Zookeeper verify=None / abort after ClickHouse passes", build(chTLS(string(chi.TLSVerifyStrict), ""), zkTLS(string(chi.TLSVerifyNone), "")), true, "zookeeper.tls.verify"},
		{"Zookeeper minVersion=garbage / abort", build(chTLS(string(chi.TLSVerifyStrict), ""), zkTLS(string(chi.TLSVerifyStrict), "tls1.0")), true, "zookeeper.tls.minVersion"},
		{"nil sub-blocks / no abort", &chi.ClusterSecurity{}, false, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			target := callBypass(c.security)
			if !c.wantAbort {
				require.Nil(t, target.Status, "no abort expected")
				return
			}
			require.NotNil(t, target.Status)
			require.Equal(t, chi.StatusAborted, target.Status.Status)
			require.NotEmpty(t, target.Status.Errors)
			require.True(t, strings.HasPrefix(target.Status.Errors[0], "["+chi.StatusReasonFIPSValidationFailed+"] "),
				"reason tag must lead the error: got %q", target.Status.Errors[0])
			require.Contains(t, target.Status.Errors[0], c.wantField, "error must name the offending field")
		})
	}

	t.Run("short-circuits at first violation — single error appended", func(t *testing.T) {
		// ClickHouse verify=None AND Zookeeper verify=None — gate should abort
		// at ClickHouse and never evaluate ZK, so only one error is appended.
		target := callBypass(build(
			chTLS(string(chi.TLSVerifyNone), ""),
			zkTLS(string(chi.TLSVerifyNone), ""),
		))
		require.NotNil(t, target.Status)
		require.Len(t, target.Status.Errors, 1)
		require.Contains(t, target.Status.Errors[0], "clickhouse.tls.verify")
	})

	t.Run("nil target — no panic", func(t *testing.T) {
		n := New(nil)
		n.req = NewRequest(nil)
		n.req.SetTarget((*chi.ClickHouseInstallation)(nil))
		require.NotPanics(t, func() { n.rejectFIPSBypass(build(chTLS(string(chi.TLSVerifyNone), ""), nil)) })
	})
}

// TestResolveTLSRootCASecretRef exercises the rootCA Secret-resolution helper.
// The function inlines a Secret-sourced CA bundle into the in-memory RootCA
// field at normalize time, with strict failure modes that set CR status=Aborted
// — no silent fallback to system roots. Tested paths:
//
//   - empty Name sentinel → ref cleared, no abort
//   - inline RootCA + ref both set → RootCAConflict abort
//   - missing Secret → RootCASecretUnresolved abort
//   - Secret found with explicit key → inlined into RootCA
//   - empty key falls back ca.crt → tls.crt → fail
func TestResolveTLSRootCASecretRef(t *testing.T) {
	// fakeSecrets builds a SecretGetter from a map[name]map[key]value.
	fakeSecrets := func(secrets map[string]map[string][]byte) subst.SecretGetter {
		return func(namespace, name string) (*core.Secret, error) {
			data, ok := secrets[name]
			if !ok {
				return nil, fmt.Errorf("secret %s/%s not found", namespace, name)
			}
			return &core.Secret{Data: data}, nil
		}
	}

	callResolve := func(secretGet subst.SecretGetter, tls *chi.ClusterSecurityClickHouseTLS) (*chi.ClusterSecurityClickHouseTLS, *chi.ClickHouseInstallation) {
		target := &chi.ClickHouseInstallation{}
		target.Name = "chi"
		target.Namespace = "ns"
		n := New(secretGet)
		n.req = NewRequest(nil)
		n.req.SetTarget(target)
		n.resolveTLSRootCASecretRef(tls)
		return tls, target
	}

	t.Run("nil tls — no-op", func(t *testing.T) {
		_, target := callResolve(nil, nil)
		require.Nil(t, target.Status)
	})

	t.Run("nil RootCASecretRef — no-op", func(t *testing.T) {
		_, target := callResolve(nil, &chi.ClusterSecurityClickHouseTLS{})
		require.Nil(t, target.Status)
	})

	t.Run("empty Name sentinel — ref cleared, no abort", func(t *testing.T) {
		tls := &chi.ClusterSecurityClickHouseTLS{RootCASecretRef: &core.SecretKeySelector{}}
		_, target := callResolve(nil, tls)
		require.Nil(t, tls.RootCASecretRef, "empty Name must clear the ref in place")
		require.Nil(t, target.Status, "empty Name must NOT abort the CR")
	})

	t.Run("inline RootCA + ref both set — RootCAConflict abort", func(t *testing.T) {
		tls := &chi.ClusterSecurityClickHouseTLS{
			RootCA:          "inline-pem",
			RootCASecretRef: &core.SecretKeySelector{LocalObjectReference: core.LocalObjectReference{Name: "ca-secret"}},
		}
		_, target := callResolve(nil, tls)
		require.NotNil(t, target.Status)
		require.Equal(t, chi.StatusAborted, target.Status.Status)
		require.True(t, strings.HasPrefix(target.Status.Errors[0], "["+chi.StatusReasonRootCAConflict+"] "))
	})

	t.Run("missing Secret — RootCASecretUnresolved abort", func(t *testing.T) {
		tls := &chi.ClusterSecurityClickHouseTLS{
			RootCASecretRef: &core.SecretKeySelector{
				LocalObjectReference: core.LocalObjectReference{Name: "absent"},
				Key:                  "ca.crt",
			},
		}
		_, target := callResolve(fakeSecrets(map[string]map[string][]byte{}), tls)
		require.NotNil(t, target.Status)
		require.True(t, strings.HasPrefix(target.Status.Errors[0], "["+chi.StatusReasonRootCASecretUnresolved+"] "))
		require.Contains(t, target.Status.Errors[0], "absent")
	})

	t.Run("Secret + explicit key — inlined into RootCA", func(t *testing.T) {
		tls := &chi.ClusterSecurityClickHouseTLS{
			RootCASecretRef: &core.SecretKeySelector{
				LocalObjectReference: core.LocalObjectReference{Name: "ca-secret"},
				Key:                  "custom-key",
			},
		}
		_, target := callResolve(fakeSecrets(map[string]map[string][]byte{
			"ca-secret": {"custom-key": []byte("pem-content")},
		}), tls)
		require.Nil(t, target.Status)
		require.Equal(t, "pem-content", tls.RootCA)
	})

	t.Run("empty key — falls back to ca.crt", func(t *testing.T) {
		tls := &chi.ClusterSecurityClickHouseTLS{
			RootCASecretRef: &core.SecretKeySelector{
				LocalObjectReference: core.LocalObjectReference{Name: "ca-secret"},
			},
		}
		_, target := callResolve(fakeSecrets(map[string]map[string][]byte{
			"ca-secret": {"ca.crt": []byte("ca-crt-content")},
		}), tls)
		require.Nil(t, target.Status)
		require.Equal(t, "ca-crt-content", tls.RootCA)
	})

	t.Run("empty key — falls back to tls.crt when ca.crt absent", func(t *testing.T) {
		tls := &chi.ClusterSecurityClickHouseTLS{
			RootCASecretRef: &core.SecretKeySelector{
				LocalObjectReference: core.LocalObjectReference{Name: "ca-secret"},
			},
		}
		_, target := callResolve(fakeSecrets(map[string]map[string][]byte{
			"ca-secret": {"tls.crt": []byte("tls-crt-content")},
		}), tls)
		require.Nil(t, target.Status)
		require.Equal(t, "tls-crt-content", tls.RootCA, "must fall back to tls.crt when ca.crt absent")
	})

	t.Run("empty key — neither ca.crt nor tls.crt — RootCASecretUnresolved abort", func(t *testing.T) {
		tls := &chi.ClusterSecurityClickHouseTLS{
			RootCASecretRef: &core.SecretKeySelector{
				LocalObjectReference: core.LocalObjectReference{Name: "ca-secret"},
			},
		}
		_, target := callResolve(fakeSecrets(map[string]map[string][]byte{
			"ca-secret": {"unrelated-key": []byte("noise")},
		}), tls)
		require.NotNil(t, target.Status)
		require.True(t, strings.HasPrefix(target.Status.Errors[0], "["+chi.StatusReasonRootCASecretUnresolved+"] "))
	})
}

// TestResolveClickHouseImage verifies the pure helper underpinning the
// admission gate. The default image MUST lack the fips marker (otherwise
// "no PodTemplate" silently passes Required policy — a real footgun if anyone
// ever changes the default tag without updating the gate).
func TestResolveClickHouseImage(t *testing.T) {
	t.Run("default image lacks fips marker — required-policy fail-closed invariant", func(t *testing.T) {
		require.False(t, strings.Contains(strings.ToLower(config.DefaultClickHouseDockerImage), "fips"),
			"DefaultClickHouseDockerImage must not contain 'fips' — flipping the default tag silently disables the admission gate's fail-closed behavior for templates that lack a clickhouse container override")
	})

	t.Run("no PodTemplate — returns operator default image", func(t *testing.T) {
		cr := buildCRWithHosts("chi", "ns", "" /* no template */)
		host := cr.Spec.Configuration.Clusters[0].Layout.Shards[0].Hosts[0]
		require.Equal(t, config.DefaultClickHouseDockerImage, resolveClickHouseImage(host))
	})

	t.Run("PodTemplate with clickhouse container — returns container image", func(t *testing.T) {
		cr := buildCRWithHosts("chi", "ns", "altinity/clickhouse-server:25.3.fips")
		host := cr.Spec.Configuration.Clusters[0].Layout.Shards[0].Hosts[0]
		require.Equal(t, "altinity/clickhouse-server:25.3.fips", resolveClickHouseImage(host))
	})
}
