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

	chk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	chkConfig "github.com/altinity/clickhouse-operator/pkg/model/chk/config"
)

func init() {
	chop.New(nil, nil, "")
}

// withFIPSImagePolicy mutates chop.Config().Security in place, restoring on cleanup.
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

// buildCHKWithHosts builds a minimal ClickHouseKeeperInstallation with one
// cluster containing the requested host configurations. Each entry adds a
// shard with one replica. Empty image string means "no PodTemplate" — the host
// resolves to the operator's default Keeper image (which lacks the fips marker).
// PodTemplates are appended in one pass and the index is populated in a second
// pass — a single-pass `&slice[len-1]` would alias the pre-realloc backing
// array on subsequent appends.
func buildCHKWithHosts(name, namespace string, hostImages ...string) *chk.ClickHouseKeeperInstallation {
	cr := &chk.ClickHouseKeeperInstallation{}
	cr.Name = name
	cr.Namespace = namespace
	cr.Spec.Templates = &chi.Templates{PodTemplatesIndex: chi.NewPodTemplatesIndex()}
	cr.Spec.Configuration = &chk.Configuration{}
	cluster := &chk.Cluster{Name: "c", Layout: &chk.ChkClusterLayout{}}

	for i, image := range hostImages {
		host := &chi.Host{Templates: &chi.TemplatesList{}}
		host.Name = hostName(i)
		if image != "" {
			ptName := "pt-" + hostName(i)
			cr.Spec.Templates.PodTemplates = append(cr.Spec.Templates.PodTemplates, chi.PodTemplate{
				Name: ptName,
				Spec: core.PodSpec{Containers: []core.Container{{Name: chkConfig.KeeperContainerName, Image: image}}},
			})
			host.Templates.PodTemplate = ptName
		}
		host.SetCR(cr)
		shard := &chk.ChkShard{Hosts: []*chi.Host{host}}
		cluster.Layout.Shards = append(cluster.Layout.Shards, shard)
	}
	for i := range cr.Spec.Templates.PodTemplates {
		pt := &cr.Spec.Templates.PodTemplates[i]
		cr.Spec.Templates.PodTemplatesIndex.Set(pt.Name, pt)
	}
	cr.Spec.Configuration.Clusters = []*chk.Cluster{cluster}
	return cr
}

func hostName(i int) string { return fmt.Sprintf("h%d", i) }

// callGate constructs a minimal CHK Normalizer with the given target and invokes
// enforceFIPSImagePolicy. Returns the target so the test can inspect its status.
func callGate(target *chk.ClickHouseKeeperInstallation) *chk.ClickHouseKeeperInstallation {
	n := New()
	n.req = NewRequest(nil)
	n.req.SetTarget(target)
	n.enforceFIPSImagePolicy()
	return target
}

// TestEnforceFIPSImagePolicy_CHK mirrors the CHI admission test for the Keeper
// path. Same cell coverage: Required+non-fips aborts, Required+fips passes,
// Permissive ignored. The CHK gate uses chk.StatusReasonFIPSImagePolicyViolation,
// which type_status.go re-exports from the CHI constant so they share the wire format.
func TestEnforceFIPSImagePolicy_CHK(t *testing.T) {
	const fipsImage = "altinity/clickhouse-keeper:25.3.fips"
	const stockImage = "clickhouse/clickhouse-keeper:25.3"

	t.Run("Required + non-fips image aborts with FIPSImagePolicyViolation", func(t *testing.T) {
		withFIPSImagePolicy(t, string(chi.FIPSImagePolicyRequired))
		target := callGate(buildCHKWithHosts("chk", "ns", stockImage))

		s := target.EnsureStatus()
		require.Equal(t, chk.StatusAborted, s.Status)
		require.NotEmpty(t, s.Errors)
		require.True(t, strings.HasPrefix(s.Errors[0], "["+chk.StatusReasonFIPSImagePolicyViolation+"] "),
			"reason tag must lead the error string: got %q", s.Errors[0])
		require.Contains(t, s.Errors[0], stockImage)
	})

	t.Run("Required + fips image does not abort", func(t *testing.T) {
		withFIPSImagePolicy(t, string(chi.FIPSImagePolicyRequired))
		target := callGate(buildCHKWithHosts("chk", "ns", fipsImage))

		s := target.EnsureStatus()
		require.Empty(t, s.Status)
		require.Empty(t, s.Errors)
	})

	t.Run("Permissive (default) ignores non-fips image", func(t *testing.T) {
		withFIPSImagePolicy(t, "")
		target := callGate(buildCHKWithHosts("chk", "ns", stockImage))

		s := target.EnsureStatus()
		require.Empty(t, s.Status)
		require.Empty(t, s.Errors)
	})

	t.Run("Required + multiple non-fips hosts appends exactly one error", func(t *testing.T) {
		withFIPSImagePolicy(t, string(chi.FIPSImagePolicyRequired))
		target := callGate(buildCHKWithHosts("chk", "ns", stockImage, stockImage))

		s := target.EnsureStatus()
		require.Equal(t, chk.StatusAborted, s.Status)
		require.Len(t, s.Errors, 1, "WalkHosts gate must short-circuit after the first violation")
	})

	t.Run("nil target — no panic, no-op", func(t *testing.T) {
		withFIPSImagePolicy(t, string(chi.FIPSImagePolicyRequired))
		n := New()
		n.req = NewRequest(nil)
		n.req.SetTarget((*chk.ClickHouseKeeperInstallation)(nil))
		require.NotPanics(t, n.enforceFIPSImagePolicy)
	})
}

// TestRejectFIPSBypass exercises the cluster-level bypass detector that fires
// under FIPS strict mode for CHK. Mirrors the CHI test — semantics are identical
// per the documented "CHK mirror" comment in normalizer.go.
//
// Scope note: same as the CHI mirror, this test covers only the two targets
// reachable from chi.ClusterSecurity — clickhouse.tls.{verify,minVersion}
// and zookeeper.tls.{verify,minVersion}. K8s-client TLS and clickhouse.access
// .scheme are CHOP-config-scope-only by design (no per-CR/per-cluster field
// on ClusterSecurity) and are covered separately by
// pkg/chop/config_manager_test.go::TestK8sInsecureGate_Policy and
// pkg/apis/clickhouse.altinity.com/v1/type_security_fips_test.go::
// TestApplyFIPSStrict_CoercesHTTPSchemeUnderFIPS respectively.
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

	callBypass := func(security *chi.ClusterSecurity) *chk.ClickHouseKeeperInstallation {
		target := &chk.ClickHouseKeeperInstallation{}
		target.Name = "chk"
		target.Namespace = "ns"
		n := New()
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
			require.Equal(t, chk.StatusAborted, target.Status.Status)
			require.NotEmpty(t, target.Status.Errors)
			require.True(t, strings.HasPrefix(target.Status.Errors[0], "["+chk.StatusReasonFIPSValidationFailed+"] "),
				"reason tag must lead the error: got %q", target.Status.Errors[0])
			require.Contains(t, target.Status.Errors[0], c.wantField)
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
		n := New()
		n.req = NewRequest(nil)
		n.req.SetTarget((*chk.ClickHouseKeeperInstallation)(nil))
		require.NotPanics(t, func() { n.rejectFIPSBypass(build(chTLS(string(chi.TLSVerifyNone), ""), nil)) })
	})
}

// TestResolveKeeperImage verifies the pure helper underpinning the CHK gate.
// The default Keeper image MUST lack the fips marker — if anyone ever flips the
// default to a fips build without updating the gate documentation, the gate's
// "no PodTemplate → fail-closed under Required" invariant changes silently.
func TestResolveKeeperImage(t *testing.T) {
	t.Run("default image lacks fips marker — required-policy fail-closed invariant", func(t *testing.T) {
		require.False(t, strings.Contains(strings.ToLower(chkConfig.DefaultKeeperDockerImage), "fips"),
			"DefaultKeeperDockerImage must not contain 'fips' — flipping the default tag silently disables the admission gate's fail-closed behavior")
	})

	t.Run("no PodTemplate — returns operator default", func(t *testing.T) {
		cr := buildCHKWithHosts("chk", "ns", "")
		host := cr.Spec.Configuration.Clusters[0].Layout.Shards[0].Hosts[0]
		require.Equal(t, chkConfig.DefaultKeeperDockerImage, resolveKeeperImage(host))
	})

	t.Run("PodTemplate with keeper container — returns container image", func(t *testing.T) {
		cr := buildCHKWithHosts("chk", "ns", "altinity/clickhouse-keeper:25.3.fips")
		host := cr.Spec.Configuration.Clusters[0].Layout.Shards[0].Hosts[0]
		require.Equal(t, "altinity/clickhouse-keeper:25.3.fips", resolveKeeperImage(host))
	})
}
