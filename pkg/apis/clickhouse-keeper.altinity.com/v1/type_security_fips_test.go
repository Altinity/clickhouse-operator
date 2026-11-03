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

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// TestStatusReasonFIPSValidationFailed verifies that the CHK status carries the
// same reason constant as CHI (operators grep for the same bracketed tag across
// both kinds of CRs) and that ReconcileAbortWithReason formats the error stream
// as `[reason] msg`. The reason MUST appear at the START — mirror of CHI test,
// pin so a format drift fails before auto-recovery skip silently breaks.
func TestStatusReasonFIPSValidationFailed(t *testing.T) {
	require.Equal(t, "FIPSValidationFailed", StatusReasonFIPSValidationFailed)

	s := &Status{}
	s.ReconcileAbortWithReason(StatusReasonFIPSValidationFailed, "cluster keeper-default.security.tls.verify=None is not permitted")

	require.Equal(t, StatusAborted, s.Status)
	require.NotEmpty(t, s.Errors)
	require.True(t, strings.HasPrefix(s.Errors[0], "[FIPSValidationFailed] "),
		"reason tag must be at the START of the error: got %q", s.Errors[0])
	require.Contains(t, s.Errors[0], "verify=None")
}

// TestStatus_ReconcileAbortWithReason_EmptyFields verifies the no-op behavior
// when both reason and msg are empty — the status flips to Aborted but no
// bogus tagged error is appended.
func TestStatus_ReconcileAbortWithReason_EmptyFields(t *testing.T) {
	s := &Status{}
	s.ReconcileAbortWithReason("", "")
	require.Equal(t, StatusAborted, s.Status)
	require.Empty(t, s.Errors, "must not append a tagged error when both fields empty")
}

// TestClusterSecurity_Promotion verifies the CHK Cluster.GetSecurity getter
// surfaces a real *apiChi.ClusterSecurity that downstream normalizer logic
// (FIPS gate, 3-level inheritance) can read — earlier the getter was a
// nil-returning stub.
func TestClusterSecurity_Promotion(t *testing.T) {
	c := &Cluster{}
	require.Nil(t, c.GetSecurity(), "unset cluster Security must report nil (back-compat default)")

	c.Security = &chi.ClusterSecurity{
		ClickHouse: &chi.ClusterSecurityClickHouse{
			TLS: &chi.ClusterSecurityClickHouseTLS{Verify: types.NewString(string(chi.TLSVerifyStrict))},
		},
	}
	got := c.GetSecurity()
	require.NotNil(t, got, "set cluster Security must round-trip through getter")
	require.Equal(t, chi.TLSVerifyStrict, got.GetClickHouse().GetTLS().GetVerify())
}

// TestChkSpec_GetSecurity_NilSafe ensures the spec-level getter handles every
// reachable nil shape: nil receiver, unset Security, set Security.
func TestChkSpec_GetSecurity_NilSafe(t *testing.T) {
	require.Nil(t, (*ChkSpec)(nil).GetSecurity(), "nil receiver must report nil")
	require.Nil(t, (&ChkSpec{}).GetSecurity(), "unset Security must report nil")

	spec := &ChkSpec{Security: &chi.ClusterSecurity{
		Zookeeper: &chi.ClusterSecurityZookeeper{
			TLS: &chi.ClusterSecurityZookeeperTLS{MinVersion: types.NewString(string(chi.TLSMinVersion13))},
		},
	}}
	got := spec.GetSecurity()
	require.NotNil(t, got)
	require.Equal(t, chi.TLSMinVersion13, got.GetZookeeper().GetTLS().GetMinVersion())
}

// TestCluster_InheritClusterSecurityFrom verifies the CHK-spec→cluster
// inheritance step that mirrors CHI's InheritClusterSecurityFrom: cluster-level
// values win where set; CHK-spec values fill empty slots.
func TestCluster_InheritClusterSecurityFrom(t *testing.T) {
	chk := &ClickHouseKeeperInstallation{
		Spec: ChkSpec{
			Security: &chi.ClusterSecurity{
				ClickHouse: &chi.ClusterSecurityClickHouse{
					TLS: &chi.ClusterSecurityClickHouseTLS{Verify: types.NewString(string(chi.TLSVerifyStrict))},
				},
				Zookeeper: &chi.ClusterSecurityZookeeper{
					TLS: &chi.ClusterSecurityZookeeperTLS{MinVersion: types.NewString(string(chi.TLSMinVersion13))},
				},
			},
		},
	}

	cluster := &Cluster{}
	cluster.InheritClusterSecurityFrom(chk)
	require.NotNil(t, cluster.Security, "inherit must allocate Security when spec has one")
	require.Equal(t, chi.TLSVerifyStrict, cluster.Security.GetClickHouse().GetTLS().GetVerify(), "ClickHouse.TLS.Verify inherited from spec")
	require.Equal(t, chi.TLSMinVersion13, cluster.Security.GetZookeeper().GetTLS().GetMinVersion(), "Zookeeper.TLS.MinVersion inherited from spec")

	// Cluster-level explicit value must take precedence over spec.
	cluster2 := &Cluster{Security: &chi.ClusterSecurity{
		ClickHouse: &chi.ClusterSecurityClickHouse{
			TLS: &chi.ClusterSecurityClickHouseTLS{Verify: types.NewString(string(chi.TLSVerifyNone))},
		},
	}}
	cluster2.InheritClusterSecurityFrom(chk)
	require.Equal(t, chi.TLSVerifyNone, cluster2.Security.GetClickHouse().GetTLS().GetVerify(), "cluster-level explicit value must win")
}

// TestCluster_InheritClusterSecurityFrom_SpecNoSecurity is the zero-regression
// path: CHKs that never set spec.security must not have an allocated
// cluster.Security after Inherit runs.
func TestCluster_InheritClusterSecurityFrom_SpecNoSecurity(t *testing.T) {
	chk := &ClickHouseKeeperInstallation{Spec: ChkSpec{}}
	cluster := &Cluster{}
	cluster.InheritClusterSecurityFrom(chk)
	require.Nil(t, cluster.Security, "no spec security must leave cluster.Security nil")
}
