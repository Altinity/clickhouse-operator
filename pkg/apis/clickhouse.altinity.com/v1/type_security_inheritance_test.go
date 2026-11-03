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

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// chTLS builds a ClickHouseTLS leaf with optional verify + minVersion. Empty
// strings produce nil pointers (the "field unset" shape the merge logic relies on).
func chTLS(verify, minVersion string) *ClusterSecurityClickHouseTLS {
	t := &ClusterSecurityClickHouseTLS{}
	if verify != "" {
		t.Verify = types.NewString(verify)
	}
	if minVersion != "" {
		t.MinVersion = types.NewString(minVersion)
	}
	return t
}

// zkTLS builds a ZookeeperTLS leaf with optional verify + minVersion.
func zkTLS(verify, minVersion string) *ClusterSecurityZookeeperTLS {
	t := &ClusterSecurityZookeeperTLS{}
	if verify != "" {
		t.Verify = types.NewString(verify)
	}
	if minVersion != "" {
		t.MinVersion = types.NewString(minVersion)
	}
	return t
}

// wrapClickHouse wraps a TLS leaf in the cluster-security ClickHouse sub-struct.
// Returns nil when the leaf is nil, mirroring the "absent block" shape callers use.
func wrapClickHouse(tls *ClusterSecurityClickHouseTLS) *ClusterSecurityClickHouse {
	if tls == nil {
		return nil
	}
	return &ClusterSecurityClickHouse{TLS: tls}
}

func wrapZookeeper(tls *ClusterSecurityZookeeperTLS) *ClusterSecurityZookeeper {
	if tls == nil {
		return nil
	}
	return &ClusterSecurityZookeeper{TLS: tls}
}

// wrapClickHouseSecurity builds a single-leaf ClusterSecurity carrying only
// security.clickhouse.tls.verify; empty string returns nil to mirror the
// "block not present" shape.
func wrapClickHouseSecurity(verify string) *ClusterSecurity {
	if verify == "" {
		return nil
	}
	return &ClusterSecurity{ClickHouse: wrapClickHouse(chTLS(verify, ""))}
}

// inherit3Levels composes the chopconf → CHI → cluster FillEmpty chain into a
// single cluster-effective ClusterSecurity. End-state-equivalent to the
// production order (cluster ← chi via InheritClusterSecurityFrom, then
// cluster ← chopconf via normalizeClusterSecurity) under FillEmpty
// associativity — we pre-merge chop into chi here so the test exercises a
// single MergeFrom hop per leg without coupling to the normalizer package.
func inherit3Levels(chopSec, chiSec, clusterSec *ClusterSecurity) *ClusterSecurity {
	if chopSec != nil {
		if chiSec == nil {
			chiSec = &ClusterSecurity{}
		}
		chiSec.ClickHouse = chiSec.ClickHouse.MergeFrom(chopSec.GetClickHouse(), MergeTypeFillEmptyValues)
		chiSec.Zookeeper = chiSec.Zookeeper.MergeFrom(chopSec.GetZookeeper(), MergeTypeFillEmptyValues)
	}
	cluster := &Cluster{Security: clusterSec}
	cluster.InheritClusterSecurityFrom(&ClickHouseInstallation{Spec: ChiSpec{Security: chiSec}})
	return cluster.Security
}

// TestClusterSecurity_InheritFrom_ThreeLevel walks the documented inheritance
// chain chopconf → CHI → cluster for every meaningful "where is the value
// set?" combination. The cluster-effective value is what the runtime actually
// uses; if any hop misroutes, this table fires.
func TestClusterSecurity_InheritFrom_ThreeLevel(t *testing.T) {
	const Strict, None = "Strict", "None"

	cases := []struct {
		name              string
		chopVerify        string
		chiVerify         string
		clusterVerify     string
		wantClusterVerify string
	}{
		// Single-level — establishes the base inheritance hops work.
		{"chop only / propagates to cluster", Strict, "", "", Strict},
		{"chi only / propagates to cluster", "", Strict, "", Strict},
		{"cluster only / stays put", "", "", Strict, Strict},
		// Override precedence — most specific wins under FillEmpty semantics.
		{"chop+chi / chi wins over chop", Strict, None, "", None},
		{"chop+cluster / cluster wins over chop", Strict, "", None, None},
		{"chi+cluster / cluster wins over chi", "", Strict, None, None},
		{"chop+chi+cluster / cluster wins over both", Strict, None, None, None},
		// Empty everywhere — no surprises, no panic.
		{"all unset / cluster stays empty", "", "", "", ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			chopSec := wrapClickHouseSecurity(c.chopVerify)
			chiSec := wrapClickHouseSecurity(c.chiVerify)
			clusterSec := wrapClickHouseSecurity(c.clusterVerify)

			resolved := inherit3Levels(chopSec, chiSec, clusterSec)
			got := resolved.GetClickHouse().GetTLS().GetVerify()
			require.Equal(t, c.wantClusterVerify, string(got), "cluster-effective Verify mismatch")
		})
	}
}

// TestClusterSecurity_InheritFrom_NilSafe verifies the nil-shape matrix the
// production normalizer relies on: any level may be nil, none may panic, and
// the cluster-effective Security must always be safe to read via getter chain.
func TestClusterSecurity_InheritFrom_NilSafe(t *testing.T) {
	cases := []struct {
		name       string
		chop       *ClusterSecurity
		chi        *ClusterSecurity
		cluster    *ClusterSecurity
		wantVerify string
		wantZKMinV string
	}{
		{"all nil — no panic", nil, nil, nil, "", ""},
		{"only chop set", wrapClickHouseSecurity("Strict"), nil, nil, "Strict", ""},
		{"only chi set", nil, wrapClickHouseSecurity("Strict"), nil, "Strict", ""},
		{"only cluster set", nil, nil, wrapClickHouseSecurity("Strict"), "Strict", ""},
		{"chop ZK MinVersion propagates", &ClusterSecurity{Zookeeper: wrapZookeeper(zkTLS("", "1.3"))}, nil, nil, "", "1.3"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resolved := inherit3Levels(c.chop, c.chi, c.cluster)
			require.Equal(t, c.wantVerify, string(resolved.GetClickHouse().GetTLS().GetVerify()))
			require.Equal(t, c.wantZKMinV, string(resolved.GetZookeeper().GetTLS().GetMinVersion()))
		})
	}
}

// TestClusterSecurity_InheritFrom_StringFieldsPropagate verifies that string
// fields (ServerName, RootCA) — which are NOT *types.String typed aliases —
// also flow through the inheritance chain. Earlier we missed this because the
// MergeFrom switch treated them differently from pointer-valued knobs.
func TestClusterSecurity_InheritFrom_StringFieldsPropagate(t *testing.T) {
	chop := &ClusterSecurity{ClickHouse: &ClusterSecurityClickHouse{TLS: &ClusterSecurityClickHouseTLS{
		ServerName: "chop.example.com",
		RootCA:     "chop-ca",
	}}}
	resolved := inherit3Levels(chop, nil, nil)
	require.Equal(t, "chop.example.com", resolved.GetClickHouse().GetTLS().GetServerName())
	require.Equal(t, "chop-ca", resolved.GetClickHouse().GetTLS().GetRootCA())

	// Cluster-level explicit value wins over chopconf.
	cluster := &ClusterSecurity{ClickHouse: &ClusterSecurityClickHouse{TLS: &ClusterSecurityClickHouseTLS{
		ServerName: "cluster.example.com",
	}}}
	resolved = inherit3Levels(chop, nil, cluster)
	require.Equal(t, "cluster.example.com", resolved.GetClickHouse().GetTLS().GetServerName())
	require.Equal(t, "chop-ca", resolved.GetClickHouse().GetTLS().GetRootCA(), "RootCA must still inherit from chopconf since cluster left it empty")
}
