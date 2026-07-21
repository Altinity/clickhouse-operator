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

package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

// fakeNamer resolves NameInstanceHostname to the host name for test purposes.
type fakeNamer struct {
	interfaces.INameManager
}

func (f *fakeNamer) Name(what interfaces.NameType, params ...any) string {
	host := params[0].(*chi.Host)
	return host.GetName()
}

func raftHost(name string, replicaIdx int, status types.ObjectStatus) *chi.Host {
	// HostSecure carries an explicit (personal) resolved value so IsSecure()
	// short-circuits without dereferencing GetCluster()/GetCR() — mirrors
	// insecureHost() in listeners_test.go, which keeps the same unit test
	// self-contained without a real cluster/CR wired into host.Runtime.
	h := &chi.Host{
		Name: name,
		HostSecure: chi.HostSecure{
			Insecure: types.NewStringBool(true),
			Secure:   types.NewStringBool(false),
		},
	}
	h.GetRuntime().GetAddress().SetReplicaIndex(replicaIdx)
	h.RaftPort = types.NewInt32(9444)
	h.GetReconcileAttributes().SetStatus(status)
	return h
}

func TestRaftConfigStartAsFollowerOnJoin(t *testing.T) {
	// Scale-up of an established 1-node cluster (ancestor has host 0) to 2 nodes:
	// member 0 established + member 1 joining. The joining host must
	// start_as_follower. "Established" is keyed off the ancestor, not off the
	// per-host statuses.
	cr := &fakeCR{
		hosts: []*chi.Host{
			raftHost("keeper-0", 0, types.ObjectStatusFound),
			raftHost("keeper-1", 1, types.ObjectStatusRequested),
		},
		ancestorHosts: []*chi.Host{
			raftHost("keeper-0", 0, types.ObjectStatusFound),
		},
	}
	g := NewGenerator(cr, &fakeNamer{}, &GeneratorOptions{})
	xml := g.getRaftConfig(nil)

	// Joining host is marked start_as_follower, established host is not
	require.Contains(t, xml, "<start_as_follower>true</start_as_follower>")
	require.Equal(t, 1, strings.Count(xml, "<start_as_follower>true</start_as_follower>"))
	require.Contains(t, xml, "keeper-0")
	require.Contains(t, xml, "keeper-1")
}

func TestRaftConfigNoStartAsFollowerOnBootstrap(t *testing.T) {
	// Fresh cluster (no ancestor): ALL hosts are new. NuRaft forbids all servers
	// being start_as_follower — none must be emitted. Even though host 0 could
	// flip to a non-Requested status mid-loop, the ancestor-keyed classification
	// keeps every host a bootstrap member.
	cr := &fakeCR{hosts: []*chi.Host{
		raftHost("keeper-0", 0, types.ObjectStatusRequested),
		raftHost("keeper-1", 1, types.ObjectStatusRequested),
	}}
	g := NewGenerator(cr, &fakeNamer{}, &GeneratorOptions{})
	xml := g.getRaftConfig(nil)
	require.NotContains(t, xml, "start_as_follower")
}

// TestRaftConfigBootstrapIgnoresMutatedStatuses reproduces the fresh-3-node
// scenario that previously deadlocked: the sequential host loop marks host 0
// Created (non-Requested) before host 1 is generated. A status-keyed
// classification would then treat host 1 as "joining an established cluster"
// and emit start_as_follower on it (and drive a membership barrier that never
// converges). With no ancestor, all three must remain bootstrap members.
func TestRaftConfigBootstrapIgnoresMutatedStatuses(t *testing.T) {
	cr := &fakeCR{hosts: []*chi.Host{
		raftHost("keeper-0", 0, types.ObjectStatusCreated),
		raftHost("keeper-1", 1, types.ObjectStatusRequested),
		raftHost("keeper-2", 2, types.ObjectStatusRequested),
	}}
	g := NewGenerator(cr, &fakeNamer{}, &GeneratorOptions{})
	xml := g.getRaftConfig(nil)
	require.NotContains(t, xml, "start_as_follower",
		"fresh install (no ancestor) must never emit start_as_follower even after host 0 flips to Created")
}

// TestIsJoiningEstablishedCluster pins the classification directly: a Requested
// host is "joining" iff the CR has an ancestor with hosts (established cluster),
// and never on a fresh install regardless of sibling statuses.
func TestIsJoiningEstablishedCluster(t *testing.T) {
	joining := raftHost("keeper-1", 1, types.ObjectStatusRequested)
	found := raftHost("keeper-0", 0, types.ObjectStatusFound)

	t.Run("ancestor with hosts -> Requested host is joining", func(t *testing.T) {
		cr := &fakeCR{
			hosts:         []*chi.Host{found, joining},
			ancestorHosts: []*chi.Host{found},
		}
		require.True(t, isJoiningEstablishedCluster(cr, joining))
	})

	t.Run("no ancestor -> bootstrap even with a non-Requested sibling", func(t *testing.T) {
		created := raftHost("keeper-0", 0, types.ObjectStatusCreated)
		cr := &fakeCR{hosts: []*chi.Host{created, joining}}
		require.False(t, isJoiningEstablishedCluster(cr, joining))
	})

	t.Run("non-Requested host is never joining", func(t *testing.T) {
		cr := &fakeCR{
			hosts:         []*chi.Host{found},
			ancestorHosts: []*chi.Host{found},
		}
		require.False(t, isJoiningEstablishedCluster(cr, found))
	})
}
