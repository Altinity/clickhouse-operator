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

package chk

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// stsGetterNotFound models an API server that authoritatively reports the
// StatefulSet as absent (the genuine "host was never created" case).
func stsGetterNotFound(_ context.Context, host *api.Host) (*apps.StatefulSet, error) {
	return nil, apiErrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "statefulsets"}, host.GetName())
}

// stsGetterTransientError models a transient apiReader failure (timeout, 500,
// connection refused) — NOT a NotFound. A host must never be staged on this.
func stsGetterTransientError(_ context.Context, _ *api.Host) (*apps.StatefulSet, error) {
	return nil, apiErrors.NewInternalError(errors.New("apiserver temporarily unavailable"))
}

// stagingHost builds a host with a given reconcile status and optional existing
// StatefulSet (hasSTS models host.Runtime.CurStatefulSet, populated by fillCurSTS).
func stagingHost(name string, replicaIdx int, status types.ObjectStatus, hasSTS bool) *api.Host {
	h := &api.Host{Name: name}
	h.GetRuntime().GetAddress().SetReplicaIndex(replicaIdx)
	h.GetReconcileAttributes().SetStatus(status)
	if hasSTS {
		h.Runtime.CurStatefulSet = &apps.StatefulSet{}
	}
	return h
}

// stagingCR wires the given hosts into a single cluster/shard and, when
// ancestorHostCount > 0, marks the CR as having an established ancestor cluster
// of that size (mirrors a prior successful reconcile).
func stagingCR(ancestorHostCount int, hosts ...*api.Host) *apiChk.ClickHouseKeeperInstallation {
	build := func(hs []*api.Host) *apiChk.ClickHouseKeeperInstallation {
		cr := &apiChk.ClickHouseKeeperInstallation{}
		cr.Spec.Configuration = apiChk.NewConfiguration()
		cr.Spec.Configuration.Clusters = []*apiChk.Cluster{
			{
				Layout: &apiChk.ChkClusterLayout{
					Shards: []*apiChk.ChkShard{
						{Hosts: hs},
					},
				},
			},
		}
		return cr
	}
	cr := build(hosts)
	if ancestorHostCount > 0 {
		ancestorHosts := make([]*api.Host, ancestorHostCount)
		for i := range ancestorHosts {
			ancestorHosts[i] = stagingHost("ancestor", i, types.ObjectStatusFound, true)
		}
		cr.SetAncestor(build(ancestorHosts))
	}
	return cr
}

// TestStageNewHostsForRaftJoin pins the two coupled safety rules:
//   - B2: staging only runs for an established cluster (ancestor has hosts); a
//     fresh install (no ancestor) bootstraps with the full static XML.
//   - B1: within an established cluster, only a Requested host whose STS does
//     NOT yet exist is staged (excluded). A Requested host whose STS already
//     exists (committed or mid-join) must stay published, or the preliminary
//     raft-XML rewrite would drop a committed voter.
func TestStageNewHostsForRaftJoin(t *testing.T) {
	ctx := context.Background()

	t.Run("fresh install (no ancestor) stages nothing", func(t *testing.T) {
		w := &worker{stsGetterFn: stsGetterNotFound}
		h0 := stagingHost("keeper-0", 0, types.ObjectStatusRequested, false)
		h1 := stagingHost("keeper-1", 1, types.ObjectStatusRequested, false)
		h2 := stagingHost("keeper-2", 2, types.ObjectStatusRequested, false)
		cr := stagingCR(0, h0, h1, h2)

		w.stageNewHostsForRaftJoin(ctx, cr)

		require.False(t, h0.GetReconcileAttributes().IsExclude())
		require.False(t, h1.GetReconcileAttributes().IsExclude())
		require.False(t, h2.GetReconcileAttributes().IsExclude())
	})

	t.Run("scale-up first pass: STS-absent Requested hosts are staged", func(t *testing.T) {
		w := &worker{stsGetterFn: stsGetterNotFound}
		h0 := stagingHost("keeper-0", 0, types.ObjectStatusFound, true)
		h1 := stagingHost("keeper-1", 1, types.ObjectStatusRequested, false)
		h2 := stagingHost("keeper-2", 2, types.ObjectStatusRequested, false)
		cr := stagingCR(1, h0, h1, h2)

		w.stageNewHostsForRaftJoin(ctx, cr)

		require.False(t, h0.GetReconcileAttributes().IsExclude(), "established host must stay published")
		require.True(t, h1.GetReconcileAttributes().IsExclude(), "STS-absent joining host is staged")
		require.True(t, h2.GetReconcileAttributes().IsExclude(), "STS-absent joining host is staged")
	})

	t.Run("interrupted retry: Requested host WITH an STS is NOT staged", func(t *testing.T) {
		w := &worker{stsGetterFn: stsGetterNotFound}
		// host1 committed {0,1} on pass 1 (STS exists) but reappears Requested on
		// the retry pass; host2 never got created. host1 must stay published so
		// the preliminary rewrite keeps {0,1} and Keeper removes no committed voter.
		h0 := stagingHost("keeper-0", 0, types.ObjectStatusFound, true)
		h1 := stagingHost("keeper-1", 1, types.ObjectStatusRequested, true)
		h2 := stagingHost("keeper-2", 2, types.ObjectStatusRequested, false)
		cr := stagingCR(1, h0, h1, h2)

		w.stageNewHostsForRaftJoin(ctx, cr)

		require.False(t, h1.GetReconcileAttributes().IsExclude(), "already-created voter must NOT be re-staged")
		require.True(t, h2.GetReconcileAttributes().IsExclude(), "still-absent host is staged")
	})

	t.Run("transient STS error: Requested host is NOT staged", func(t *testing.T) {
		// fillCurSTS swallows a transient apiReader error into a nil CurStatefulSet.
		// A direct re-probe that returns a non-NotFound error must leave the host
		// published — staging it could drop a committed voter (the churn B1 prevents).
		w := &worker{stsGetterFn: stsGetterTransientError}
		h0 := stagingHost("keeper-0", 0, types.ObjectStatusFound, true)
		h1 := stagingHost("keeper-1", 1, types.ObjectStatusRequested, false)
		cr := stagingCR(1, h0, h1)

		w.stageNewHostsForRaftJoin(ctx, cr)

		require.False(t, h1.GetReconcileAttributes().IsExclude(),
			"a Requested host whose STS presence is uncertain (transient error) must NOT be staged")
	})
}
