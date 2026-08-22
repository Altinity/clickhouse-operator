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
	"testing"

	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
)

func TestRaftQuorumSize(t *testing.T) {
	require.Equal(t, 0, raftQuorumSize(0))
	require.Equal(t, 1, raftQuorumSize(1))
	require.Equal(t, 2, raftQuorumSize(3))
	require.Equal(t, 3, raftQuorumSize(5))
}

func TestEnsembleHasLiveQuorum(t *testing.T) {
	cr := chkWithHosts(3)
	w := &worker{}

	t.Run("no ready members — bootstrap / resume-from-stopped", func(t *testing.T) {
		w.countReadyEnsembleMembersFn = func(context.Context, api.ICustomResource) int { return 0 }
		require.False(t, w.ensembleHasLiveQuorum(context.Background(), cr))
	})

	t.Run("below quorum", func(t *testing.T) {
		w.countReadyEnsembleMembersFn = func(context.Context, api.ICustomResource) int { return 1 }
		require.False(t, w.ensembleHasLiveQuorum(context.Background(), cr))
	})

	t.Run("at quorum", func(t *testing.T) {
		w.countReadyEnsembleMembersFn = func(context.Context, api.ICustomResource) int { return 2 }
		require.True(t, w.ensembleHasLiveQuorum(context.Background(), cr))
	})
}

func TestShouldWaitHostReady(t *testing.T) {
	ctx := context.Background()

	t.Run("single host always waits Ready even with 0 ReadyReplicas", func(t *testing.T) {
		w := &worker{
			countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 0 },
		}
		host := hostOnCR(chkWithHosts(1))
		require.True(t, w.shouldWaitHostReady(ctx, host))
	})

	t.Run("multi-host without live quorum does not wait Ready", func(t *testing.T) {
		w := &worker{
			countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 0 },
		}
		host := hostOnCR(chkWithHosts(3))
		require.False(t, w.shouldWaitHostReady(ctx, host))
	})

	t.Run("multi-host with live quorum waits Ready", func(t *testing.T) {
		w := &worker{
			countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 2 },
		}
		host := hostOnCR(chkWithHosts(3))
		require.True(t, w.shouldWaitHostReady(ctx, host))
	})
}

func TestPrepareStsReconcileOptsWaitSection(t *testing.T) {
	ctx := context.Background()

	t.Run("no live quorum skips Ready", func(t *testing.T) {
		w := &worker{}
		host := hostOnCR(chkWithHosts(3))
		opts := w.prepareStsReconcileOptsWaitSection(ctx, host, nil, false)
		require.True(t, opts.WaitUntilStarted())
		require.False(t, opts.WaitUntilReady())
	})

	t.Run("waitReady waits Ready", func(t *testing.T) {
		w := &worker{}
		host := hostOnCR(chkWithHosts(3))
		opts := w.prepareStsReconcileOptsWaitSection(ctx, host, nil, true)
		require.True(t, opts.WaitUntilReady())
	})

	t.Run("waitReady can opt out of Ready probe", func(t *testing.T) {
		w := &worker{}
		host := hostOnCR(chkWithHosts(3))
		host.GetCluster().GetReconcile().Host.Wait.Probes.Readiness = types.NewStringBool(false)
		opts := w.prepareStsReconcileOptsWaitSection(ctx, host, statefulset.NewReconcileStatefulSetOptions(), true)
		require.False(t, opts.WaitUntilReady())
	})

	t.Run("single-host post-restart still waits Ready", func(t *testing.T) {
		// Simulates ReadyReplicas=0 after force-restart: shouldWaitHostReady was
		// true beforehand; prepareSts must honor that and not fall back to Started-only.
		w := &worker{
			countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 0 },
		}
		host := hostOnCR(chkWithHosts(1))
		waitReady := w.shouldWaitHostReady(ctx, host)
		require.True(t, waitReady)
		opts := w.prepareStsReconcileOptsWaitSection(ctx, host, nil, waitReady)
		require.True(t, opts.WaitUntilReady())
	})
}

func TestEnsureQuorumSafeToDisruptHost(t *testing.T) {
	ctx := context.Background()
	cr := chkWithHosts(3)
	host := hostOnCR(cr)
	host.Runtime.CurStatefulSet = &apps.StatefulSet{}
	host.Runtime.CurStatefulSet.Status.ReadyReplicas = 1

	t.Run("allows disrupt when no live quorum", func(t *testing.T) {
		w := &worker{
			countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 0 },
		}
		require.NoError(t, w.ensureQuorumSafeToDisruptHost(ctx, host))
	})

	t.Run("allows disrupt when siblings keep quorum", func(t *testing.T) {
		w := &worker{
			countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 3 },
		}
		require.NoError(t, w.ensureQuorumSafeToDisruptHost(ctx, host))
	})

	t.Run("refuses disrupt when remaining would be below quorum", func(t *testing.T) {
		w := &worker{
			countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 2 },
		}
		err := w.ensureQuorumSafeToDisruptHost(ctx, host)
		require.Error(t, err)
		require.Contains(t, err.Error(), "would drop below Raft quorum")
	})

	t.Run("allows disrupt of the sole host (n=1)", func(t *testing.T) {
		solo := hostOnCR(chkWithHosts(1))
		solo.Runtime.CurStatefulSet = &apps.StatefulSet{}
		solo.Runtime.CurStatefulSet.Status.ReadyReplicas = 1
		w := &worker{
			countReadyEnsembleMembersFn: func(context.Context, api.ICustomResource) int { return 1 },
		}
		require.NoError(t, w.ensureQuorumSafeToDisruptHost(ctx, solo))
	})
}

func TestChkStatefulSetFallbackAborts(t *testing.T) {
	f := newChkStatefulSetFallback()
	require.Equal(t, common.ErrCRUDAbort, f.OnStatefulSetCreateFailed(nil, nil))
	require.Equal(t, common.ErrCRUDAbort, f.OnStatefulSetUpdateFailed(nil, nil, nil, nil))
}

func chkWithHosts(n int) *apiChk.ClickHouseKeeperInstallation {
	cr := &apiChk.ClickHouseKeeperInstallation{}
	cr.EnsureRuntime()
	cluster := &apiChk.Cluster{Name: "c"}
	cluster.Layout = apiChk.NewChkClusterLayout()
	shard := &apiChk.ChkShard{Name: "s"}
	for i := 0; i < n; i++ {
		h := &api.Host{Name: "h"}
		h.Runtime.Address.ClusterName = cluster.Name
		h.Runtime.Address.ShardName = shard.Name
		h.Runtime.Address.HostName = "h"
		shard.Hosts = append(shard.Hosts, h)
	}
	cluster.Layout.Shards = []*apiChk.ChkShard{shard}
	cluster.Runtime.CHK = cr
	cluster.Reconcile = (&api.ClusterReconcile{}).Ensure()
	cluster.Reconcile.Host.Wait.Probes = &api.ReconcileHostWaitProbes{}
	cr.Spec.Configuration = &apiChk.Configuration{
		Clusters: []*apiChk.Cluster{cluster},
	}
	return cr
}

func hostOnCR(cr *apiChk.ClickHouseKeeperInstallation) *api.Host {
	cluster := cr.Spec.Configuration.Clusters[0]
	host := cluster.Layout.Shards[0].Hosts[0]
	host.SetCR(cr)
	host.Runtime.Address.ClusterName = cluster.Name
	host.Runtime.Address.ShardName = cluster.Layout.Shards[0].Name
	return host
}
