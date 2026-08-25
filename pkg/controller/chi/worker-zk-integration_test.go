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

package chi

import (
	"testing"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/stretchr/testify/require"
)

func TestShouldReconcileClusterZookeeperPath(t *testing.T) {
	zkNodes := api.ZookeeperNodes{{Host: "zk", Port: types.NewInt32(2181)}}

	newCluster := func(cr *api.ClickHouseInstallation) *api.Cluster {
		cluster := &api.Cluster{
			Zookeeper: &api.ZookeeperConfig{Nodes: zkNodes},
		}
		cluster.Runtime.SetCR(cr)
		return cluster
	}

	t.Run("stopped CR skips", func(t *testing.T) {
		cr := &api.ClickHouseInstallation{
			ObjectMeta: meta.ObjectMeta{Generation: 5},
			Spec:       api.ChiSpec{Stop: types.NewStringBool(true)},
		}
		require.False(t, shouldReconcileClusterZookeeperPath(newCluster(cr)))
	})

	t.Run("empty zookeeper skips", func(t *testing.T) {
		cr := &api.ClickHouseInstallation{ObjectMeta: meta.ObjectMeta{Generation: 5}}
		cluster := &api.Cluster{}
		cluster.Runtime.SetCR(cr)
		require.False(t, shouldReconcileClusterZookeeperPath(cluster))
	})

	t.Run("same generation as ancestor without action-plan work skips", func(t *testing.T) {
		ancestor := &api.ClickHouseInstallation{ObjectMeta: meta.ObjectMeta{Generation: 5}}
		cr := &api.ClickHouseInstallation{ObjectMeta: meta.ObjectMeta{Generation: 5}}
		cr.SetAncestor(ancestor)
		cr.EnsureRuntime().ActionPlan = api.MakeActionPlan(ancestor, cr)
		require.False(t, shouldReconcileClusterZookeeperPath(newCluster(cr)))
	})

	t.Run("generation advanced still reconciles", func(t *testing.T) {
		ancestor := &api.ClickHouseInstallation{ObjectMeta: meta.ObjectMeta{Generation: 4}}
		cr := &api.ClickHouseInstallation{ObjectMeta: meta.ObjectMeta{Generation: 5}}
		cr.SetAncestor(ancestor)
		cr.EnsureRuntime().ActionPlan = api.MakeActionPlan(ancestor, cr)
		require.True(t, shouldReconcileClusterZookeeperPath(newCluster(cr)))
	})

	t.Run("same generation with action-plan work still reconciles", func(t *testing.T) {
		ancestor := &api.ClickHouseInstallation{
			ObjectMeta: meta.ObjectMeta{Generation: 5},
			Spec:       api.ChiSpec{Troubleshoot: types.NewStringBool(false)},
		}
		cr := &api.ClickHouseInstallation{
			ObjectMeta: meta.ObjectMeta{Generation: 5},
			Spec:       api.ChiSpec{Troubleshoot: types.NewStringBool(true)},
		}
		cr.SetAncestor(ancestor)
		cr.EnsureRuntime().ActionPlan = api.MakeActionPlan(ancestor, cr)
		require.True(t, cr.EnsureRuntime().ActionPlan.HasActionsToDo())
		require.True(t, shouldReconcileClusterZookeeperPath(newCluster(cr)))
	})

	t.Run("no ancestor still reconciles", func(t *testing.T) {
		cr := &api.ClickHouseInstallation{ObjectMeta: meta.ObjectMeta{Generation: 1}}
		require.True(t, shouldReconcileClusterZookeeperPath(newCluster(cr)))
	})
}
