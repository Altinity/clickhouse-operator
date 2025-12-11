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
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/model/zookeeper"
)

func (w *worker) reconcileClusterZookeeperRootPath(cluster *api.Cluster) error {
	// Cluster ZK path reconciliation is optional
	if !shouldReconcileClusterZookeeperPath(cluster) {
		// Nothing to reconcile
		return nil
	}

	// Yes, we are expected to reconcile ZK path

	w.a.V(1).
		WithEvent(cluster.GetCR(), a.EventActionCreate, a.EventReasonCreateStarted).
		WithAction(cluster.GetCR()).
		M(cluster.GetCR()).F().
		Info("Confirm ZK is configured for cluster %s/%s/%s", cluster.GetCR().GetNamespace(), cluster.GetCR().GetName(), cluster.GetName())

	ensureZkPath(cluster)

	w.a.V(1).
		WithEvent(cluster.GetCR(), a.EventActionCreate, a.EventReasonCreateCompleted).
		WithAction(cluster.GetCR()).
		M(cluster.GetCR()).F().
		Info("ZK is configured for cluster %s/%s/%s", cluster.GetCR().GetNamespace(), cluster.GetCR().GetName(), cluster.GetName())

	return nil
}

func ensureZkPath(cluster *api.Cluster) {
	conn := zookeeper.NewConnection(cluster.Zookeeper.Nodes)
	path := zookeeper.NewPathManager(conn)
	path.Ensure(cluster.Zookeeper.Root)
	path.Close()
}

func shouldReconcileClusterZookeeperPath(cluster *api.Cluster) bool {
	if cluster.IsStopped() {
		// Nothing to reconcile
		return false
	}
	if cluster.Zookeeper.IsEmpty() {
		// Nothing to reconcile
		return false
	}

	return true
}
