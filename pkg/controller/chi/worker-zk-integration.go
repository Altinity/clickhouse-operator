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
	"github.com/altinity/clickhouse-operator/pkg/model/zookeeper"
)

func reconcileZookeeperRootPath(cluster *api.Cluster) {
	if !shouldReconcileZookeeperPath(cluster) {
		// Nothing to reconcile
		return
	}
	conn := zookeeper.NewConnection(cluster.Zookeeper.Nodes)
	path := zookeeper.NewPathManager(conn)
	path.Ensure(cluster.Zookeeper.Root)
	path.Close()
}

func shouldReconcileZookeeperPath(cluster *api.Cluster) bool {
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
