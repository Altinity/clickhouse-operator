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
	"github.com/altinity/clickhouse-operator/pkg/chop"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/model/zookeeper"
)

func (w *worker) reconcileClusterZookeeperRootPath(cluster *api.Cluster) error {
	// Cluster ZK path reconciliation is optional
	if !shouldReconcileClusterZookeeperPath(cluster) {
		// Nothing to reconcile
		return nil
	}

	// When the resolved ZK ensemble is TLS-only, the operator's
	// plaintext go-zookeeper dial cannot reach it. The bundled client has no
	// TLS-aware path-ensure today, so skip the precreation step and let the
	// ClickHouse server itself create the root path on its first DDL — that
	// dial happens inside ClickHouse over its (TLS-aware) Keeper client.
	if clusterZookeeperRequiresTLSDial(cluster) {
		w.a.V(1).M(cluster.GetCR()).F().
			Info("Skip ZK root-path ensure for cluster %s/%s/%s: ensemble is TLS-only and the operator dial is plaintext",
				cluster.GetCR().GetNamespace(), cluster.GetCR().GetName(), cluster.GetName())
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

// clusterZookeeperRequiresTLSDial reports whether any resolved ZK node is
// marked Secure (port 2281 / zk-secure / explicit secure flag). The
// go-zookeeper client embedded in the operator does not speak TLS without
// CertFile/KeyFile material, which the operator does not provision today.
// Callers that would otherwise open a plaintext socket should bail out and
// rely on ClickHouse itself for the dial.
func clusterZookeeperRequiresTLSDial(cluster *api.Cluster) bool {
	if (cluster == nil) || cluster.Zookeeper.IsEmpty() {
		return false
	}
	for _, node := range cluster.Zookeeper.Nodes {
		if node.Secure.IsTrue() {
			return true
		}
	}
	return false
}

func ensureZkPath(cluster *api.Cluster) {
	// Plumb cluster-level security.zookeeper.tls.{minVersion,verify} into the
	// ZK dial. Defaults preserve current behavior (Go default TLS version,
	// strict verify since RootCAs+ServerName are always set). CHOP-config
	// defaults reach here via InheritClusterSecurityFrom + normalizeClusterSecurity.
	zk := cluster.GetSecurity().GetZookeeper().GetTLS()
	mv := zk.GetMinVersion()
	verify := zk.GetVerify()
	// FIPS-compatible mode rejects ZK digest-auth (SHA-1 password hashing
	// inside the vendored go-zookeeper library). Pulled at dial-construction
	// time so each connection inherits the chopconf decision atomically.
	// Fires under EITHER security.policy=Enforced OR security.fips.enforced=true.
	rejectDigest := chop.Config().Security.RequiresHardening()
	var params *zookeeper.ConnectionParams
	if (mv != "") || (verify == api.TLSVerifyNone) || rejectDigest {
		params = &zookeeper.ConnectionParams{
			MinTLSVersion:      string(mv),
			InsecureSkipVerify: verify == api.TLSVerifyNone,
			RejectDigestAuth:   rejectDigest,
		}
	}
	conn := zookeeper.NewConnection(cluster.Zookeeper.Nodes, params)
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
