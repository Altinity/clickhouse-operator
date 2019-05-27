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
	"github.com/golang/glog"

	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopmodel "github.com/altinity/clickhouse-operator/pkg/model"
	apps "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// newDeleteOptions returns filled *metav1.DeleteOptions
func newDeleteOptions() *metav1.DeleteOptions {
	gracePeriodSeconds := int64(0)
	propagationPolicy := metav1.DeletePropagationForeground
	return &metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriodSeconds,
		PropagationPolicy:  &propagationPolicy,
	}
}

// deleteTablesOnReplica deletes ClickHouse tables on replica before replica is deleted
func (c *Controller) deleteTablesOnReplica(replica *chop.ChiReplica) {
	// Delete tables on replica
	tableNames, dropTableSQLs, _ := c.schemer.ReplicaGetDropTables(replica)
	glog.V(1).Infof("Drop tables: %v as %v\n", tableNames, dropTableSQLs)
	_ = c.schemer.ReplicaApplySQLs(replica, dropTableSQLs, false)
}

// deleteReplica deletes all kubernetes resources related to replica *chop.ChiReplica
func (c *Controller) deleteReplica(replica *chop.ChiReplica) error {
	// Each replica consists of
	// 1. Tables on replica - we need to delete tables on replica in order to clean Zookeeper data
	// 2. StatefulSet
	// 3. ConfigMap
	// 4. Service
	// Need to delete all these item

	c.deleteTablesOnReplica(replica)

	// Delete StatefulSet
	statefulSetName := chopmodel.CreateStatefulSetName(replica)
	c.statefulSetDelete(replica.Address.Namespace, statefulSetName)

	// Delete ConfigMap
	configMapName := chopmodel.CreateConfigMapPodName(replica)
	if err := c.kubeClient.CoreV1().ConfigMaps(replica.Address.Namespace).Delete(configMapName, newDeleteOptions()); err == nil {
		glog.V(1).Infof("ConfigMap %s deleted", configMapName)
	} else {
		glog.V(1).Infof("ConfigMap delete FAILED %v\n", err)
	}

	// Delete Service
	statefulSetServiceName := chopmodel.CreateStatefulSetServiceName(replica)
	if err := c.kubeClient.CoreV1().Services(replica.Address.Namespace).Delete(statefulSetServiceName, newDeleteOptions()); err == nil {
		glog.V(1).Infof("Service %s deleted", statefulSetServiceName)
	} else {
		glog.V(1).Infof("Server delete FAILED %v\n", err)
	}

	return nil
}

// deleteShard deletes all kubernetes resources related to shard *chop.ChiShard
func (c *Controller) deleteShard(shard *chop.ChiShard) {
	shard.WalkReplicas(c.deleteReplica)
}

// deleteCluster deletes all kubernetes resources related to cluster *chop.ChiCluster
func (c *Controller) deleteCluster(cluster *chop.ChiCluster) {
	cluster.WalkReplicas(c.deleteReplica)
}

// deleteChi deletes all kubernetes resources related to chi *chop.ClickHouseInstallation
func (c *Controller) deleteChi(chi *chop.ClickHouseInstallation) {
	chi.WalkClusters(func(cluster *chop.ChiCluster) error {
		c.deleteCluster(cluster)
		return nil
	})

	// Delete common ConfigMap's
	// Delete CHI service
	//
	// chi-b3d29f-common-configd   2      61s
	// chi-b3d29f-common-usersd    0      61s
	// service/clickhouse-example-01         LoadBalancer   10.106.183.200   <pending>     8123:31607/TCP,9000:31492/TCP,9009:31357/TCP   33s   clickhouse.altinity.com/chi=example-01

	configMapCommon := chopmodel.CreateConfigMapCommonName(chi)
	configMapCommonUsersName := chopmodel.CreateConfigMapCommonUsersName(chi)

	// Delete ConfigMap
	err := c.kubeClient.CoreV1().ConfigMaps(chi.Namespace).Delete(configMapCommon, newDeleteOptions())
	if err == nil {
		glog.V(1).Infof("OK delete ConfigMap %s\n", configMapCommon)
	} else {
		glog.V(1).Infof("FAIL delete ConfigMap %s %v\n", configMapCommon, err)
	}

	err = c.kubeClient.CoreV1().ConfigMaps(chi.Namespace).Delete(configMapCommonUsersName, newDeleteOptions())
	if err == nil {
		glog.V(1).Infof("OK delete ConfigMap %s\n", configMapCommonUsersName)
	} else {
		glog.V(1).Infof("FAIL delete ConfigMap %s %v\n", configMapCommonUsersName, err)
	}

	chiServiceName := chopmodel.CreateChiServiceName(chi)
	// Delete Service
	err = c.kubeClient.CoreV1().Services(chi.Namespace).Delete(chiServiceName, newDeleteOptions())
	if err == nil {
		glog.V(1).Infof("OK delete Service %s\n", chiServiceName)
	} else {
		glog.V(1).Infof("FAIL delete Service %s %v\n", chiServiceName, err)
	}
}

// statefulSetDeletePod delete a pod of a StatefulSet. This requests StatefulSet to relaunch deleted pod
func (c *Controller) statefulSetDeletePod(statefulSet *apps.StatefulSet) error {
	name := chopmodel.CreatePodName(statefulSet)
	glog.V(1).Infof("Delete Pod %s/%s\n", statefulSet.Namespace, name)
	return c.kubeClient.CoreV1().Pods(statefulSet.Namespace).Delete(name, newDeleteOptions())
}

// statefulSetDelete gracefully deletes StatefulSet through zeroing Pod's count
func (c *Controller) statefulSetDelete(namespace, name string) error {
	// IMPORTANT
	// StatefulSets do not provide any guarantees on the termination of pods when a StatefulSet is deleted.
	// To achieve ordered and graceful termination of the pods in the StatefulSet,
	// it is possible to scale the StatefulSet down to 0 prior to deletion.

	statefulSet, err := c.statefulSetLister.StatefulSets(namespace).Get(name)
	if err != nil {
		return nil
	}

	// Zero pods count. This is the proper and graceful way to delete StatefulSet
	var zero int32 = 0
	statefulSet.Spec.Replicas = &zero
	statefulSet, _ = c.kubeClient.AppsV1().StatefulSets(statefulSet.Namespace).Update(statefulSet)
	_ = c.waitStatefulSetGeneration(statefulSet.Namespace, statefulSet.Name, statefulSet.Generation)

	// And now delete empty StatefulSet
	if err := c.kubeClient.AppsV1().StatefulSets(namespace).Delete(name, newDeleteOptions()); err == nil {
		glog.V(1).Infof("StatefulSet %s/%s deleted", namespace, name)
	} else {
		glog.V(1).Infof("StatefulSet FAILED TO DELETE %v\n", err)
	}

	return nil
}
