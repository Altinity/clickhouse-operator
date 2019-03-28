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
	chopparser "github.com/altinity/clickhouse-operator/pkg/parser"
	apps "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newDeleteOptions() *metav1.DeleteOptions {
	gracePeriodSeconds := int64(0)
	propagationPolicy := metav1.DeletePropagationForeground
	return &metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriodSeconds,
		PropagationPolicy: &propagationPolicy,
	}
}

func (c *Controller) deleteReplica(replica *chop.ChiClusterLayoutShardReplica) error {
	configMapName := chopparser.CreateConfigMapDeploymentName(replica)
	statefulSetName := chopparser.CreateStatefulSetName(replica)
	statefulSetServiceName := chopparser.CreateStatefulSetServiceName(replica)

	// Delete StatefulSet
	statefulSet, _ := c.statefulSetLister.StatefulSets(replica.Address.Namespace).Get(statefulSetName)
	if statefulSet != nil {
		// Delete StatefulSet
		_ = c.kubeClient.AppsV1().StatefulSets(replica.Address.Namespace).Delete(statefulSetName, newDeleteOptions())
	}

	// Delete ConfigMap
	_ = c.kubeClient.CoreV1().ConfigMaps(replica.Address.Namespace).Delete(configMapName, newDeleteOptions())

	// Delete Service
	_ = c.kubeClient.CoreV1().Services(replica.Address.Namespace).Delete(statefulSetServiceName, newDeleteOptions())

	return nil
}

func (c *Controller) deleteShard(shard *chop.ChiClusterLayoutShard) {
	shard.WalkReplicas(c.deleteReplica)
}

func (c *Controller) deleteCluster(cluster *chop.ChiCluster) {
	cluster.WalkReplicas(c.deleteReplica)
}

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

	configMapCommon := chopparser.CreateConfigMapCommonName(chi.Name)
	configMapCommonUsersName := chopparser.CreateConfigMapCommonUsersName(chi.Name)
	// Delete ConfigMap
	err := c.kubeClient.CoreV1().ConfigMaps(chi.Namespace).Delete(configMapCommon, newDeleteOptions())
	if err != nil {
		glog.Infof("FAIL delete ConfigMap %s %v\n", configMapCommon, err)
	}
	err = c.kubeClient.CoreV1().ConfigMaps(chi.Namespace).Delete(configMapCommonUsersName, newDeleteOptions())
	if err != nil {
		glog.Infof("FAIL delete ConfigMap %s %v\n", configMapCommonUsersName, err)
	}

	chiServiceName := chopparser.CreateChiServiceName(chi)
	// Delete Service
	err = c.kubeClient.CoreV1().Services(chi.Namespace).Delete(chiServiceName, newDeleteOptions())
	if err != nil {
		glog.Infof("FAIL delete Service %s %v\n", chiServiceName, err)
	}
}

// statefulSetDeletePod delete all pod of a StatefulSet. This requests StatefulSet to relaunch deleted pods
func (c *Controller) statefulSetDeletePod(statefulSet *apps.StatefulSet) error {
	return c.kubeClient.CoreV1().Pods(statefulSet.Namespace).Delete(chopparser.CreatePodName(statefulSet), newDeleteOptions())
}
