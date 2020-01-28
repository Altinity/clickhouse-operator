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
	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopmodel "github.com/altinity/clickhouse-operator/pkg/model"
	"github.com/golang/glog"
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

// deleteHost deletes all kubernetes resources related to replica *chop.ChiHost
func (c *Controller) deleteHost(host *chop.ChiHost) error {
	// Each host consists of
	// 1. Tables on host - we need to delete tables on the host in order to clean Zookeeper data
	// 2. StatefulSet
	// 3. PersistentVolumeClaim
	// 4. ConfigMap
	// 5. Service
	// Need to delete all these item
	glog.V(1).Infof("Controller delete host %s/%s", host.Address.ClusterName, host.Name)

	_ = c.statefulSetDelete(host)
	_ = c.persistentVolumeClaimDelete(host)
	_ = c.configMapDelete(host)
	_ = c.deleteServiceHost(host)

	// When deleting the whole CHI (not particular host), CHI may already be unavailable, so update CHI tolerantly
	host.CHI.Status.DeletedHostsCount++
	_ = c.updateChiObjectStatus(host.CHI, true)

	glog.V(1).Infof("End delete host %s/%s", host.Address.ClusterName, host.Name)

	return nil
}

// deleteConfigMapsChi
func (c *Controller) deleteConfigMapsChi(chi *chop.ClickHouseInstallation) error {
	// Delete common ConfigMap's
	// Delete CHI service
	//
	// chi-b3d29f-common-configd   2      61s
	// chi-b3d29f-common-usersd    0      61s
	// service/clickhouse-example-01         LoadBalancer   10.106.183.200   <pending>     8123:31607/TCP,9000:31492/TCP,9009:31357/TCP   33s   clickhouse.altinity.com/chi=example-01

	var err error

	configMapCommon := chopmodel.CreateConfigMapCommonName(chi)
	configMapCommonUsersName := chopmodel.CreateConfigMapCommonUsersName(chi)

	// Delete ConfigMap
	err = c.kubeClient.CoreV1().ConfigMaps(chi.Namespace).Delete(configMapCommon, newDeleteOptions())
	if err == nil {
		glog.V(1).Infof("OK delete ConfigMap %s/%s", chi.Namespace, configMapCommon)
	} else {
		glog.V(1).Infof("FAIL delete ConfigMap %s/%s %v", chi.Namespace, configMapCommon, err)
	}

	err = c.kubeClient.CoreV1().ConfigMaps(chi.Namespace).Delete(configMapCommonUsersName, newDeleteOptions())
	if err == nil {
		glog.V(1).Infof("OK delete ConfigMap %s/%s", chi.Namespace, configMapCommonUsersName)
	} else {
		glog.V(1).Infof("FAIL delete ConfigMap %s/%s %v", chi.Namespace, configMapCommonUsersName, err)
	}

	return err
}

// statefulSetDeletePod delete a pod of a StatefulSet. This requests StatefulSet to relaunch deleted pod
func (c *Controller) statefulSetDeletePod(statefulSet *apps.StatefulSet) error {
	name := chopmodel.CreatePodName(statefulSet)
	glog.V(1).Infof("Delete Pod %s/%s", statefulSet.Namespace, name)
	return c.kubeClient.CoreV1().Pods(statefulSet.Namespace).Delete(name, newDeleteOptions())
}

// statefulSetDelete gracefully deletes StatefulSet through zeroing Pod's count
func (c *Controller) statefulSetDelete(host *chop.ChiHost) error {
	// IMPORTANT
	// StatefulSets do not provide any guarantees on the termination of pods when a StatefulSet is deleted.
	// To achieve ordered and graceful termination of the pods in the StatefulSet,
	// it is possible to scale the StatefulSet down to 0 prior to deletion.

	// Namespaced name
	name := chopmodel.CreateStatefulSetName(host)
	namespace := host.Address.Namespace

	glog.V(1).Infof("statefulSetDelete(%s/%s)", namespace, name)

	statefulSet, err := c.statefulSetLister.StatefulSets(namespace).Get(name)
	if err != nil {
		glog.V(1).Infof("error get StatefulSet %s/%s", namespace, name)
		return nil
	}

	// Zero pods count. This is the proper and graceful way to delete StatefulSet
	var zero int32 = 0
	statefulSet.Spec.Replicas = &zero
	statefulSet, _ = c.kubeClient.AppsV1().StatefulSets(namespace).Update(statefulSet)
	_ = c.waitStatefulSetGeneration(namespace, statefulSet.Name, statefulSet.Generation)

	// And now delete empty StatefulSet
	if err := c.kubeClient.AppsV1().StatefulSets(namespace).Delete(name, newDeleteOptions()); err == nil {
		glog.V(1).Infof("StatefulSet %s/%s deleted", namespace, name)
	} else {
		glog.V(1).Infof("StatefulSet %s/%s FAILED TO DELETE %v", namespace, name, err)
	}

	return nil
}

// persistentVolumeClaimDelete deletes PersistentVolumeClaim
func (c *Controller) persistentVolumeClaimDelete(host *chop.ChiHost) error {

	namespace := host.Address.Namespace
	labeler := chopmodel.NewLabeler(c.chop, host.CHI)
	listOptions := newListOptions(labeler.GetSelectorHostScope(host))
	if list, err := c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).List(listOptions); err == nil {
		glog.V(1).Infof("OK get list of PVC for host %s/%s", namespace, host.Name)
		for i := range list.Items {
			pvc := &list.Items[i]

			if chopmodel.HostCanDeletePVC(host, pvc.Name) {
				if err := c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Delete(pvc.Name, newDeleteOptions()); err == nil {
					glog.V(1).Infof("OK delete PVC %s/%s", namespace, pvc.Name)
				} else {
					glog.V(1).Infof("FAIL delete PVC %s/%s %v", namespace, pvc.Name, err)
				}
			} else {
				glog.V(1).Infof("PVC should not be deleted, leave them intact")
			}

		}
	} else {
		glog.V(1).Infof("FAIL get list of PVC for host %s/%s %v", namespace, host.Name, err)
	}

	return nil
}

// configMapDelete deletes ConfigMap
func (c *Controller) configMapDelete(host *chop.ChiHost) error {
	name := chopmodel.CreateConfigMapPodName(host)
	namespace := host.Address.Namespace

	glog.V(1).Infof("configMapDelete(%s/%s)", namespace, name)

	if err := c.kubeClient.CoreV1().ConfigMaps(namespace).Delete(name, newDeleteOptions()); err == nil {
		glog.V(1).Infof("ConfigMap %s/%s deleted", namespace, name)
	} else {
		glog.V(1).Infof("ConfigMap %s/%s delete FAILED %v", namespace, name, err)
	}

	return nil
}

// deleteServiceHost deletes Service
func (c *Controller) deleteServiceHost(host *chop.ChiHost) error {
	serviceName := chopmodel.CreateStatefulSetServiceName(host)
	namespace := host.Address.Namespace
	glog.V(1).Infof("deleteServiceReplica(%s/%s)", namespace, serviceName)
	return c.deleteServiceIfExists(namespace, serviceName)
}

// deleteServiceShard
func (c *Controller) deleteServiceShard(shard *chop.ChiShard) error {
	serviceName := chopmodel.CreateShardServiceName(shard)
	namespace := shard.Address.Namespace
	glog.V(1).Infof("deleteServiceShard(%s/%s)", namespace, serviceName)
	return c.deleteServiceIfExists(namespace, serviceName)
}

// deleteServiceCluster
func (c *Controller) deleteServiceCluster(cluster *chop.ChiCluster) error {
	serviceName := chopmodel.CreateClusterServiceName(cluster)
	namespace := cluster.Address.Namespace
	glog.V(1).Infof("deleteServiceCluster(%s/%s)", namespace, serviceName)
	return c.deleteServiceIfExists(namespace, serviceName)
}

// deleteServiceChi
func (c *Controller) deleteServiceChi(chi *chop.ClickHouseInstallation) error {
	serviceName := chopmodel.CreateChiServiceName(chi)
	namespace := chi.Namespace
	glog.V(1).Infof("deleteServiceChi(%s/%s)", namespace, serviceName)
	return c.deleteServiceIfExists(namespace, serviceName)
}

// deleteServiceIfExists
func (c *Controller) deleteServiceIfExists(namespace, name string) error {
	// Delete Service in case it does not exist

	// Check service exists
	_, err := c.kubeClient.CoreV1().Services(namespace).Get(name, metav1.GetOptions{})

	if err != nil {
		// No such a service, nothing to delete
		return nil
	}

	// Delete service
	err = c.kubeClient.CoreV1().Services(namespace).Delete(name, newDeleteOptions())
	if err == nil {
		glog.V(1).Infof("OK delete Service %s/%s", namespace, name)
	} else {
		glog.V(1).Infof("FAIL delete Service %s/%s %v", namespace, name, err)
	}

	return err
}
