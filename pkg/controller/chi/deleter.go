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
	"time"

	log "github.com/golang/glog"
	// log "k8s.io/klog"
	apps "k8s.io/api/apps/v1"

	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopmodel "github.com/altinity/clickhouse-operator/pkg/model"
)

// deleteHost deletes all kubernetes resources related to replica *chop.ChiHost
func (c *Controller) deleteHost(host *chop.ChiHost) error {
	// Each host consists of
	// 1. Tables on host - we need to delete tables on the host in order to clean Zookeeper data
	// 2. StatefulSet
	// 3. PersistentVolumeClaim
	// 4. ConfigMap
	// 5. Service
	// Need to delete all these item

	log.V(1).Infof("Controller delete host started %s/%s", host.Address.ClusterName, host.Name)

	_ = c.deleteStatefulSet(host)
	_ = c.deletePVC(host)
	_ = c.deleteConfigMap(host)
	_ = c.deleteServiceHost(host)

	log.V(1).Infof("Controller delete host completed %s/%s", host.Address.ClusterName, host.Name)

	return nil
}

// deleteConfigMapsCHI
func (c *Controller) deleteConfigMapsCHI(chi *chop.ClickHouseInstallation) error {
	// Delete common ConfigMap's
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
		log.V(1).Infof("OK delete ConfigMap %s/%s", chi.Namespace, configMapCommon)
	} else {
		log.V(1).Infof("FAIL delete ConfigMap %s/%s %v", chi.Namespace, configMapCommon, err)
	}

	err = c.kubeClient.CoreV1().ConfigMaps(chi.Namespace).Delete(configMapCommonUsersName, newDeleteOptions())
	if err == nil {
		log.V(1).Infof("OK delete ConfigMap %s/%s", chi.Namespace, configMapCommonUsersName)
	} else {
		log.V(1).Infof("FAIL delete ConfigMap %s/%s %v", chi.Namespace, configMapCommonUsersName, err)
	}

	return err
}

// statefulSetDeletePod delete a pod of a StatefulSet. This requests StatefulSet to relaunch deleted pod
func (c *Controller) statefulSetDeletePod(statefulSet *apps.StatefulSet) error {
	name := chopmodel.CreatePodName(statefulSet)
	log.V(1).Infof("Delete Pod %s/%s", statefulSet.Namespace, name)
	return c.kubeClient.CoreV1().Pods(statefulSet.Namespace).Delete(name, newDeleteOptions())
}

// deleteStatefulSet gracefully deletes StatefulSet through zeroing Pod's count
func (c *Controller) deleteStatefulSet(host *chop.ChiHost) error {
	// IMPORTANT
	// StatefulSets do not provide any guarantees on the termination of pods when a StatefulSet is deleted.
	// To achieve ordered and graceful termination of the pods in the StatefulSet,
	// it is possible to scale the StatefulSet down to 0 prior to deletion.

	// Namespaced name
	name := chopmodel.CreateStatefulSetName(host)
	namespace := host.Address.Namespace

	log.V(1).Infof("deleteStatefulSet(%s/%s)", namespace, name)

	statefulSet, err := c.getStatefulSetByHost(host)
	if err != nil {
		log.V(1).Infof("error get StatefulSet %s/%s", namespace, name)
		return nil
	}

	// Scale StatefulSet down to 0 pods count.
	// This is the proper and graceful way to delete StatefulSet
	var zero int32 = 0
	statefulSet.Spec.Replicas = &zero
	statefulSet, _ = c.kubeClient.AppsV1().StatefulSets(namespace).Update(statefulSet)
	_ = c.waitStatefulSetGeneration(namespace, statefulSet.Name, statefulSet.Generation)

	// And now delete empty StatefulSet
	if err := c.kubeClient.AppsV1().StatefulSets(namespace).Delete(name, newDeleteOptions()); err == nil {
		log.V(1).Infof("StatefulSet %s/%s deleted", namespace, name)
	} else {
		log.V(1).Infof("StatefulSet %s/%s FAILED TO DELETE %v", namespace, name, err)
		return nil
	}

	for {
		// TODO
		// There should be better way to sync cache
		if _, err := c.getStatefulSetByHost(host); err == nil {
			log.V(1).Infof("StatefulSet %s/%s deleted, cache NOT yet synced", namespace, name)
			time.Sleep(5 * time.Second)
		} else {
			log.V(1).Infof("StatefulSet %s/%s deleted, cache synced. Desc: %v", namespace, name, err)
			break
		}
	}

	return nil
}

// deletePVC deletes PersistentVolumeClaim
func (c *Controller) deletePVC(host *chop.ChiHost) error {

	namespace := host.Address.Namespace
	labeler := chopmodel.NewLabeler(c.chop, host.CHI)

	list, err := c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).List(newListOptions(labeler.GetSelectorHostScope(host)))
	if err != nil {
		log.V(1).Infof("FAIL get list of PVC for host %s/%s %v", namespace, host.Name, err)
		return err
	}

	log.V(1).Infof("OK get list of PVC for host %s/%s", namespace, host.Name)
	for i := range list.Items {
		// Convenience wrapper
		pvc := &list.Items[i]

		if !chopmodel.HostCanDeletePVC(host, pvc.Name) {
			log.V(1).Infof("PVC %s/%s should not be deleted, leave it intact", namespace, pvc.Name)
			// Move to the next PVC
			continue
		}

		// Actually delete PVC
		if err := c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Delete(pvc.Name, newDeleteOptions()); err == nil {
			log.V(1).Infof("OK delete PVC %s/%s", namespace, pvc.Name)
		} else {
			log.V(1).Infof("FAIL delete PVC %s/%s %v", namespace, pvc.Name, err)
		}
	}

	return nil
}

// deleteConfigMap deletes ConfigMap
func (c *Controller) deleteConfigMap(host *chop.ChiHost) error {
	name := chopmodel.CreateConfigMapPodName(host)
	namespace := host.Address.Namespace

	log.V(1).Infof("deleteConfigMap(%s/%s)", namespace, name)

	if err := c.kubeClient.CoreV1().ConfigMaps(namespace).Delete(name, newDeleteOptions()); err == nil {
		log.V(1).Infof("ConfigMap %s/%s deleted", namespace, name)
	} else {
		log.V(1).Infof("ConfigMap %s/%s delete FAILED %v", namespace, name, err)
	}

	return nil
}

// deleteServiceHost deletes Service
func (c *Controller) deleteServiceHost(host *chop.ChiHost) error {
	serviceName := chopmodel.CreateStatefulSetServiceName(host)
	namespace := host.Address.Namespace
	log.V(1).Infof("deleteServiceReplica(%s/%s)", namespace, serviceName)
	return c.deleteServiceIfExists(namespace, serviceName)
}

// deleteServiceShard
func (c *Controller) deleteServiceShard(shard *chop.ChiShard) error {
	serviceName := chopmodel.CreateShardServiceName(shard)
	namespace := shard.Address.Namespace
	log.V(1).Infof("deleteServiceShard(%s/%s)", namespace, serviceName)
	return c.deleteServiceIfExists(namespace, serviceName)
}

// deleteServiceCluster
func (c *Controller) deleteServiceCluster(cluster *chop.ChiCluster) error {
	serviceName := chopmodel.CreateClusterServiceName(cluster)
	namespace := cluster.Address.Namespace
	log.V(1).Infof("deleteServiceCluster(%s/%s)", namespace, serviceName)
	return c.deleteServiceIfExists(namespace, serviceName)
}

// deleteServiceCHI
func (c *Controller) deleteServiceCHI(chi *chop.ClickHouseInstallation) error {
	serviceName := chopmodel.CreateCHIServiceName(chi)
	namespace := chi.Namespace
	log.V(1).Infof("deleteServiceCHI(%s/%s)", namespace, serviceName)
	return c.deleteServiceIfExists(namespace, serviceName)
}

// deleteServiceIfExists
func (c *Controller) deleteServiceIfExists(namespace, name string) error {
	// Delete Service in case it does not exist

	// Check specified service exists
	_, err := c.kubeClient.CoreV1().Services(namespace).Get(name, newGetOptions())

	if err != nil {
		// No such a service, nothing to delete
		return nil
	}

	// Delete service
	err = c.kubeClient.CoreV1().Services(namespace).Delete(name, newDeleteOptions())
	if err == nil {
		log.V(1).Infof("OK delete Service %s/%s", namespace, name)
	} else {
		log.V(1).Infof("FAIL delete Service %s/%s %v", namespace, name, err)
	}

	return err
}
