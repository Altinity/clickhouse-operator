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
	"fmt"
	"errors"

	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopparser "github.com/altinity/clickhouse-operator/pkg/parser"
	"github.com/golang/glog"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// createOrUpdateChiResources creates k8s resources based on ClickHouseInstallation object specification
func (c *Controller) createOrUpdateChiResources(chi *chop.ClickHouseInstallation) (*chop.ClickHouseInstallation, error) {
	chiCopy, err := chopparser.ChiCopyAndNormalize(chi)
	listOfLists := chopparser.ChiCreateObjects(chiCopy)
	err = c.createOrUpdateResources(chiCopy, listOfLists)

	return chiCopy, err
}

func (c *Controller) deleteReplica(replica *chop.ChiClusterLayoutShardReplica) error {

	configMapName := chopparser.CreateConfigMapDeploymentName(replica)
	statefulSetName := chopparser.CreateStatefulSetName(replica)
	statefulSetServiceName := chopparser.CreateStatefulSetServiceName(replica)

	// Delete StatefulSet
	statefulSet, _ := c.statefulSetLister.StatefulSets(replica.Address.Namespace).Get(statefulSetName)
	if statefulSet != nil {
		// Delete StatefulSet
		_ = c.kubeClient.AppsV1().StatefulSets(replica.Address.Namespace).Delete(statefulSetName, &metav1.DeleteOptions{})
	}

	// Delete ConfigMap
	_ = c.kubeClient.CoreV1().ConfigMaps(replica.Address.Namespace).Delete(configMapName, &metav1.DeleteOptions{})

	// Delete Service
	_ = c.kubeClient.CoreV1().Services(replica.Address.Namespace).Delete(statefulSetServiceName, &metav1.DeleteOptions{})

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
	_ = c.kubeClient.CoreV1().ConfigMaps(chi.Namespace).Delete(configMapCommon, &metav1.DeleteOptions{})
	_ = c.kubeClient.CoreV1().ConfigMaps(chi.Namespace).Delete(configMapCommonUsersName, &metav1.DeleteOptions{})

	chiServiceName := chopparser.CreateChiServiceName(chi.Namespace)
	// Delete Service
	_ = c.kubeClient.CoreV1().Services(chi.Namespace).Delete(chiServiceName, &metav1.DeleteOptions{})
}

func (c *Controller) createOrUpdateResources(chi *chop.ClickHouseInstallation, listOfLists []interface{}) error {
	for i := range listOfLists {
		switch listOfLists[i].(type) {
		case chopparser.ServiceList:
			for j := range listOfLists[i].(chopparser.ServiceList) {
				if err := c.createOrUpdateServiceResource(chi, listOfLists[i].(chopparser.ServiceList)[j]); err != nil {
					return err
				}
			}
		case chopparser.ConfigMapList:
			for j := range listOfLists[i].(chopparser.ConfigMapList) {
				if err := c.createOrUpdateConfigMapResource(chi, listOfLists[i].(chopparser.ConfigMapList)[j]); err != nil {
					return err
				}
			}
		case chopparser.StatefulSetList:
			for j := range listOfLists[i].(chopparser.StatefulSetList) {
				if err := c.createOrUpdateStatefulSetResource(chi, listOfLists[i].(chopparser.StatefulSetList)[j]); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// createOrUpdateConfigMapResource creates core.ConfigMap resource
func (c *Controller) createOrUpdateConfigMapResource(chi *chop.ClickHouseInstallation, configMap *core.ConfigMap) error {
	// Check whether object with such name already exists in k8s
	res, err := c.configMapLister.ConfigMaps(chi.Namespace).Get(configMap.Name)
	if res != nil {
		// Object with such name already exists, this is not an error
		glog.Infof("Update ConfigMap %s/%s\n", configMap.Namespace, configMap.Name)
		_, err := c.kubeClient.CoreV1().ConfigMaps(chi.Namespace).Update(configMap)
		if err != nil {
			return err
		}
		return nil
	}

	// Object with such name does not exist or error happened

	if apierrors.IsNotFound(err) {
		// Object with such name not found - create it
		_, err = c.kubeClient.CoreV1().ConfigMaps(chi.Namespace).Create(configMap)
	}
	if err != nil {
		return err
	}

	// Object created
	return nil
}

// createOrUpdateServiceResource creates core.Service resource
func (c *Controller) createOrUpdateServiceResource(chi *chop.ClickHouseInstallation, service *core.Service) error {
	// Check whether object with such name already exists in k8s
	res, err := c.serviceLister.Services(chi.Namespace).Get(service.Name)
	if res != nil {
		// Object with such name already exists, this is not an error
		return nil
	}

	// Object with such name does not exist or error happened

	if apierrors.IsNotFound(err) {
		// Object with such name not found - create it
		_, err = c.kubeClient.CoreV1().Services(chi.Namespace).Create(service)
	}
	if err != nil {
		return err
	}

	// Object created
	return nil
}

// createOrUpdateStatefulSetResource creates apps.StatefulSet resource
func (c *Controller) createOrUpdateStatefulSetResource(chi *chop.ClickHouseInstallation, newStatefulSet *apps.StatefulSet) error {
	// Check whether object with such name already exists in k8s
	preupdateStatefulSet, err := c.statefulSetLister.StatefulSets(chi.Namespace).Get(newStatefulSet.Name)
	if preupdateStatefulSet != nil {
		// Object with such name already exists, this is not an error
		glog.Infof("Update StatefulSet %s/%s\n", preupdateStatefulSet.Namespace, preupdateStatefulSet.Name)
		generation := preupdateStatefulSet.Generation
		updatedStatefulSet, err := c.kubeClient.AppsV1().StatefulSets(chi.Namespace).Update(newStatefulSet)
		// After calling "Update()"
		// 1. ObjectMeta.Generation is target generation
		// 2. Status.ObservedGeneration may be <= ObjectMeta.Generation
		if err != nil {
			return err
		}

		if updatedStatefulSet.Generation == generation {
			glog.Infof("No generation change needed for StatefulSet %s/%s\n", updatedStatefulSet.Namespace, updatedStatefulSet.Name)
			return nil
		}

		glog.Infof("Generation change %d=>%d required for StatefulSet %s/%s\n", generation, updatedStatefulSet.Generation, updatedStatefulSet.Namespace, updatedStatefulSet.Name)

		// StatefulSet can be considered as ready when:
		// 1. Status.ObservedGeneration ==
		// 		ObjectMeta.Generation ==
		// 2. Status.ReadyReplicas == Spec.Replicas
		start := time.Now()
		for {
			curStatefulSet, _ := c.statefulSetLister.StatefulSets(chi.Namespace).Get(newStatefulSet.Name)
			if (curStatefulSet.Status.ObservedGeneration == updatedStatefulSet.Generation) &&
				(curStatefulSet.Status.ObservedGeneration == curStatefulSet.Generation) &&
				(curStatefulSet.Status.ReadyReplicas == *curStatefulSet.Spec.Replicas) &&
				(curStatefulSet.Status.CurrentReplicas == *curStatefulSet.Spec.Replicas) &&
				(curStatefulSet.Status.UpdatedReplicas == *curStatefulSet.Spec.Replicas) &&
				(curStatefulSet.Status.CurrentRevision == curStatefulSet.Status.UpdateRevision) {
				// StatefulSet ready
				glog.Infof("Update completed up to Generation %v: status:%s\n", curStatefulSet.Generation, strStatefulSetStatus(&curStatefulSet.Status))
				break // for
			} else {
				glog.Info("======================\n")
				glog.Infof("%s\n", strStatefulSetStatus(&updatedStatefulSet.Status))
				glog.Infof("%s\n", strStatefulSetStatus(&curStatefulSet.Status))
				if time.Since(start) < 1*time.Minute {
					// Wait some more time
					time.Sleep(1 * time.Second)
				} else {
					// No more wait, revert back
					glog.Errorf("Updated failed %s\n", strStatefulSetStatus(&curStatefulSet.Status))
					return errors.New(fmt.Sprintf("Updated failed %s", strStatefulSetStatus(&curStatefulSet.Status)))
				}
			}
		}

		return nil
	}

	if apierrors.IsNotFound(err) {
		// Object with such name not found - create it
		_, err = c.kubeClient.AppsV1().StatefulSets(chi.Namespace).Create(newStatefulSet)
	}
	if err != nil {
		return err
	}

	// Object created
	return nil
}

func strStatefulSetStatus(status *apps.StatefulSetStatus) string {
	return fmt.Sprintf(
		"ObservedGeneration:%d Replicas:%d ReadyReplicas:%d CurrentReplicas:%d UpdatedReplicas:%d CurrentRevision:%s UpdateRevision:%s",
		status.ObservedGeneration,
		status.Replicas,
		status.ReadyReplicas,
		status.CurrentReplicas,
		status.UpdatedReplicas,
		status.CurrentRevision,
		status.UpdateRevision,
	)
}