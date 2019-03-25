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
	chopparser "github.com/altinity/clickhouse-operator/pkg/parser"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"github.com/golang/glog"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// createCHIResources creates k8s resources based on ClickHouseInstallation object specification
func (c *Controller) createCHIResources(chi *chop.ClickHouseInstallation) (*chop.ClickHouseInstallation, error) {
	chiCopy, err := chopparser.ChiCopyAndNormalize(chi)
	listOfLists := chopparser.ChiCreateObjects(chiCopy)
	err = c.createOrUpdateResources(chiCopy, listOfLists)

	return chiCopy, err
}

func (c *Controller) deleteReplica(replica *chop.ChiClusterLayoutShardReplica) error {

	configMapName := chopparser.CreateConfigMapDeploymentName(replica)
	statefulSetName := chopparser.CreateStatefulSetName(replica)
	statefulSetServiceName := chopparser.CreateStatefulSetName(replica)

	// Delete StatefulSet
	statefulSet, _ := c.statefulSetLister.StatefulSets(replica.Address.Namespace).Get(statefulSetName)
	if statefulSet != nil {
		// Delete StatefulSet
		_ = c.kubeClient.AppsV1().StatefulSets(replica.Address.Namespace).Delete(statefulSetName, &metav1.DeleteOptions{})
		return nil
	}

	// Delete ConfigMap
	_ = c.kubeClient.CoreV1().ConfigMaps(replica.Address.Namespace).Delete(configMapName, &metav1.DeleteOptions{})

	// Delete Service
	_ = c.kubeClient.CoreV1().Services(replica.Address.Namespace).Delete(statefulSetServiceName, &metav1.DeleteOptions{})

	return nil
}

func (c *Controller) deleteCluster(cluster *chop.ChiCluster) {
	cluster.WalkReplicas(c.deleteReplica)
}

func (c *Controller) deleteShard(shard *chop.ChiClusterLayoutShard) {
	shard.WalkReplicas(c.deleteReplica)
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
func (c *Controller) createOrUpdateStatefulSetResource(chi *chop.ClickHouseInstallation, statefulSet *apps.StatefulSet) error {
	// Check whether object with such name already exists in k8s
	res, err := c.statefulSetLister.StatefulSets(chi.Namespace).Get(statefulSet.Name)
	if res != nil {
		// Object with such name already exists, this is not an error
		glog.Infof("Update StatefulSet %s/%s\n", statefulSet.Namespace, statefulSet.Name)
		_, err := c.kubeClient.AppsV1().StatefulSets(chi.Namespace).Update(statefulSet)
		if err != nil {
			return err
		}
		return nil
	}

	if apierrors.IsNotFound(err) {
		// Object with such name not found - create it
		_, err = c.kubeClient.AppsV1().StatefulSets(chi.Namespace).Create(statefulSet)
	}
	if err != nil {
		return err
	}

	// Object created
	return nil
}
