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
	"errors"
	"fmt"
	"github.com/altinity/clickhouse-operator/pkg/config"

	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopmodels "github.com/altinity/clickhouse-operator/pkg/models"
	"github.com/golang/glog"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// createOrUpdateChiResources creates or updates kubernetes resources based on ClickHouseInstallation object specification
func (c *Controller) createOrUpdateChiResources(chi *chop.ClickHouseInstallation) (*chop.ClickHouseInstallation, error) {
	chiCopy, err := chopmodels.ChiCopyAndNormalize(chi)
	listOfLists := chopmodels.ChiCreateObjects(chiCopy, c.chopConfig)
	err = c.createOrUpdateResources(chiCopy, listOfLists)

	return chiCopy, err
}

func (c *Controller) createOrUpdateResources(chi *chop.ClickHouseInstallation, listOfLists []interface{}) error {
	for i := range listOfLists {
		switch listOfLists[i].(type) {
		case chopmodels.ServiceList:
			for j := range listOfLists[i].(chopmodels.ServiceList) {
				if err := c.createOrUpdateServiceResource(chi, listOfLists[i].(chopmodels.ServiceList)[j]); err != nil {
					return err
				}
			}
		case chopmodels.ConfigMapList:
			for j := range listOfLists[i].(chopmodels.ConfigMapList) {
				if err := c.createOrUpdateConfigMapResource(chi, listOfLists[i].(chopmodels.ConfigMapList)[j]); err != nil {
					return err
				}
			}
		case chopmodels.StatefulSetList:
			for j := range listOfLists[i].(chopmodels.StatefulSetList) {
				if err := c.createOrUpdateStatefulSetResource(chi, listOfLists[i].(chopmodels.StatefulSetList)[j]); err != nil {
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
	preUpdateStatefulSet, err := c.statefulSetLister.StatefulSets(chi.Namespace).Get(newStatefulSet.Name)
	if preUpdateStatefulSet != nil {
		preUpdateStatefulSet = preUpdateStatefulSet.DeepCopy()
		// Object with such name already exists, this is not an error
		glog.Infof("Update StatefulSet %s/%s\n", preUpdateStatefulSet.Namespace, preUpdateStatefulSet.Name)
		preUpdateGeneration := preUpdateStatefulSet.Generation
		updatedStatefulSet, err := c.kubeClient.AppsV1().StatefulSets(chi.Namespace).Update(newStatefulSet)
		// After calling "Update()"
		// 1. ObjectMeta.Generation is target generation
		// 2. Status.ObservedGeneration may be <= ObjectMeta.Generation
		if err != nil {
			return err
		}

		if updatedStatefulSet.Generation == preUpdateGeneration {
			glog.Infof("No generation change needed for StatefulSet %s/%s\n", updatedStatefulSet.Namespace, updatedStatefulSet.Name)
			return nil
		}

		glog.Infof("Generation change %d=>%d required for StatefulSet %s/%s\n", preUpdateGeneration, updatedStatefulSet.Generation, updatedStatefulSet.Namespace, updatedStatefulSet.Name)

		// StatefulSet can be considered as ready when:
		// 1. Status.ObservedGeneration ==
		// 		ObjectMeta.Generation ==
		// 2. Status.ReadyReplicas == Spec.Replicas
		start := time.Now()
		for {
			curStatefulSet, _ := c.statefulSetLister.StatefulSets(chi.Namespace).Get(newStatefulSet.Name)
			if hasStatefulSetReachedGeneration(curStatefulSet, updatedStatefulSet.Generation) {
				// StatefulSet ready
				glog.Infof("Update completed up to Generation %v: status:%s\n", curStatefulSet.Generation, strStatefulSetStatus(&curStatefulSet.Status))
				break // for
			} else {
				glog.Info("======================\n")
				glog.Infof("%s\n", strStatefulSetStatus(&updatedStatefulSet.Status))
				glog.Infof("%s\n", strStatefulSetStatus(&curStatefulSet.Status))
				if time.Since(start) < time.Duration(c.chopConfig.StatefulSetUpdateTimeout)*time.Second {
					// Wait some more time
					time.Sleep(time.Duration(c.chopConfig.StatefulSetUpdatePollPeriod) * time.Second)
				} else {
					// No more wait, do something
					return c.onStatefulSetUpdateFailed(curStatefulSet, preUpdateStatefulSet)
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

// hasStatefulSetReachedGeneration returns whether has StatefulSet reached the expected generation after upgrade or not
func hasStatefulSetReachedGeneration(statefulSet *apps.StatefulSet, generation int64) bool {
	// StatefulSet has .spec generation we are waiting for
	return (statefulSet.Generation == generation) &&
		// and this .spec generation is being applied to replicas - it is observed right now
		(statefulSet.Status.ObservedGeneration == statefulSet.Generation) &&
		// and all replicas are in "Ready" status - meaning ready to be used - no failure inside
		(statefulSet.Status.ReadyReplicas == *statefulSet.Spec.Replicas) &&
		// and all replicas are of expected generation
		(statefulSet.Status.CurrentReplicas == *statefulSet.Spec.Replicas) &&
		// and all replicas are updated - meaning rolling update completed over all replicas
		(statefulSet.Status.UpdatedReplicas == *statefulSet.Spec.Replicas) &&
		// and current revision is an updated one - meaning rolling update completed over all replicas
		(statefulSet.Status.CurrentRevision == statefulSet.Status.UpdateRevision)
}

func (c *Controller) onStatefulSetUpdateFailed(curStatefulSet, preUpdateStatefulSet *apps.StatefulSet) error {
	switch c.chopConfig.OnStatefulSetUpdateFailureAction {
	case config.OnStatefulSetUpdateFailureActionAbort:
		glog.Errorf("Updated failed %s\n", preUpdateStatefulSet.Name)
		return errors.New(fmt.Sprintf("Updated failed %s", preUpdateStatefulSet.Name))
	case config.OnStatefulSetUpdateFailureActionRevert:
		curStatefulSet.Spec = preUpdateStatefulSet.Spec
		curStatefulSet, err := c.kubeClient.AppsV1().StatefulSets(curStatefulSet.Namespace).Update(curStatefulSet)
		err = c.statefulSetDeletePod(curStatefulSet)
		return err
	default:
		glog.Errorf("Unknown c.chopConfig.OnStatefulSetUpdateFailureAction=%s\n", c.chopConfig.OnStatefulSetUpdateFailureAction)
	}
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
