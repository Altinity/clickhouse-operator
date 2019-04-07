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
func (c *Controller) createOrUpdateChiResources(chi *chop.ClickHouseInstallation) error {
	listOfLists := chopmodels.ChiCreateObjects(chi, c.chopConfig)
	return c.createOrUpdateResources(chi, listOfLists)
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
	oldStatefulSet, err := c.statefulSetLister.StatefulSets(chi.Namespace).Get(newStatefulSet.Name)
	if oldStatefulSet != nil {
		return c.updateStatefulSet(oldStatefulSet, newStatefulSet)
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

func (c *Controller) updateStatefulSet(
	oldStatefulSet *apps.StatefulSet,
	newStatefulSet *apps.StatefulSet,
) error {
	// Convenience shortcuts
	namespace := oldStatefulSet.Namespace
	name := oldStatefulSet.Name
	generation := oldStatefulSet.Generation
	glog.Infof("updateStatefulSet(%s/%s)\n", namespace, name)

	updatedStatefulSet, err := c.kubeClient.AppsV1().StatefulSets(namespace).Update(newStatefulSet)
	if err != nil {
		// Update failed
		return err
	}

	// After calling "Update()"
	// 1. ObjectMeta.Generation is target generation
	// 2. Status.ObservedGeneration may be <= ObjectMeta.Generation

	if updatedStatefulSet.Generation == generation {
		glog.Infof("updateStatefulSet(%s/%s) - no generation change\n", namespace, name)
		return nil
	}

	glog.Infof("updateStatefulSet(%s/%s) - generation change %d=>%d\n", namespace, name, generation, updatedStatefulSet.Generation)

	if err := c.waitStatefulSetGeneration(namespace, name, updatedStatefulSet.Generation); err == nil {
		// Target generation reached
		return nil
	} else {
		// Unable to reach target generation
		return c.onStatefulSetUpdateFailed(oldStatefulSet)
	}

	return errors.New("updateStatefulSet() - unknown poisition")
}

func (c *Controller) waitStatefulSetGeneration(namespace, name string, targetGeneration int64) error {
	// StatefulSet can be considered as "reached target generation" when:
	// 1. Status.ObservedGeneration == ObjectMeta.Generation
	// 2. Status.ReadyReplicas == Spec.Replicas
	start := time.Now()
	for {
		if statefulSet, err := c.statefulSetLister.StatefulSets(namespace).Get(name); err != nil {
			// Unable to get StatefulSet
			return err
		} else if hasStatefulSetReachedGeneration(statefulSet, targetGeneration) {
			// StatefulSet ready
			glog.Infof("waitStatefulSetGeneration() - update completed up to Generation %d: status:%s\n", statefulSet.Generation, strStatefulSetStatus(&statefulSet.Status))
			return nil
		} else if time.Since(start) < (time.Duration(c.chopConfig.StatefulSetUpdateTimeout) * time.Second) {
			// Wait some more time
			time.Sleep(time.Duration(c.chopConfig.StatefulSetUpdatePollPeriod) * time.Second)
		} else {
			// Timeout reached
			return errors.New(fmt.Sprintf("waitStatefulSetGeneration(%s/%s) - wait timeout", namespace, name))
		}
	}

	return errors.New(fmt.Sprintf("waitStatefulSetGeneration(%s/%s) - unknown position", namespace, name))
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

func (c *Controller) onStatefulSetUpdateFailed(oldStatefulSet *apps.StatefulSet) error {
	switch c.chopConfig.OnStatefulSetUpdateFailureAction {
	case config.OnStatefulSetUpdateFailureActionAbort:
		// Do nothing
		glog.Errorf("onStatefulSetUpdateFailed(%s/%s) - abort\n", oldStatefulSet.Namespace, oldStatefulSet.Name)
		return errors.New(fmt.Sprintf("Updated failed %s", oldStatefulSet.Name))

	case config.OnStatefulSetUpdateFailureActionRevert:
		// Need to revert current StatefulSet to oldStatefulSet
		if statefulSet, err := c.statefulSetLister.StatefulSets(oldStatefulSet.Namespace).Get(oldStatefulSet.Name); err != nil {
			// Unable to get StatefulSet
			return err
		} else {
			// Get current status of oldStatefulSet and apply "previous" .Spec
			// make copy of "previous" .Spec just to be sure nothing gets corrupted
			statefulSet.Spec = *oldStatefulSet.Spec.DeepCopy()
			statefulSet, err = c.kubeClient.AppsV1().StatefulSets(statefulSet.Namespace).Update(statefulSet)
			_ = c.statefulSetDeletePod(statefulSet)
			return nil
		}
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
