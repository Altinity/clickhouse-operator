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
	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/config"
	chopmodel "github.com/altinity/clickhouse-operator/pkg/model"
	"github.com/golang/glog"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// reconcileChi reconciles ClickHouseInstallation
func (c *Controller) reconcileChi(chi *chop.ClickHouseInstallation) error {
	creator := chopmodel.NewCreator(chi, c.chopConfig, c.version)
	listOfObjectsLists := creator.CreateObjects()

	for i := range listOfObjectsLists {
		switch listOfObjectsLists[i].(type) {
		case chopmodel.ServiceList:
			for j := range listOfObjectsLists[i].(chopmodel.ServiceList) {
				if err := c.reconcileService(listOfObjectsLists[i].(chopmodel.ServiceList)[j]); err != nil {
					return err
				}
			}
		case chopmodel.ConfigMapList:
			for j := range listOfObjectsLists[i].(chopmodel.ConfigMapList) {
				if err := c.reconcileConfigMap(listOfObjectsLists[i].(chopmodel.ConfigMapList)[j]); err != nil {
					return err
				}
			}
		case chopmodel.StatefulSetList:
			for j := range listOfObjectsLists[i].(chopmodel.StatefulSetList) {
				if err := c.reconcileStatefulSet(listOfObjectsLists[i].(chopmodel.StatefulSetList)[j]); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// reconcileConfigMap reconciles core.ConfigMap
func (c *Controller) reconcileConfigMap(configMap *core.ConfigMap) error {
	// Check whether object with such name already exists in k8s
	curConfigMap, err := c.getConfigMap(&configMap.ObjectMeta)

	if curConfigMap != nil {
		// Object with such name already exists, this is not an error
		glog.V(1).Infof("Update ConfigMap %s/%s\n", configMap.Namespace, configMap.Name)
		_, err := c.kubeClient.CoreV1().ConfigMaps(configMap.Namespace).Update(configMap)
		if err != nil {
			return err
		}
		return nil
	}

	// Object with such name does not exist or error happened

	if apierrors.IsNotFound(err) {
		// Object with such name not found - create it
		_, err = c.kubeClient.CoreV1().ConfigMaps(configMap.Namespace).Create(configMap)
	}
	if err != nil {
		return err
	}

	// Object created
	return nil
}

// reconcileService reconciles core.Service
func (c *Controller) reconcileService(service *core.Service) error {
	// Check whether object with such name already exists in k8s
	curService, err := c.getService(&service.ObjectMeta)

	if curService != nil {
		// Object with such name already exists, this is not an error
		return nil
	}

	// Object with such name does not exist or error happened

	if apierrors.IsNotFound(err) {
		// Object with such name not found - create it
		_, err = c.kubeClient.CoreV1().Services(service.Namespace).Create(service)
	}
	if err != nil {
		return err
	}

	// Object created
	return nil
}

// reconcileStatefulSet reconciles apps.StatefulSet
func (c *Controller) reconcileStatefulSet(newStatefulSet *apps.StatefulSet) error {
	// Check whether object with such name already exists in k8s
	curStatefulSet, err := c.getStatefulSet(&newStatefulSet.ObjectMeta)

	if curStatefulSet != nil {
		// StatefulSet already exists - update it
		return c.updateStatefulSet(curStatefulSet, newStatefulSet)
	}

	if apierrors.IsNotFound(err) {
		// StatefulSet with such name not found - create StatefulSet
		return c.createStatefulSet(newStatefulSet)
	}

	// Error has happened with .Get()
	return err
}

func (c *Controller) createStatefulSet(statefulSet *apps.StatefulSet) error {
	if statefulSet, err := c.kubeClient.AppsV1().StatefulSets(statefulSet.Namespace).Create(statefulSet); err != nil {
		return err
	} else if err := c.waitStatefulSetGeneration(statefulSet.Namespace, statefulSet.Name, statefulSet.Generation); err == nil {
		// Target generation reached, StatefulSet created successfully
		return nil
	} else {
		// Unable to reach target generation, StatefulSet create failed, time to rollback?
		return c.onStatefulSetCreateFailed(statefulSet)
	}

	return errors.New("createStatefulSet() - unknown position")
}

func (c *Controller) updateStatefulSet(oldStatefulSet *apps.StatefulSet, newStatefulSet *apps.StatefulSet) error {
	// Convenience shortcuts
	namespace := newStatefulSet.Namespace
	name := newStatefulSet.Name
	glog.V(1).Infof("updateStatefulSet(%s/%s)\n", namespace, name)

	// Apply newStatefulSet and wait for Generation to change
	updatedStatefulSet, err := c.kubeClient.AppsV1().StatefulSets(namespace).Update(newStatefulSet)
	if err != nil {
		// Update failed
		return err
	}

	// After calling "Update()"
	// 1. ObjectMeta.Generation is target generation
	// 2. Status.ObservedGeneration may be <= ObjectMeta.Generation

	if updatedStatefulSet.Generation == oldStatefulSet.Generation {
		// Generation is not updated - no changes in .spec section were made
		glog.V(1).Infof("updateStatefulSet(%s/%s) - no generation change\n", namespace, name)
		return nil
	}

	glog.V(1).Infof("updateStatefulSet(%s/%s) - generation change %d=>%d\n", namespace, name, oldStatefulSet.Generation, updatedStatefulSet.Generation)

	if err := c.waitStatefulSetGeneration(namespace, name, updatedStatefulSet.Generation); err == nil {
		// Target generation reached, StatefulSet updated successfully
		return nil
	} else {
		// Unable to reach target generation, StatefulSet update failed, time to rollback?
		return c.onStatefulSetUpdateFailed(oldStatefulSet)
	}

	return errors.New("updateStatefulSet() - unknown position")
}

// waitStatefulSetGeneration polls StatefulSet for reaching target generation
func (c *Controller) waitStatefulSetGeneration(namespace, name string, targetGeneration int64) error {
	// Wait for some limited time for StatefulSet to reach target generation
	// Wait timeout is specified in c.chopConfig.StatefulSetUpdateTimeout in seconds
	start := time.Now()
	for {
		if statefulSet, err := c.statefulSetLister.StatefulSets(namespace).Get(name); err != nil {
			// Unable to get StatefulSet
			if apierrors.IsNotFound(err) {
				// Object with such name not found - may be is still being created - wait for it
				glog.V(1).Infof("waitStatefulSetGeneration() - object not yet created, wait for it\n")
				time.Sleep(time.Duration(c.chopConfig.StatefulSetUpdatePollPeriod) * time.Second)
			} else {
				// Some kind of total error
				glog.V(1).Infof("ERROR waitStatefulSetGeneration(%s/%s) Get() FAILED\n", namespace, name)
				return err
			}
		} else if hasStatefulSetReachedGeneration(statefulSet, targetGeneration) {
			// StatefulSet is available and generation reached
			// All is good, job done, exit
			glog.V(1).Infof("waitStatefulSetGeneration(OK):%s\n", strStatefulSetStatus(&statefulSet.Status))
			return nil
		} else if time.Since(start) < (time.Duration(c.chopConfig.StatefulSetUpdateTimeout) * time.Second) {
			// StatefulSet is available but generation is not yet reached
			// Wait some more time
			glog.V(1).Infof("waitStatefulSetGeneration():%s\n", strStatefulSetStatus(&statefulSet.Status))
			time.Sleep(time.Duration(c.chopConfig.StatefulSetUpdatePollPeriod) * time.Second)
		} else {
			// StatefulSet is available but generation is not yet reached
			// Timeout reached
			// Failed, time to quit
			glog.V(1).Infof("ERROR waitStatefulSetGeneration(%s/%s) - TIMEOUT reached\n", namespace, name)
			return errors.New(fmt.Sprintf("waitStatefulSetGeneration(%s/%s) - wait timeout", namespace, name))
		}
	}

	return errors.New(fmt.Sprintf("waitStatefulSetGeneration(%s/%s) - unknown position", namespace, name))
}

// onStatefulSetCreateFailed handles situation when StatefulSet create failed
// It can just delete failed StatefulSet or do nothing
func (c *Controller) onStatefulSetCreateFailed(failedStatefulSet *apps.StatefulSet) error {
	// Convenience shortcuts
	namespace := failedStatefulSet.Namespace
	name := failedStatefulSet.Name

	// What to do with StatefulSet - look into chop configuration settings
	switch c.chopConfig.OnStatefulSetCreateFailureAction {
	case config.OnStatefulSetCreateFailureActionAbort:
		// Do nothing, just report appropriate error
		glog.V(1).Infof("onStatefulSetCreateFailed(%s/%s) - abort\n", namespace, name)
		return errors.New(fmt.Sprintf("Create failed on %s/%s", namespace, name))

	case config.OnStatefulSetCreateFailureActionDelete:
		// Delete gracefully problematic failed StatefulSet
		glog.V(1).Infof("onStatefulSetCreateFailed(%s/%s) - going to DELETE FAILED StatefulSet\n", namespace, name)
		c.statefulSetDelete(namespace, name)
		return c.shouldContinueOnCreateFailed()
	default:
		glog.V(1).Infof("Unknown c.chopConfig.OnStatefulSetCreateFailureAction=%s\n", c.chopConfig.OnStatefulSetCreateFailureAction)
		return nil
	}

	return errors.New(fmt.Sprintf("onStatefulSetCreateFailed(%s/%s) - unknown position", namespace, name))
}

// onStatefulSetUpdateFailed handles situation when StatefulSet update failed
// It can try to revert StatefulSet to its previous version, specified in rollbackStatefulSet
func (c *Controller) onStatefulSetUpdateFailed(rollbackStatefulSet *apps.StatefulSet) error {
	// Convenience shortcuts
	namespace := rollbackStatefulSet.Namespace
	name := rollbackStatefulSet.Name

	// What to do with StatefulSet - look into chop configuration settings
	switch c.chopConfig.OnStatefulSetUpdateFailureAction {
	case config.OnStatefulSetUpdateFailureActionAbort:
		// Do nothing, just report appropriate error
		glog.V(1).Infof("onStatefulSetUpdateFailed(%s/%s) - abort\n", namespace, name)
		return errors.New(fmt.Sprintf("Update failed on %s/%s", namespace, name))

	case config.OnStatefulSetUpdateFailureActionRollback:
		// Need to revert current StatefulSet to oldStatefulSet
		glog.V(1).Infof("onStatefulSetUpdateFailed(%s/%s) - going to ROLLBACK FAILED StatefulSet\n", namespace, name)
		if statefulSet, err := c.statefulSetLister.StatefulSets(namespace).Get(name); err != nil {
			// Unable to get StatefulSet
			return err
		} else {
			// Make copy of "previous" .Spec just to be sure nothing gets corrupted
			// Update StatefulSet to its 'previous' oldStatefulSet - this is expected to rollback inapplicable changes
			// Having StatefulSet .spec in rolled back status we need to delete current Pod - because in case of Pod being seriously broken,
			// it is the only way to go. Just delete Pod and StatefulSet will recreated Pod with current .spec
			// This will rollback Pod to previous .spec
			statefulSet.Spec = *rollbackStatefulSet.Spec.DeepCopy()
			statefulSet, err = c.kubeClient.AppsV1().StatefulSets(namespace).Update(statefulSet)
			_ = c.statefulSetDeletePod(statefulSet)

			return c.shouldContinueOnUpdateFailed()
		}
	default:
		glog.V(1).Infof("Unknown c.chopConfig.OnStatefulSetUpdateFailureAction=%s\n", c.chopConfig.OnStatefulSetUpdateFailureAction)
		return nil
	}

	return errors.New(fmt.Sprintf("onStatefulSetUpdateFailed(%s/%s) - unknown position", namespace, name))
}

// shouldContinueOnCreateFailed return nil in case 'continue' or error in case 'do not continue'
func (c *Controller) shouldContinueOnCreateFailed() error {
	// Check configuration option regarding should we continue when errors met on the way
	// c.chopConfig.OnStatefulSetUpdateFailureAction
	var continueUpdate = false
	if continueUpdate {
		// Continue update
		return nil
	}

	// Do not continue update
	return errors.New(fmt.Sprintf("Create stopped due to previous errors"))
}

// shouldContinueOnUpdateFailed return nil in case 'continue' or error in case 'do not continue'
func (c *Controller) shouldContinueOnUpdateFailed() error {
	// Check configuration option regarding should we continue when errors met on the way
	// c.chopConfig.OnStatefulSetUpdateFailureAction
	var continueUpdate = false
	if continueUpdate {
		// Continue update
		return nil
	}

	// Do not continue update
	return errors.New(fmt.Sprintf("Update stopped due to previous errors"))
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

// strStatefulSetStatus returns human-friendly string representation of StatefulSet status
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
