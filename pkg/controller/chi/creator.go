// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
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
	log "github.com/golang/glog"
	"k8s.io/api/core/v1"

	// log "k8s.io/klog"

	apps "k8s.io/api/apps/v1"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

const (
	waitStatefulSetGenerationTimeoutBeforeStartBothering = 60
	waitStatefulSetGenerationTimeoutToCreateStatefulSet  = 30
)

// createStatefulSet is an internal function, used in reconcileStatefulSet only
func (c *Controller) createStatefulSet(statefulSet *apps.StatefulSet, host *chop.ChiHost) error {
	log.V(1).Infof("Create StatefulSet %s/%s", statefulSet.Namespace, statefulSet.Name)
	if statefulSet, err := c.kubeClient.AppsV1().StatefulSets(statefulSet.Namespace).Create(statefulSet); err != nil {
		// Error call Create()
		return err
	} else if err := c.waitStatefulSetGeneration(statefulSet.Namespace, statefulSet.Name, statefulSet.Generation); err == nil {
		// Target generation reached, StatefulSet created successfully
		return nil
	} else {
		// Unable to reach target generation, StatefulSet create failed, time to rollback?
		return c.onStatefulSetCreateFailed(statefulSet, host)
	}

	return fmt.Errorf("unexpected flow")
}

// updateStatefulSet is an internal function, used in reconcileStatefulSet only
func (c *Controller) updateStatefulSet(oldStatefulSet *apps.StatefulSet, newStatefulSet *apps.StatefulSet) error {
	// Convenience shortcuts
	namespace := newStatefulSet.Namespace
	name := newStatefulSet.Name
	log.V(2).Infof("updateStatefulSet(%s/%s)", namespace, name)

	// Apply newStatefulSet and wait for Generation to change
	updatedStatefulSet, err := c.kubeClient.AppsV1().StatefulSets(namespace).Update(newStatefulSet)
	if err != nil {
		// Update failed
		log.V(1).Infof("updateStatefulSet(%s/%s) - git err: %v", namespace, name, err)
		return err
	}

	// After calling "Update()"
	// 1. ObjectMeta.Generation is target generation
	// 2. Status.ObservedGeneration may be <= ObjectMeta.Generation

	if updatedStatefulSet.Generation == oldStatefulSet.Generation {
		// Generation is not updated - no changes in .spec section were made
		log.V(2).Infof("updateStatefulSet(%s/%s) - no generation change", namespace, name)
		return nil
	}

	log.V(1).Infof("updateStatefulSet(%s/%s) - generation change %d=>%d", namespace, name, oldStatefulSet.Generation, updatedStatefulSet.Generation)

	if err := c.waitStatefulSetGeneration(namespace, name, updatedStatefulSet.Generation); err == nil {
		// Target generation reached, StatefulSet updated successfully
		return nil
	} else {
		// Unable to reach target generation, StatefulSet update failed, time to rollback?
		return c.onStatefulSetUpdateFailed(oldStatefulSet)
	}

	return fmt.Errorf("unexpected flow")
}

// updateStatefulSet is an internal function, used in reconcileStatefulSet only
func (c *Controller) updatePersistentVolume(pv *v1.PersistentVolume) error {
	// Convenience shortcuts
	namespace := pv.Namespace
	name := pv.Name
	log.V(2).Infof("updatePersistentVolume(%s/%s)", namespace, name)

	// Apply newStatefulSet and wait for Generation to change
	_, err := c.kubeClient.CoreV1().PersistentVolumes().Update(pv)
	if err != nil {
		// Update failed
		log.V(1).Infof("updatePersistentVolume(%s/%s) - git err: %v", namespace, name, err)
		return err
	}

	return nil
}

// waitStatefulSetGeneration polls StatefulSet for reaching target generation.
// Used in createStatefulSet, updateStatefulSet and deleteStatefulSet function only
func (c *Controller) waitStatefulSetGeneration(namespace, name string, targetGeneration int64) error {
	// Wait for some limited time for StatefulSet to reach target generation
	// Wait timeout is specified in c.chopConfig.StatefulSetUpdateTimeout in seconds
	start := time.Now()
	for {
		if statefulSet, err := c.statefulSetLister.StatefulSets(namespace).Get(name); err == nil {
			// Object is found
			if hasStatefulSetReachedGeneration(statefulSet, targetGeneration) {
				// StatefulSet is available and generation reached
				// All is good, job done, exit
				log.V(1).Infof("waitStatefulSetGeneration(%s/%s)-OK  :%s", namespace, name, strStatefulSetStatus(&statefulSet.Status))
				return nil
			}

			// Object is found, target generation not reached yet
			if time.Since(start) >= (time.Duration(waitStatefulSetGenerationTimeoutBeforeStartBothering) * time.Second) {
				// Generation not yet reached
				// Start bothering with log messages after some time only
				log.V(1).Infof("waitStatefulSetGeneration(%s/%s)-WAIT:%s", namespace, name, strStatefulSetStatus(&statefulSet.Status))
			}
		} else if apierrors.IsNotFound(err) {
			// Object is not found - it either failed to be created or just still not created
			if time.Since(start) >= (time.Duration(waitStatefulSetGenerationTimeoutToCreateStatefulSet) * time.Second) {
				// No more wait for object to be created. Consider create as failed.
				log.V(1).Infof("ERROR waitStatefulSetGeneration(%s/%s) Get() FAILED - StatefulSet still not found, abort", namespace, name)
				return err
			}
			// Object with such name not found - may be is still being created - wait for it
			log.V(1).Infof("waitStatefulSetGeneration(%s/%s)-WAIT: object not yet created, need to wait", namespace, name)
		} else {
			// Some kind of total error
			log.Errorf("ERROR waitStatefulSetGeneration(%s/%s) Get() FAILED", namespace, name)
			return err
		}

		// StatefulSet is either not created or generation is not yet reached

		if time.Since(start) >= (time.Duration(c.chop.Config().StatefulSetUpdateTimeout) * time.Second) {
			// Timeout reached, no good result available, time to quit
			log.V(1).Infof("ERROR waitStatefulSetGeneration(%s/%s) - TIMEOUT reached", namespace, name)
			return errors.New(fmt.Sprintf("waitStatefulSetGeneration(%s/%s) - wait timeout", namespace, name))
		}

		// Wait some more time
		log.V(2).Infof("waitStatefulSetGeneration(%s/%s)", namespace, name)
		select {
		case <-time.After(time.Duration(c.chop.Config().StatefulSetUpdatePollPeriod) * time.Second):
		}
	}

	return fmt.Errorf("unexpected flow")
}

// onStatefulSetCreateFailed handles situation when StatefulSet create failed
// It can just delete failed StatefulSet or do nothing
func (c *Controller) onStatefulSetCreateFailed(failedStatefulSet *apps.StatefulSet, host *chop.ChiHost) error {
	// Convenience shortcuts
	namespace := failedStatefulSet.Namespace
	name := failedStatefulSet.Name

	// What to do with StatefulSet - look into chop configuration settings
	switch c.chop.Config().OnStatefulSetCreateFailureAction {
	case chop.OnStatefulSetCreateFailureActionAbort:
		// Report appropriate error, it will break reconcile loop
		log.V(1).Infof("onStatefulSetCreateFailed(%s/%s) - abort", namespace, name)
		return errors.New(fmt.Sprintf("Create failed on %s/%s", namespace, name))

	case chop.OnStatefulSetCreateFailureActionDelete:
		// Delete gracefully failed StatefulSet
		log.V(1).Infof("onStatefulSetCreateFailed(%s/%s) - going to DELETE FAILED StatefulSet", namespace, name)
		_ = c.deleteHost(host)
		return c.shouldContinueOnCreateFailed()

	case chop.OnStatefulSetCreateFailureActionIgnore:
		// Ignore error, continue reconcile loop
		log.V(1).Infof("onStatefulSetCreateFailed(%s/%s) - going to ignore error", namespace, name)
		return nil

	default:
		log.V(1).Infof("Unknown c.chop.Config().OnStatefulSetCreateFailureAction=%s", c.chop.Config().OnStatefulSetCreateFailureAction)
		return nil
	}

	return fmt.Errorf("unexpected flow")
}

// onStatefulSetUpdateFailed handles situation when StatefulSet update failed
// It can try to revert StatefulSet to its previous version, specified in rollbackStatefulSet
func (c *Controller) onStatefulSetUpdateFailed(rollbackStatefulSet *apps.StatefulSet) error {
	// Convenience shortcuts
	namespace := rollbackStatefulSet.Namespace
	name := rollbackStatefulSet.Name

	// What to do with StatefulSet - look into chop configuration settings
	switch c.chop.Config().OnStatefulSetUpdateFailureAction {
	case chop.OnStatefulSetUpdateFailureActionAbort:
		// Report appropriate error, it will break reconcile loop
		log.V(1).Infof("onStatefulSetUpdateFailed(%s/%s) - abort", namespace, name)
		return errors.New(fmt.Sprintf("Update failed on %s/%s", namespace, name))

	case chop.OnStatefulSetUpdateFailureActionRollback:
		// Need to revert current StatefulSet to oldStatefulSet
		log.V(1).Infof("onStatefulSetUpdateFailed(%s/%s) - going to ROLLBACK FAILED StatefulSet", namespace, name)
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

	case chop.OnStatefulSetUpdateFailureActionIgnore:
		// Ignore error, continue reconcile loop
		log.V(1).Infof("onStatefulSetUpdateFailed(%s/%s) - going to ignore error", namespace, name)
		return nil

	default:
		log.V(1).Infof("Unknown c.chop.Config().OnStatefulSetUpdateFailureAction=%s", c.chop.Config().OnStatefulSetUpdateFailureAction)
		return nil
	}

	return fmt.Errorf("unexpected flow")
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
	return fmt.Errorf("create stopped due to previous errors")
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
	return fmt.Errorf("update stopped due to previous errors")
}

// hasStatefulSetReachedGeneration returns whether has StatefulSet reached the expected generation after upgrade or not
func hasStatefulSetReachedGeneration(statefulSet *apps.StatefulSet, generation int64) bool {
	if statefulSet == nil {
		return false
	}

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
