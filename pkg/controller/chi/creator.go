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
	"github.com/altinity/clickhouse-operator/pkg/util"

	apps "k8s.io/api/apps/v1"
	"k8s.io/api/core/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// createStatefulSet is an internal function, used in reconcileStatefulSet only
func (c *Controller) createStatefulSet(statefulSet *apps.StatefulSet, host *chop.ChiHost) error {
	log.V(1).M(host).F().P()

	if _, err := c.kubeClient.AppsV1().StatefulSets(statefulSet.Namespace).Create(statefulSet); err != nil {
		// Unable to create StatefulSet at all
		return err
	}

	// StatefulSet created, wait until it is ready

	if err := c.waitHostReady(host); err == nil {
		// Target generation reached, StatefulSet created successfully
		return nil
	}

	// Unable to run StatefulSet, StatefulSet create failed, time to rollback?
	return c.onStatefulSetCreateFailed(statefulSet, host)
}

// updateStatefulSet is an internal function, used in reconcileStatefulSet only
func (c *Controller) updateStatefulSet(oldStatefulSet *apps.StatefulSet, newStatefulSet *apps.StatefulSet, host *chop.ChiHost) error {
	log.V(2).M(host).F().P()

	// Apply newStatefulSet and wait for Generation to change
	updatedStatefulSet, err := c.kubeClient.AppsV1().StatefulSets(newStatefulSet.Namespace).Update(newStatefulSet)
	if err != nil {
		// Update failed
		log.V(1).M(host).A().Error("%v", err)
		return err
	}

	// After calling "Update()"
	// 1. ObjectMeta.Generation is target generation
	// 2. Status.ObservedGeneration may be <= ObjectMeta.Generation

	if updatedStatefulSet.Generation == oldStatefulSet.Generation {
		// Generation is not updated - no changes in .spec section were made
		log.V(2).M(host).F().Info("no generation change")
		return nil
	}

	log.V(1).M(host).F().Info("generation change %d=>%d", oldStatefulSet.Generation, updatedStatefulSet.Generation)

	if err := c.waitHostReady(host); err == nil {
		// Target generation reached, StatefulSet updated successfully
		return nil
	} else {
		// Unable to run StatefulSet, StatefulSet update failed, time to rollback?
		return c.onStatefulSetUpdateFailed(oldStatefulSet, host)
	}

	return fmt.Errorf("unexpected flow")
}

// updateStatefulSet is an internal function, used in reconcileStatefulSet only
func (c *Controller) updatePersistentVolume(pv *v1.PersistentVolume) error {
	log.V(2).M(pv).F().P()

	// Apply newStatefulSet and wait for Generation to change
	_, err := c.kubeClient.CoreV1().PersistentVolumes().Update(pv)
	if err != nil {
		// Update failed
		log.V(1).M(pv).A().Error("%v", err)
		return err
	}

	return nil
}

// onStatefulSetCreateFailed handles situation when StatefulSet create failed
// It can just delete failed StatefulSet or do nothing
func (c *Controller) onStatefulSetCreateFailed(failedStatefulSet *apps.StatefulSet, host *chop.ChiHost) error {
	// What to do with StatefulSet - look into chop configuration settings
	switch c.chop.Config().OnStatefulSetCreateFailureAction {
	case chop.OnStatefulSetCreateFailureActionAbort:
		// Report appropriate error, it will break reconcile loop
		log.V(1).M(host).F().Info("abort")
		return errors.New(fmt.Sprintf("Create failed on %s", util.NamespaceNameString(failedStatefulSet.ObjectMeta)))

	case chop.OnStatefulSetCreateFailureActionDelete:
		// Delete gracefully failed StatefulSet
		log.V(1).M(host).F().Info("going to DELETE FAILED StatefulSet %s", util.NamespaceNameString(failedStatefulSet.ObjectMeta))
		_ = c.deleteHost(host)
		return c.shouldContinueOnCreateFailed()

	case chop.OnStatefulSetCreateFailureActionIgnore:
		// Ignore error, continue reconcile loop
		log.V(1).M(host).F().Info("going to ignore error %s", util.NamespaceNameString(failedStatefulSet.ObjectMeta))
		return nil

	default:
		log.V(1).M(host).A().Error("Unknown c.chop.Config().OnStatefulSetCreateFailureAction=%s", c.chop.Config().OnStatefulSetCreateFailureAction)
		return nil
	}

	return fmt.Errorf("unexpected flow")
}

// onStatefulSetUpdateFailed handles situation when StatefulSet update failed
// It can try to revert StatefulSet to its previous version, specified in rollbackStatefulSet
func (c *Controller) onStatefulSetUpdateFailed(rollbackStatefulSet *apps.StatefulSet, host *chop.ChiHost) error {
	// Convenience shortcuts
	namespace := rollbackStatefulSet.Namespace
	name := rollbackStatefulSet.Name

	// What to do with StatefulSet - look into chop configuration settings
	switch c.chop.Config().OnStatefulSetUpdateFailureAction {
	case chop.OnStatefulSetUpdateFailureActionAbort:
		// Report appropriate error, it will break reconcile loop
		log.V(1).M(host).F().Info("abort StatefulSet %s", util.NamespaceNameString(rollbackStatefulSet.ObjectMeta))
		return errors.New(fmt.Sprintf("Update failed on %s/%s", namespace, name))

	case chop.OnStatefulSetUpdateFailureActionRollback:
		// Need to revert current StatefulSet to oldStatefulSet
		log.V(1).M(host).F().Info("going to ROLLBACK FAILED StatefulSet %s", util.NamespaceNameString(rollbackStatefulSet.ObjectMeta))
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
			_ = c.statefulSetDeletePod(statefulSet, host)

			return c.shouldContinueOnUpdateFailed()
		}

	case chop.OnStatefulSetUpdateFailureActionIgnore:
		// Ignore error, continue reconcile loop
		log.V(1).M(host).F().Info("going to ignore error %s", util.NamespaceNameString(rollbackStatefulSet.ObjectMeta))
		return nil

	default:
		log.V(1).M(host).A().Error("Unknown c.chop.Config().OnStatefulSetUpdateFailureAction=%s", c.chop.Config().OnStatefulSetUpdateFailureAction)
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
