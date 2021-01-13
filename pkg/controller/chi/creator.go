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
	"context"
	"errors"
	"fmt"
	"github.com/altinity/clickhouse-operator/pkg/util"

	log "github.com/golang/glog"
	// log "k8s.io/klog"
	"k8s.io/api/core/v1"

	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	apps "k8s.io/api/apps/v1"
)

// createStatefulSet is an internal function, used in reconcileStatefulSet only
func (c *Controller) createStatefulSet(ctx context.Context, statefulSet *apps.StatefulSet, host *chop.ChiHost) error {
	if util.IsContextDone(ctx) {
		return nil
	}
	log.V(1).Infof("Create StatefulSet %s/%s", statefulSet.Namespace, statefulSet.Name)
	if statefulSet, err := c.kubeClient.AppsV1().StatefulSets(statefulSet.Namespace).Create(statefulSet); err != nil {
		// Error call Create()
		return err
	} else if err := c.waitHostReady(ctx, host); err == nil {
		// Target generation reached, StatefulSet created successfully
		return nil
	} else {
		// Unable to run StatefulSet, StatefulSet create failed, time to rollback?
		return c.onStatefulSetCreateFailed(ctx, statefulSet, host)
	}

	return fmt.Errorf("unexpected flow")
}

// updateStatefulSet is an internal function, used in reconcileStatefulSet only
func (c *Controller) updateStatefulSet(
	ctx context.Context,
	oldStatefulSet *apps.StatefulSet,
	newStatefulSet *apps.StatefulSet,
	host *chop.ChiHost,
) error {
	if util.IsContextDone(ctx) {
		return nil
	}
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

	if err := c.waitHostReady(ctx, host); err == nil {
		// Target generation reached, StatefulSet updated successfully
		return nil
	} else {
		// Unable to run StatefulSet, StatefulSet update failed, time to rollback?
		return c.onStatefulSetUpdateFailed(ctx, oldStatefulSet)
	}

	return fmt.Errorf("unexpected flow")
}

// updateStatefulSet is an internal function, used in reconcileStatefulSet only
func (c *Controller) updatePersistentVolume(ctx context.Context, pv *v1.PersistentVolume) error {
	if util.IsContextDone(ctx) {
		return nil
	}
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

// onStatefulSetCreateFailed handles situation when StatefulSet create failed
// It can just delete failed StatefulSet or do nothing
func (c *Controller) onStatefulSetCreateFailed(ctx context.Context, failedStatefulSet *apps.StatefulSet, host *chop.ChiHost) error {
	if util.IsContextDone(ctx) {
		return nil
	}
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
		_ = c.deleteHost(ctx, host)
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
func (c *Controller) onStatefulSetUpdateFailed(ctx context.Context, rollbackStatefulSet *apps.StatefulSet) error {
	if util.IsContextDone(ctx) {
		return nil
	}
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
			_ = c.statefulSetDeletePod(ctx, statefulSet)

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
