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
	"fmt"

	"gopkg.in/d4l3k/messagediff.v1"
	appsV1 "k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chiV1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// createStatefulSet is an internal function, used in reconcileStatefulSet only
func (c *Controller) createStatefulSet(ctx context.Context, host *chiV1.ChiHost) ErrorCRUD {
	log.V(1).M(host).F().P()

	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	statefulSet := host.DesiredStatefulSet

	log.V(1).Info("Create StatefulSet %s/%s", statefulSet.Namespace, statefulSet.Name)
	if _, err := c.kubeClient.AppsV1().StatefulSets(statefulSet.Namespace).Create(ctx, statefulSet, newCreateOptions()); err != nil {
		log.V(1).M(host).F().Error("StatefulSet create failed. err: %v", err)
		return errCRUDRecreate
	}

	// StatefulSet created, wait until host is ready
	if err := c.waitHostReady(ctx, host); err != nil {
		log.V(1).M(host).F().Error("StatefulSet create wait failed. err: %v", err)
		return c.onStatefulSetCreateFailed(ctx, host)
	}

	log.V(2).M(host).F().Info("Target generation reached, StatefulSet created successfully")
	return nil
}

// updateStatefulSet is an internal function, used in reconcileStatefulSet only
func (c *Controller) updateStatefulSet(
	ctx context.Context,
	oldStatefulSet *appsV1.StatefulSet,
	newStatefulSet *appsV1.StatefulSet,
	host *chiV1.ChiHost,
) ErrorCRUD {
	log.V(2).M(host).F().P()

	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	// Apply newStatefulSet and wait for Generation to change
	updatedStatefulSet, err := c.kubeClient.AppsV1().StatefulSets(newStatefulSet.Namespace).Update(ctx, newStatefulSet, newUpdateOptions())
	if err != nil {
		log.V(1).M(host).F().Error("StatefulSet update failed. err: %v", err)
		diff, equal := messagediff.DeepDiff(oldStatefulSet.Spec, newStatefulSet.Spec)

		str := ""
		if equal {
			str += "EQUAL: "
		} else {
			str += "NOT EQUAL: "
		}

		if len(diff.Added) > 0 {
			// Something added
			str += util.MessageDiffItemString("added spec items", "", diff.Added)
		}

		if len(diff.Removed) > 0 {
			// Something removed
			str += util.MessageDiffItemString("removed spec items", "", diff.Removed)
		}

		if len(diff.Modified) > 0 {
			// Something modified
			str += util.MessageDiffItemString("modified spec items", "", diff.Modified)
		}
		log.V(1).M(host).F().Error("%s", str)

		return errCRUDRecreate
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

	if err := c.waitHostReady(ctx, host); err != nil {
		log.V(1).M(host).F().Error("StatefulSet update wait failed. err: %v", err)
		return c.onStatefulSetUpdateFailed(ctx, oldStatefulSet, host)
	}

	log.V(2).M(host).F().Info("Target generation reached, StatefulSet updated successfully")
	return nil
}

// Comment out PV
// updatePersistentVolume
//func (c *Controller) updatePersistentVolume(ctx context.Context, pv *coreV1.PersistentVolume) (*coreV1.PersistentVolume, error) {
//	log.V(2).M(pv).F().P()
//	if util.IsContextDone(ctx) {
//		log.V(2).Info("task is done")
//		return nil, fmt.Errorf("task is done")
//	}
//
//	var err error
//	pv, err = c.kubeClient.CoreV1().PersistentVolumes().Update(ctx, pv, newUpdateOptions())
//	if err != nil {
//		// Update failed
//		log.V(1).M(pv).F().Error("%v", err)
//		return nil, err
//	}
//
//	return pv, err
//}

// updatePersistentVolumeClaim
func (c *Controller) updatePersistentVolumeClaim(ctx context.Context, pvc *coreV1.PersistentVolumeClaim) (*coreV1.PersistentVolumeClaim, error) {
	log.V(2).M(pvc).F().P()
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil, fmt.Errorf("task is done")
	}

	_, err := c.kubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, newGetOptions())
	if err != nil {
		if apiErrors.IsNotFound(err) {
			// This is not an error per se, means PVC is not created (yet)?
			_, err = c.kubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Create(ctx, pvc, newCreateOptions())
			if err != nil {
				log.V(1).M(pvc).F().Error("unable to Create PVC err: %v", err)
			}
			return pvc, err
		}
		// Any non-NotFound API error - unable to proceed
		log.V(1).M(pvc).F().Error("ERROR unable to get PVC(%s/%s) err: %v", pvc.Namespace, pvc.Name, err)
		return nil, err
	}

	_, err = c.kubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, newUpdateOptions())
	if err != nil {
		// Update failed
		//if strings.Contains(err.Error(), "field can not be less than previous value") {
		//	return pvc, nil
		//} else {
		log.V(1).M(pvc).F().Error("unable to Update PVC err: %v", err)
		//	return nil, err
		//}
	}
	return pvc, err
}

// onStatefulSetCreateFailed handles situation when StatefulSet create failed
// It can just delete failed StatefulSet or do nothing
func (c *Controller) onStatefulSetCreateFailed(ctx context.Context, host *chiV1.ChiHost) ErrorCRUD {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return errCRUDIgnore
	}

	// What to do with StatefulSet - look into chop configuration settings
	switch chop.Config().Reconcile.StatefulSet.Create.OnFailure {
	case chiV1.OnStatefulSetCreateFailureActionAbort:
		// Report appropriate error, it will break reconcile loop
		log.V(1).M(host).F().Info("abort")
		return errCRUDAbort

	case chiV1.OnStatefulSetCreateFailureActionDelete:
		// Delete gracefully failed StatefulSet
		log.V(1).M(host).F().Info("going to DELETE FAILED StatefulSet %s", util.NamespaceNameString(host.DesiredStatefulSet.ObjectMeta))
		_ = c.deleteHost(ctx, host)
		return c.shouldContinueOnCreateFailed()

	case chiV1.OnStatefulSetCreateFailureActionIgnore:
		// Ignore error, continue reconcile loop
		log.V(1).M(host).F().Info("going to ignore error %s", util.NamespaceNameString(host.DesiredStatefulSet.ObjectMeta))
		return errCRUDIgnore

	default:
		log.V(1).M(host).F().Error("Unknown c.chop.Config().OnStatefulSetCreateFailureAction=%s", chop.Config().Reconcile.StatefulSet.Create.OnFailure)
		return errCRUDIgnore
	}

	return errCRUDUnexpectedFlow
}

// onStatefulSetUpdateFailed handles situation when StatefulSet update failed
// It can try to revert StatefulSet to its previous version, specified in rollbackStatefulSet
func (c *Controller) onStatefulSetUpdateFailed(ctx context.Context, rollbackStatefulSet *appsV1.StatefulSet, host *chiV1.ChiHost) ErrorCRUD {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return errCRUDIgnore
	}

	// Convenience shortcuts
	namespace := rollbackStatefulSet.Namespace
	name := rollbackStatefulSet.Name

	// What to do with StatefulSet - look into chop configuration settings
	switch chop.Config().Reconcile.StatefulSet.Update.OnFailure {
	case chiV1.OnStatefulSetUpdateFailureActionAbort:
		// Report appropriate error, it will break reconcile loop
		log.V(1).M(host).F().Info("abort StatefulSet %s", util.NamespaceNameString(rollbackStatefulSet.ObjectMeta))
		return errCRUDAbort

	case chiV1.OnStatefulSetUpdateFailureActionRollback:
		// Need to revert current StatefulSet to oldStatefulSet
		log.V(1).M(host).F().Info("going to ROLLBACK FAILED StatefulSet %s", util.NamespaceNameString(rollbackStatefulSet.ObjectMeta))
		statefulSet, err := c.kubeClient.AppsV1().StatefulSets(namespace).Get(ctx, name, newGetOptions())
		if err != nil {
			log.V(1).M(host).F().Warning("Unable to fetch current StatefulSet %s. err: %q", util.NamespaceNameString(rollbackStatefulSet.ObjectMeta), err)
			return c.shouldContinueOnUpdateFailed()
		}

		// Make copy of "previous" .Spec just to be sure nothing gets corrupted
		// Update StatefulSet to its 'previous' oldStatefulSet - this is expected to rollback inapplicable changes
		// Having StatefulSet .spec in rolled back status we need to delete current Pod - because in case of Pod being seriously broken,
		// it is the only way to go. Just delete Pod and StatefulSet will recreated Pod with current .spec
		// This will rollback Pod to previous .spec
		statefulSet.Spec = *rollbackStatefulSet.Spec.DeepCopy()
		statefulSet, _ = c.kubeClient.AppsV1().StatefulSets(namespace).Update(ctx, statefulSet, newUpdateOptions())
		_ = c.statefulSetDeletePod(ctx, statefulSet, host)

		return c.shouldContinueOnUpdateFailed()

	case chiV1.OnStatefulSetUpdateFailureActionIgnore:
		// Ignore error, continue reconcile loop
		log.V(1).M(host).F().Info("going to ignore error %s", util.NamespaceNameString(rollbackStatefulSet.ObjectMeta))
		return errCRUDIgnore

	default:
		log.V(1).M(host).F().Error("Unknown c.chop.Config().OnStatefulSetUpdateFailureAction=%s", chop.Config().Reconcile.StatefulSet.Update.OnFailure)
		return errCRUDIgnore
	}

	return errCRUDUnexpectedFlow
}

// shouldContinueOnCreateFailed return nil in case 'continue' or error in case 'do not continue'
func (c *Controller) shouldContinueOnCreateFailed() ErrorCRUD {
	// Check configuration option regarding should we continue when errors met on the way
	// c.chopConfig.OnStatefulSetUpdateFailureAction
	var continueUpdate = false
	if continueUpdate {
		// Continue update
		return errCRUDIgnore
	}

	// Do not continue update
	return errCRUDAbort
}

// shouldContinueOnUpdateFailed return nil in case 'continue' or error in case 'do not continue'
func (c *Controller) shouldContinueOnUpdateFailed() ErrorCRUD {
	// Check configuration option regarding should we continue when errors met on the way
	// c.chopConfig.OnStatefulSetUpdateFailureAction
	var continueUpdate = false
	if continueUpdate {
		// Continue update
		return errCRUDIgnore
	}

	// Do not continue update
	return errCRUDAbort
}

func (c *Controller) createSecret(ctx context.Context, secret *coreV1.Secret) error {
	log.V(1).M(secret).F().P()

	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	log.V(1).Info("Create Secret %s/%s", secret.Namespace, secret.Name)
	if _, err := c.kubeClient.CoreV1().Secrets(secret.Namespace).Create(ctx, secret, newCreateOptions()); err != nil {
		// Unable to create StatefulSet at all
		log.V(1).Error("Create Secret %s/%s failed err:%v", secret.Namespace, secret.Name, err)
		return err
	}

	return nil
}
