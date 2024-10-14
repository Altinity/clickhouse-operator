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

	apps "k8s.io/api/apps/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// OnStatefulSetCreateFailed handles situation when StatefulSet create failed on k8s level
func (c *Controller) OnStatefulSetCreateFailed(ctx context.Context, host *api.Host) common.ErrorCRUD {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return common.ErrCRUDIgnore
	}

	// What to do with StatefulSet - look into chop configuration settings
	switch chop.Config().Reconcile.StatefulSet.Create.OnFailure {
	case api.OnStatefulSetCreateFailureActionAbort:
		// Report appropriate error, it will break reconcile loop
		log.V(1).M(host).F().Info("abort")
		return common.ErrCRUDAbort

	case api.OnStatefulSetCreateFailureActionDelete:
		// Delete gracefully failed StatefulSet
		log.V(1).M(host).F().Info(
			"going to DELETE FAILED StatefulSet %s",
			util.NamespaceNameString(host.Runtime.DesiredStatefulSet.GetObjectMeta()))
		_ = c.deleteHost(ctx, host)
		return c.shouldContinueOnCreateFailed()

	case api.OnStatefulSetCreateFailureActionIgnore:
		// Ignore error, continue reconcile loop
		log.V(1).M(host).F().Info(
			"going to ignore error %s",
			util.NamespaceNameString(host.Runtime.DesiredStatefulSet.GetObjectMeta()))
		return common.ErrCRUDIgnore

	default:
		log.V(1).M(host).F().Error(
			"Unknown c.chop.Config().OnStatefulSetCreateFailureAction=%s",
			chop.Config().Reconcile.StatefulSet.Create.OnFailure)
		return common.ErrCRUDIgnore
	}

	return common.ErrCRUDUnexpectedFlow
}

// OnStatefulSetUpdateFailed handles situation when StatefulSet update failed in k8s level
// It can try to revert StatefulSet to its previous version, specified in rollbackStatefulSet
func (c *Controller) OnStatefulSetUpdateFailed(ctx context.Context, rollbackStatefulSet *apps.StatefulSet, host *api.Host, kubeSTS interfaces.IKubeSTS) common.ErrorCRUD {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return common.ErrCRUDIgnore
	}

	// What to do with StatefulSet - look into chop configuration settings
	switch chop.Config().Reconcile.StatefulSet.Update.OnFailure {
	case api.OnStatefulSetUpdateFailureActionAbort:
		// Report appropriate error, it will break reconcile loop
		log.V(1).M(host).F().Info("abort StatefulSet %s", util.NamespaceNameString(rollbackStatefulSet.GetObjectMeta()))
		return common.ErrCRUDAbort

	case api.OnStatefulSetUpdateFailureActionRollback:
		// Need to revert current StatefulSet to oldStatefulSet
		log.V(1).M(host).F().Info("going to ROLLBACK FAILED StatefulSet %s", util.NamespaceNameString(rollbackStatefulSet.GetObjectMeta()))
		curStatefulSet, err := kubeSTS.Get(ctx, host)
		if err != nil {
			log.V(1).M(host).F().Warning("Unable to fetch current StatefulSet %s. err: %q", util.NamespaceNameString(rollbackStatefulSet.GetObjectMeta()), err)
			return c.shouldContinueOnUpdateFailed()
		}

		// Make copy of "rollback to" .Spec just to be sure nothing gets corrupted
		// Update StatefulSet to its 'rollback to' StatefulSet - this is expected to rollback inapplicable changes
		// Having StatefulSet .spec in rolled back status we need to delete current Pod - because in case of Pod
		// being seriously broken, it is the only way to go.
		// Just delete Pod and StatefulSet will recreated Pod with current .spec
		// This will rollback Pod to "rollback to" .spec
		curStatefulSet.Spec = *rollbackStatefulSet.Spec.DeepCopy()
		curStatefulSet, _ = kubeSTS.Update(ctx, curStatefulSet)
		_ = c.statefulSetDeletePod(ctx, curStatefulSet, host)

		return c.shouldContinueOnUpdateFailed()

	case api.OnStatefulSetUpdateFailureActionIgnore:
		// Ignore error, continue reconcile loop
		log.V(1).M(host).F().Info("going to ignore error %s", util.NamespaceNameString(rollbackStatefulSet.GetObjectMeta()))
		return common.ErrCRUDIgnore

	default:
		log.V(1).M(host).F().Error("Unknown c.chop.Config().OnStatefulSetUpdateFailureAction=%s", chop.Config().Reconcile.StatefulSet.Update.OnFailure)
		return common.ErrCRUDIgnore
	}

	return common.ErrCRUDUnexpectedFlow
}

// shouldContinueOnCreateFailed return nil in case 'continue' or error in case 'do not continue'
func (c *Controller) shouldContinueOnCreateFailed() common.ErrorCRUD {
	// Check configuration option regarding should we continue when errors met on the way
	// c.chopConfig.OnStatefulSetUpdateFailureAction
	var continueUpdate = false
	if continueUpdate {
		// Continue update
		return common.ErrCRUDIgnore
	}

	// Do not continue update
	return common.ErrCRUDAbort
}

// shouldContinueOnUpdateFailed return nil in case 'continue' or error in case 'do not continue'
func (c *Controller) shouldContinueOnUpdateFailed() common.ErrorCRUD {
	// Check configuration option regarding should we continue when errors met on the way
	// c.chopConfig.OnStatefulSetUpdateFailureAction
	var continueUpdate = false
	if continueUpdate {
		// Continue update
		return common.ErrCRUDIgnore
	}

	// Do not continue update
	return common.ErrCRUDAbort
}
