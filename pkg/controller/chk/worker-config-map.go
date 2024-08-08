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

package chk

import (
	"context"
	"time"

	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// reconcileConfigMap reconciles core.ConfigMap which belongs to specified CHI
func (w *worker) reconcileConfigMap(
	ctx context.Context,
	cr apiChi.ICustomResource,
	configMap *core.ConfigMap,
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(cr).S().P()
	defer w.a.V(2).M(cr).E().P()

	// Check whether this object already exists in k8s
	curConfigMap, err := w.c.getConfigMap(ctx, configMap.GetObjectMeta())

	if curConfigMap != nil {
		// We have ConfigMap - try to update it
		err = w.updateConfigMap(ctx, cr, configMap)
	}

	if apiErrors.IsNotFound(err) {
		// ConfigMap not found - even during Update process - try to create it
		err = w.createConfigMap(ctx, cr, configMap)
	}

	if err != nil {
		w.a.WithEvent(cr, common.EventActionReconcile, common.EventReasonReconcileFailed).
			WithStatusAction(cr).
			WithStatusError(cr).
			M(cr).F().
			Error("FAILED to reconcile ConfigMap: %s CHI: %s ", configMap.GetName(), cr.GetName())
	}

	return err
}

// updateConfigMap
func (w *worker) updateConfigMap(ctx context.Context, cr apiChi.ICustomResource, configMap *core.ConfigMap) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	updatedConfigMap, err := w.c.updateConfigMap(ctx, configMap)
	if err == nil {
		w.a.V(1).
			WithEvent(cr, common.EventActionUpdate, common.EventReasonUpdateCompleted).
			WithStatusAction(cr).
			M(cr).F().
			Info("Update ConfigMap %s/%s", configMap.Namespace, configMap.Name)
		if updatedConfigMap.ResourceVersion != configMap.ResourceVersion {
			w.task.SetCmUpdate(time.Now())
		}
	} else {
		w.a.WithEvent(cr, common.EventActionUpdate, common.EventReasonUpdateFailed).
			WithStatusAction(cr).
			WithStatusError(cr).
			M(cr).F().
			Error("Update ConfigMap %s/%s failed with error %v", configMap.Namespace, configMap.Name, err)
	}

	return err
}

// createConfigMap
func (w *worker) createConfigMap(ctx context.Context, cr apiChi.ICustomResource, configMap *core.ConfigMap) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	err := w.c.createConfigMap(ctx, configMap)
	if err == nil {
		w.a.V(1).
			WithEvent(cr, common.EventActionCreate, common.EventReasonCreateCompleted).
			WithStatusAction(cr).
			M(cr).F().
			Info("Create ConfigMap %s", util.NamespaceNameString(configMap))
	} else {
		w.a.WithEvent(cr, common.EventActionCreate, common.EventReasonCreateFailed).
			WithStatusAction(cr).
			WithStatusError(cr).
			M(cr).F().
			Error("Create ConfigMap %s failed with error %v", util.NamespaceNameString(configMap), err)
	}

	return err
}
