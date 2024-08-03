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
	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

//func (w *worker) reconcileConfigMap(chk *apiChk.ClickHouseKeeperInstallation) error {
//	return w.c.reconcile(
//		chk,
//		&core.ConfigMap{},
//		w.task.Creator().CreateConfigMap(
//			interfaces.ConfigMapConfig,
//			chkConfig.NewConfigFilesGeneratorOptionsKeeper().SetSettings(chk.GetSpec().GetConfiguration().GetSettings()),
//		),
//		"ConfigMap",
//		func(_cur, _new client.Object) error {
//			cur, ok1 := _cur.(*core.ConfigMap)
//			new, ok2 := _new.(*core.ConfigMap)
//			if !ok1 || !ok2 {
//				return fmt.Errorf("unable to cast")
//			}
//			cur.Data = new.Data
//			cur.BinaryData = new.BinaryData
//			return nil
//		},
//	)
//}

// reconcileConfigMap reconciles core.ConfigMap which belongs to specified CHI
func (w *worker) reconcileConfigMap(
	ctx context.Context,
	chk *apiChk.ClickHouseKeeperInstallation,
	configMap *core.ConfigMap,
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(chk).S().P()
	defer w.a.V(2).M(chk).E().P()

	// Check whether this object already exists in k8s
	curConfigMap, err := w.c.getConfigMap(ctx, configMap.GetObjectMeta())

	if curConfigMap != nil {
		// We have ConfigMap - try to update it
		err = w.updateConfigMap(ctx, chk, configMap)
	}

	if apiErrors.IsNotFound(err) {
		// ConfigMap not found - even during Update process - try to create it
		err = w.createConfigMap(ctx, chk, configMap)
	}

	if err != nil {
		w.a.WithEvent(chk, common.EventActionReconcile, common.EventReasonReconcileFailed).
			WithStatusAction(chk).
			WithStatusError(chk).
			M(chk).F().
			Error("FAILED to reconcile ConfigMap: %s CHI: %s ", configMap.Name, chk.Name)
	}

	return err
}

// updateConfigMap
func (w *worker) updateConfigMap(ctx context.Context, chk *apiChk.ClickHouseKeeperInstallation, configMap *core.ConfigMap) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	updatedConfigMap, err := w.c.updateConfigMap(ctx, configMap)
	if err == nil {
		w.a.V(1).
			WithEvent(chk, common.EventActionUpdate, common.EventReasonUpdateCompleted).
			WithStatusAction(chk).
			M(chk).F().
			Info("Update ConfigMap %s/%s", configMap.Namespace, configMap.Name)
		if updatedConfigMap.ResourceVersion != configMap.ResourceVersion {
			w.task.SetCmUpdate(time.Now())
		}
	} else {
		w.a.WithEvent(chk, common.EventActionUpdate, common.EventReasonUpdateFailed).
			WithStatusAction(chk).
			WithStatusError(chk).
			M(chk).F().
			Error("Update ConfigMap %s/%s failed with error %v", configMap.Namespace, configMap.Name, err)
	}

	return err
}

// createConfigMap
func (w *worker) createConfigMap(ctx context.Context, chk *apiChk.ClickHouseKeeperInstallation, configMap *core.ConfigMap) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	err := w.c.createConfigMap(ctx, configMap)
	if err == nil {
		w.a.V(1).
			WithEvent(chk, common.EventActionCreate, common.EventReasonCreateCompleted).
			WithStatusAction(chk).
			M(chk).F().
			Info("Create ConfigMap %s", util.NamespaceNameString(configMap))
	} else {
		w.a.WithEvent(chk, common.EventActionCreate, common.EventReasonCreateFailed).
			WithStatusAction(chk).
			WithStatusError(chk).
			M(chk).F().
			Error("Create ConfigMap %s failed with error %v", util.NamespaceNameString(configMap), err)
	}

	return err
}
