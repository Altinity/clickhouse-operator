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
	"time"

	core "k8s.io/api/core/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// updateConfigMap
func (w *worker) updateConfigMap(ctx context.Context, chi *api.ClickHouseInstallation, configMap *core.ConfigMap) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	updatedConfigMap, err := w.c.kubeClient.CoreV1().ConfigMaps(configMap.Namespace).Update(ctx, configMap, controller.NewUpdateOptions())
	if err == nil {
		w.a.V(1).
			WithEvent(chi, common.EventActionUpdate, common.EventReasonUpdateCompleted).
			WithStatusAction(chi).
			M(chi).F().
			Info("Update ConfigMap %s/%s", configMap.Namespace, configMap.Name)
		if updatedConfigMap.ResourceVersion != configMap.ResourceVersion {
			w.task.CmUpdate = time.Now()
		}
	} else {
		w.a.WithEvent(chi, common.EventActionUpdate, common.EventReasonUpdateFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).F().
			Error("Update ConfigMap %s/%s failed with error %v", configMap.Namespace, configMap.Name, err)
	}

	return err
}

// createConfigMap
func (w *worker) createConfigMap(ctx context.Context, chi *api.ClickHouseInstallation, configMap *core.ConfigMap) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	_, err := w.c.kubeClient.CoreV1().ConfigMaps(configMap.Namespace).Create(ctx, configMap, controller.NewCreateOptions())
	if err == nil {
		w.a.V(1).
			WithEvent(chi, common.EventActionCreate, common.EventReasonCreateCompleted).
			WithStatusAction(chi).
			M(chi).F().
			Info("Create ConfigMap %s/%s", configMap.Namespace, configMap.Name)
	} else {
		w.a.WithEvent(chi, common.EventActionCreate, common.EventReasonCreateFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).F().
			Error("Create ConfigMap %s/%s failed with error %v", configMap.Namespace, configMap.Name, err)
	}

	return err
}
