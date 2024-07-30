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

	core "k8s.io/api/core/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// createSecret
func (w *worker) createSecret(ctx context.Context, chi *api.ClickHouseInstallation, secret *core.Secret) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	_, err := w.c.kubeClient.CoreV1().Secrets(secret.Namespace).Create(ctx, secret, controller.NewCreateOptions())
	if err == nil {
		w.a.V(1).
			WithEvent(chi, common.EventActionCreate, common.EventReasonCreateCompleted).
			WithStatusAction(chi).
			M(chi).F().
			Info("Create Secret %s/%s", secret.Namespace, secret.Name)
	} else {
		w.a.WithEvent(chi, common.EventActionCreate, common.EventReasonCreateFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).F().
			Error("Create Secret %s/%s failed with error %v", secret.Namespace, secret.Name, err)
	}

	return err
}
