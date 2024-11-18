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
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// reconcileSecret reconciles core.Secret
func (w *worker) reconcileSecret(ctx context.Context, cr api.ICustomResource, secret *core.Secret) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(cr).S().Info(secret.Name)
	defer w.a.V(2).M(cr).E().Info(secret.Name)

	// Check whether this object already exists
	if _, err := w.c.getSecret(ctx, secret); err == nil {
		// We have Secret - try to update it
		return nil
	}

	// Secret not found or broken. Try to recreate
	_ = w.c.deleteSecretIfExists(ctx, secret.Namespace, secret.Name)
	err := w.createSecret(ctx, cr, secret)
	if err != nil {
		w.a.WithEvent(cr, a.EventActionReconcile, a.EventReasonReconcileFailed).
			WithAction(cr).
			WithError(cr).
			M(cr).F().
			Error("FAILED to reconcile Secret: %s CHI: %s ", secret.Name, cr.GetName())
	}

	return err
}

// createSecret
func (w *worker) createSecret(ctx context.Context, cr api.ICustomResource, secret *core.Secret) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	err := w.c.createSecret(ctx, secret)
	if err == nil {
		w.a.V(1).
			WithEvent(cr, a.EventActionCreate, a.EventReasonCreateCompleted).
			WithAction(cr).
			M(cr).F().
			Info("Create Secret %s/%s", secret.Namespace, secret.Name)
	} else {
		w.a.WithEvent(cr, a.EventActionCreate, a.EventReasonCreateFailed).
			WithAction(cr).
			WithError(cr).
			M(cr).F().
			Error("Create Secret %s/%s failed with error %v", secret.Namespace, secret.Name, err)
	}

	return err
}
