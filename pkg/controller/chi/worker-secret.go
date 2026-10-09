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
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer"
)

// reconcileSecret reconciles core.Secret
func (w *worker) reconcileSecret(ctx context.Context, cr api.ICustomResource, secret *core.Secret) error {
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

// reconcileHotReloadUsersSecret regenerates chop-generated-hot-reload-users.xml
// from the referenced Secret keys and writes it to the CHI-owned Secret. Normal
// users stay in the users ConfigMap. The CHI spec is not modified. A read
// failure returns before any write.
func (w *worker) reconcileHotReloadUsersSecret(ctx context.Context, cr *api.ClickHouseInstallation) error {
	usersXML, err := normalizer.RenderHotReloadUsersXML(
		cr.GetSpecT().GetConfiguration().GetUsers(),
		cr.GetNamespace(),
		func(namespace, name string) (*core.Secret, error) {
			return w.c.kube.Secret().Get(ctx, &core.Secret{
				ObjectMeta: meta.ObjectMeta{Namespace: namespace, Name: name},
			})
		},
	)
	if err != nil {
		w.a.WithEvent(cr, a.EventActionReconcile, a.EventReasonReconcileFailed).
			WithAction(cr).
			WithError(cr).
			M(cr).F().
			Error("FAILED to render hot-reload users configuration: %s", err)
		return err
	}
	secret := w.task.Creator().CreateHotReloadUsersSecret(config.ChopGeneratedHotReloadUsersConfigFilename(), usersXML)
	return w.reconcileHotReloadSecretData(ctx, cr, secret)
}

// reconcileHotReloadSecretData creates the managed Secret, or updates it when
// its data changed. An unchanged Secret is left alone so a resync does not
// write and re-enqueue itself.
func (w *worker) reconcileHotReloadSecretData(ctx context.Context, cr api.ICustomResource, desired *core.Secret) error {
	cur, err := w.c.getSecret(ctx, desired)
	if apiErrors.IsNotFound(err) {
		err = w.createSecret(ctx, cr, desired)
		if err == nil {
			w.task.RegistryReconciled().RegisterSecret(desired.GetObjectMeta())
		} else {
			w.task.RegistryFailed().RegisterSecret(desired.GetObjectMeta())
		}
		return err
	}
	if err != nil {
		w.task.RegistryFailed().RegisterSecret(desired.GetObjectMeta())
		return err
	}
	if apiequality.Semantic.DeepEqual(cur.Data, desired.Data) {
		w.task.RegistryReconciled().RegisterSecret(cur.GetObjectMeta())
		return nil
	}
	cur.Data = desired.Data
	updated, err := w.c.kube.Secret().Update(ctx, cur)
	if err != nil {
		w.a.WithEvent(cr, a.EventActionUpdate, a.EventReasonUpdateFailed).
			WithAction(cr).
			WithError(cr).
			M(cr).F().
			Error("Update Secret %s/%s failed with error %v", desired.Namespace, desired.Name, err)
		w.task.RegistryFailed().RegisterSecret(desired.GetObjectMeta())
		return err
	}
	w.a.V(1).
		WithEvent(cr, a.EventActionUpdate, a.EventReasonUpdateCompleted).
		WithAction(cr).
		M(cr).F().
		Info("Update Secret %s/%s", desired.Namespace, desired.Name)
	w.task.RegistryReconciled().RegisterSecret(updated.GetObjectMeta())
	return nil
}
