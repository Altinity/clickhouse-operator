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
	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/util"
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

// prepareBackupSecret makes sure the backup Secret of a backup-enabled CR is complete before the CR is
// normalized for reconcile, as the normalizer takes the backup user's password from it. A CR normalized
// before the Secret was complete lacks that user, so it is normalized anew, and reported as changed. Whether
// backup is enabled is told by the normalized CR, as backup may come from a template. A suspended CR is not
// reconciled, so its Secret is left as it is: new values would reach the sidecars, but not the users config.
// An error is left to reconcileBackupSecret, which fails the reconcile on it.
func (w *worker) prepareBackupSecret(
	ctx context.Context,
	cr *api.ClickHouseInstallation,
	normalize func() *api.ClickHouseInstallation,
) (*api.ClickHouseInstallation, bool) {
	if cr.Spec.Suspend.Value() {
		return cr, false
	}
	if changed, _ := w.ensureBackupSecret(ctx, cr); changed {
		return normalize(), true
	}
	return cr, false
}

// reconcileBackupSecret reconciles the Secret with the credentials of the CR's clickhouse-backup sidecars and
// the backup user, registering it so that it is not purged as an unknown object
func (w *worker) reconcileBackupSecret(ctx context.Context, cr *api.ClickHouseInstallation) error {
	if !cr.IsBackupEnabled() {
		return nil
	}

	secret := w.task.Creator().CreateBackupSecret()
	if _, err := w.reconcileSecretKeepValues(ctx, cr, secret); err != nil {
		w.task.RegistryFailed().RegisterSecret(secret.GetObjectMeta())
		return err
	}
	w.task.RegistryReconciled().RegisterSecret(secret.GetObjectMeta())
	return nil
}

// ensureBackupSecret ensures the backup Secret of a backup-enabled CR, reporting whether it has been changed
func (w *worker) ensureBackupSecret(ctx context.Context, cr *api.ClickHouseInstallation) (bool, error) {
	if !cr.IsBackupEnabled() {
		return false, nil
	}
	return w.reconcileSecretKeepValues(ctx, cr, w.task.Creator().CreateBackupSecret())
}

// reconcileSecretKeepValues reconciles core.Secret whose values, once generated, must never change: ClickHouse,
// the sidecars and the backup Jobs pick them up at different times, so a new password would lock one out of
// the other until every pod restarted. An existing Secret only gets the keys it lacks or has empty, and an
// error reading it is returned rather than taken for its absence. Reports whether the Secret has been changed.
func (w *worker) reconcileSecretKeepValues(ctx context.Context, cr api.ICustomResource, secret *core.Secret) (bool, error) {
	w.a.V(2).M(cr).S().Info("%s", secret.Name)
	defer w.a.V(2).M(cr).E().Info("%s", secret.Name)

	curSecret, err := w.c.getSecret(ctx, secret)
	switch {
	case apiErrors.IsNotFound(err):
		if err := w.createSecret(ctx, cr, secret); err != nil {
			return false, err
		}
		return true, nil
	case err != nil:
		w.a.WithEvent(cr, a.EventActionReconcile, a.EventReasonReconcileFailed).
			WithAction(cr).
			WithError(cr).
			M(cr).F().
			Error("FAILED to get Secret: %s err: %v", util.NamespaceNameString(secret), err)
		return false, err
	}

	filled := false
	for key, value := range secret.StringData {
		if len(curSecret.Data[key]) == 0 {
			if curSecret.Data == nil {
				curSecret.Data = make(map[string][]byte)
			}
			curSecret.Data[key] = []byte(value)
			filled = true
		}
	}
	if !filled {
		return false, nil
	}

	if _, err := w.c.kube.Secret().Update(ctx, curSecret); err != nil {
		w.a.WithEvent(cr, a.EventActionUpdate, a.EventReasonUpdateFailed).
			WithAction(cr).
			WithError(cr).
			M(cr).F().
			Error("Update Secret: %s failed with error: %v", util.NamespaceNameString(secret), err)
		return false, err
	}
	w.a.V(1).
		WithEvent(cr, a.EventActionUpdate, a.EventReasonUpdateCompleted).
		WithAction(cr).
		M(cr).F().
		Info("Update Secret: %s added missing keys", util.NamespaceNameString(secret))
	return true, nil
}
