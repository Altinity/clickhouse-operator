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
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi/cmd_queue"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// getSecret gets secret
func (c *Controller) getSecret(ctx context.Context, secret *core.Secret) (*core.Secret, error) {
	return c.kube.Secret().Get(ctx, secret)
}

func (c *Controller) createSecret(ctx context.Context, secret *core.Secret) error {
	log.V(1).Info("Create Secret %s", util.NamespacedName(secret))
	if _, err := c.kube.Secret().Create(ctx, secret); err != nil {
		// Unable to create StatefulSet at all
		log.V(1).Error("Create Secret %s failed err: %v", util.NamespacedName(secret), err)
		return err
	}

	return nil
}

// enqueueBackupSecretOwner reconciles the CR whose backup Secret has been deleted, which creates the Secret anew.
// Without it the Secret stays missing until something else reconciles the CR, while the backup and restore Jobs,
// which take their credentials from it, do not start. Enqueueing a CR cancels its reconcile in progress, so a CR
// that deletes its backup Secret itself - being deleted, or reconciled with backup turned off - is left alone.
func (c *Controller) enqueueBackupSecretOwner(secret *core.Secret) {
	owner := meta.GetControllerOf(secret)
	if (owner == nil) || (owner.Kind != api.ClickHouseInstallationCRDResourceKind) {
		return
	}
	named := &api.ClickHouseInstallation{ObjectMeta: meta.ObjectMeta{Namespace: secret.GetNamespace(), Name: owner.Name}}
	if secret.GetName() != c.namer.Name(interfaces.NameBackupAutoSecret, named) {
		return
	}

	obj, err := c.kube.CR().Get(controller.NewContext(), secret.GetNamespace(), owner.Name)
	switch {
	case apiErrors.IsNotFound(err):
		return
	case err != nil:
		log.V(1).M(secret).F().Warning("Unable to get the owner of deleted backup Secret %s err: %v", util.NamespacedName(secret), err)
		return
	}
	cr, ok := obj.(*api.ClickHouseInstallation)
	if !ok || (cr == nil) || (cr.GetUID() != owner.UID) || !cr.GetDeletionTimestamp().IsZero() || !usesBackup(cr) || !ShouldEnqueue(cr) {
		return
	}
	log.V(1).M(cr).F().Info("Backup Secret %s deleted - reconcile to create it anew", util.NamespacedName(secret))
	c.enqueueObject(cmd_queue.NewReconcileCHI(cmd_queue.ReconcileAdd, nil, cr))
}

// usesBackup reports whether backup is enabled in the normalized CR - which takes in the templates, which may enable
// backup - the one being reconciled, or else the last completed one
func usesBackup(cr *api.ClickHouseInstallation) bool {
	if cr.HasTarget() {
		return cr.GetTarget().IsBackupEnabled()
	}
	return cr.GetAncestorT().IsBackupEnabled()
}
