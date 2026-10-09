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
	"k8s.io/apimachinery/pkg/types"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
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
// and updates the CHI-owned Secret only when that document changed. A render
// failure returns before any write, so the last valid Secret stays mounted.
// Deletion of an obsolete Secret happens after the rollout, in
// deleteObsoleteHotReloadUsersSecret.
func (w *worker) reconcileHotReloadUsersSecret(ctx context.Context, cr *api.ClickHouseInstallation) error {
	if !cr.GetRuntime().GetAttributes().GetHotReloadUsers() {
		return nil
	}
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
		// The managed Secret is left as it was. A failed render must not replace
		// the last document the Pods are still loading.
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

// deleteObsoleteHotReloadUsersSecret removes chi-<chi>-users after hotReload
// has been turned off. A Pod that still projects the Secret keeps it, and a
// Secret this CHI does not own is left alone.
func (w *worker) deleteObsoleteHotReloadUsersSecret(ctx context.Context, cr *api.ClickHouseInstallation) error {
	name := w.c.namer.Name(interfaces.NameSecretCommonUsers, cr)
	cur, err := w.c.getSecret(ctx, &core.Secret{
		ObjectMeta: meta.ObjectMeta{Namespace: cr.GetNamespace(), Name: name},
	})
	if apiErrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	pods, err := w.podsOfCR(ctx, cr)
	if err != nil {
		w.a.V(1).M(cr).F().Warning("Leave Secret %s/%s in place: unable to list Pods that may still mount it: %v", cur.Namespace, cur.Name, err)
		return nil
	}
	if !shouldDeleteUnreferencedSecret(cur, cr.GetName(), cr.GetUID(), api.ClickHouseInstallationCRDResourceKind, pods) {
		w.a.V(1).M(cr).F().Info("Leave Secret %s/%s in place until no Pod mounts it and this CHI owns it", cur.Namespace, cur.Name)
		return nil
	}
	err = w.c.kube.Secret().Delete(ctx, cur.Namespace, cur.Name)
	if apiErrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		w.a.WithEvent(cr, a.EventActionDelete, a.EventReasonDeleteFailed).
			WithAction(cr).
			WithError(cr).
			M(cr).F().
			Error("Delete Secret %s/%s failed with error %v", cur.Namespace, cur.Name, err)
		return err
	}
	w.a.V(1).
		WithEvent(cr, a.EventActionDelete, a.EventReasonDeleteCompleted).
		WithAction(cr).
		M(cr).F().
		Info("Delete Secret %s/%s", cur.Namespace, cur.Name)
	return nil
}

// podsOfCR reads every host Pod. A missing Pod is skipped. Any other Get error
// is returned so the caller does not treat an unknown Pod as "not mounting".
func (w *worker) podsOfCR(ctx context.Context, cr *api.ClickHouseInstallation) ([]*core.Pod, error) {
	var pods []*core.Pod
	var failed error
	cr.WalkHosts(func(host *api.Host) error {
		if failed != nil {
			return nil
		}
		pod, err := w.c.kube.Pod().Get(ctx, host)
		if apiErrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			failed = err
			return nil
		}
		pods = append(pods, pod)
		return nil
	})
	return pods, failed
}

// shouldDeleteUnreferencedSecret reports whether secret may be removed.
// This CHI must own it, and none of pods may still mount it.
func shouldDeleteUnreferencedSecret(secret *core.Secret, ownerName string, ownerUID types.UID, ownerKind string, pods []*core.Pod) bool {
	if secret == nil || secret.Name == "" || !secretOwnedBy(secret, ownerName, ownerUID, ownerKind) {
		return false
	}
	for _, pod := range pods {
		if podReferencesSecret(pod, secret.Name) {
			return false
		}
	}
	return true
}

// podReferencesSecret reports whether pod mounts secretName, either as a Secret
// volume or as a projected Secret source.
func podReferencesSecret(pod *core.Pod, secretName string) bool {
	if pod == nil || secretName == "" {
		return false
	}
	for i := range pod.Spec.Volumes {
		vol := &pod.Spec.Volumes[i]
		if vol.Secret != nil && vol.Secret.SecretName == secretName {
			return true
		}
		if vol.Projected == nil {
			continue
		}
		for j := range vol.Projected.Sources {
			src := &vol.Projected.Sources[j]
			if src.Secret != nil && src.Secret.Name == secretName {
				return true
			}
		}
	}
	return false
}

// secretOwnedBy reports whether secret has an owner reference for this object.
// An empty UID does not match, so an unpopulated owner cannot authorize a delete.
func secretOwnedBy(secret *core.Secret, ownerName string, ownerUID types.UID, ownerKind string) bool {
	if secret == nil || ownerName == "" || ownerUID == "" || ownerKind == "" {
		return false
	}
	for i := range secret.OwnerReferences {
		ref := &secret.OwnerReferences[i]
		if ref.UID == ownerUID && ref.Name == ownerName && ref.Kind == ownerKind {
			return true
		}
	}
	return false
}
