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
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// reconcileSecret reconciles core.Secret
func (w *worker) reconcileSecret(ctx context.Context, cr api.ICustomResource, secret *core.Secret) error {
	w.a.V(2).M(cr).S().Info("%s", secret.Name)
	defer w.a.V(2).M(cr).E().Info("%s", secret.Name)

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
// normalized for reconcile, as the normalizer reads the backup user's credentials from it. A CR normalized
// before the Secret was complete is normalized anew, and reported as changed, so that the reconcile goes on to
// give the sidecars the new REST API credentials. Whether
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
	if !shouldDeleteUnreferencedSecret(cur, cr, pods) {
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

// podsOfCR reads host Pods for the desired CHI and for its ancestor. Hosts removed
// in this reconcile exist only on the ancestor, and their Pods may still mount the
// generated Secret. A missing Pod is skipped. Any other Get error is returned so
// the caller does not treat an unknown Pod as "not mounting".
func (w *worker) podsOfCR(ctx context.Context, cr *api.ClickHouseInstallation) ([]*core.Pod, error) {
	var pods []*core.Pod
	var failed error
	seen := map[string]struct{}{}
	collect := func(host *api.Host) error {
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
		key := pod.Namespace + "/" + pod.Name
		if _, ok := seen[key]; ok {
			return nil
		}
		seen[key] = struct{}{}
		pods = append(pods, pod)
		return nil
	}
	cr.WalkHosts(collect)
	if ancestor := cr.GetAncestorT(); ancestor != nil && failed == nil {
		ancestor.WalkHosts(collect)
	}
	return pods, failed
}

// shouldDeleteUnreferencedSecret reports whether secret may be removed.
// This CHI must be its controller, and none of pods may still mount it.
// An empty UID is not a match: IsControlledBy treats two empty UIDs as equal.
func shouldDeleteUnreferencedSecret(secret *core.Secret, owner meta.Object, pods []*core.Pod) bool {
	if secret == nil || secret.Name == "" || owner == nil || owner.GetUID() == "" || !meta.IsControlledBy(secret, owner) {
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
