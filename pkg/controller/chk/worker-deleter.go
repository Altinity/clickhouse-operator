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
	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"time"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/model"
	chkLabeler "github.com/altinity/clickhouse-operator/pkg/model/chk/tags/labeler"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func (w *worker) clean(ctx context.Context, cr api.ICustomResource) {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile clean is aborted. CR: %s ", cr.GetName())
		return
	}

	w.a.V(1).
		WithEvent(cr, a.EventActionReconcile, a.EventReasonReconcileInProgress).
		WithAction(cr).
		M(cr).F().
		Info("remove items scheduled for deletion")

	// Remove deleted items
	w.a.V(1).M(cr).F().Info("List of objects which have failed to reconcile:\n%s", w.task.RegistryFailed())
	w.a.V(1).M(cr).F().Info("List of successfully reconciled objects:\n%s", w.task.RegistryReconciled())
	objs := w.c.discovery(ctx, cr)
	need := w.task.RegistryReconciled()
	w.a.V(1).M(cr).F().Info("List of existing objects:\n%s", objs)
	objs.Subtract(need)
	w.a.V(1).M(cr).F().Info("List of non-reconciled objects:\n%s", objs)

	// Raft safety barrier: a StatefulSet/PVC of a Keeper member may only be
	// deleted once every published member reports committed Raft membership
	// equal to the published set (v3 invariant 3). The gate is keyed off what
	// `objs` actually contains, not off the transient ActionPlan — see
	// raftSafeToPurge. On timeout, or while the cluster is stopped/
	// unreachable: skip the purge entirely (fail-safe stuck) — a
	// removed-but-alive pod is safe; a deleted voting member is not. The
	// reconcile is then failed by the convergence gate and requeued, and the
	// purge is retried (and only then completed) once membership actually
	// converges.
	if !w.raftSafeToPurge(ctx, cr, objs) {
		w.a.V(1).M(cr).F().Warning("skip purge: committed raft membership has not converged to the desired set")
		return
	}

	if w.purge(ctx, cr, objs, w.task.RegistryFailed()) > 0 {
		util.WaitContextDoneOrTimeout(ctx, 1*time.Minute)
	}

	//cr.EnsureStatus().SyncHostTablesCreated()
}

// purge
func (w *worker) purge(
	ctx context.Context,
	cr api.ICustomResource,
	reg *model.Registry,
	reconcileFailedObjs *model.Registry,
) (cnt int) {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Purge is aborted. CR: %s ", cr.GetName())
		return cnt
	}

	reg.Walk(func(entityType model.EntityType, m meta.Object) {
		switch entityType {
		case model.StatefulSet:
			cnt += w.purgeStatefulSet(ctx, cr, reconcileFailedObjs, m)
		case model.PVC:
			w.purgePVC(ctx, cr, reconcileFailedObjs, m)
		case model.ConfigMap:
			w.purgeConfigMap(ctx, cr, reconcileFailedObjs, m)
		case model.Service:
			w.purgeService(ctx, cr, reconcileFailedObjs, m)
		case model.Secret:
			w.purgeSecret(ctx, cr, reconcileFailedObjs, m)
		case model.PDB:
			w.purgePDB(ctx, cr, reconcileFailedObjs, m)
		}
	})
	return cnt
}

func (w *worker) purgeStatefulSet(
	ctx context.Context,
	cr api.ICustomResource,
	reconcileFailedObjs *model.Registry,
	m meta.Object,
) int {
	if shouldPurgeStatefulSet(cr, reconcileFailedObjs, m) {
		w.a.V(1).M(m).F().Info("Delete StatefulSet: %s", util.NamespaceNameString(m))
		if err := w.c.kube.STS().Delete(ctx, m.GetNamespace(), m.GetName()); err != nil {
			w.a.V(1).M(m).F().Error("FAILED to delete StatefulSet: %s, err: %v", util.NamespaceNameString(m), err)
		}
		return 1
	}
	return 0
}

func (w *worker) purgePVC(
	ctx context.Context,
	cr api.ICustomResource,
	reconcileFailedObjs *model.Registry,
	m meta.Object,
) {
	if shouldPurgePVC(cr, reconcileFailedObjs, m) {
		if chkLabeler.New(nil).GetReclaimPolicy(m) == api.PVCReclaimPolicyDelete {
			w.a.V(1).M(m).F().Info("Delete PVC: %s", util.NamespaceNameString(m))
			if err := w.c.kube.Storage().Delete(ctx, m.GetNamespace(), m.GetName()); err != nil {
				w.a.V(1).M(m).F().Error("FAILED to delete PVC: %s, err: %v", util.NamespaceNameString(m), err)
			}
		}
	}
}

func (w *worker) purgeConfigMap(
	ctx context.Context,
	cr api.ICustomResource,
	reconcileFailedObjs *model.Registry,
	m meta.Object,
) {
	if shouldPurgeConfigMap(cr, reconcileFailedObjs, m) {
		w.a.V(1).M(m).F().Info("Delete ConfigMap: %s", util.NamespaceNameString(m))
		if err := w.c.kube.ConfigMap().Delete(ctx, m.GetNamespace(), m.GetName()); err != nil {
			w.a.V(1).M(m).F().Error("FAILED to delete ConfigMap: %s, err: %v", util.NamespaceNameString(m), err)
		}
	}
}

func (w *worker) purgeService(
	ctx context.Context,
	cr api.ICustomResource,
	reconcileFailedObjs *model.Registry,
	m meta.Object,
) {
	if shouldPurgeService(cr, reconcileFailedObjs, m) {
		w.a.V(1).M(m).F().Info("Delete Service: %s", util.NamespaceNameString(m))
		if err := w.c.kube.Service().Delete(ctx, m.GetNamespace(), m.GetName()); err != nil {
			w.a.V(1).M(m).F().Error("FAILED to delete Service: %s, err: %v", util.NamespaceNameString(m), err)
		}
	}
}

func (w *worker) purgeSecret(
	ctx context.Context,
	cr api.ICustomResource,
	reconcileFailedObjs *model.Registry,
	m meta.Object,
) {
	if shouldPurgeSecret(cr, reconcileFailedObjs, m) {
		w.a.V(1).M(m).F().Info("Delete Secret: %s", util.NamespaceNameString(m))
		if err := w.c.kube.Secret().Delete(ctx, m.GetNamespace(), m.GetName()); err != nil {
			w.a.V(1).M(m).F().Error("FAILED to delete Secret: %s, err: %v", util.NamespaceNameString(m), err)
		}
	}
}

func (w *worker) purgePDB(
	ctx context.Context,
	cr api.ICustomResource,
	reconcileFailedObjs *model.Registry,
	m meta.Object,
) {
	if shouldPurgePDB(cr, reconcileFailedObjs, m) {
		w.a.V(1).M(m).F().Info("Delete PDB: %s", util.NamespaceNameString(m))
		if err := w.c.kube.PDB().Delete(ctx, m.GetNamespace(), m.GetName()); err != nil {
			w.a.V(1).M(m).F().Error("FAILED to delete PDB: %s, err: %v", util.NamespaceNameString(m), err)
		}
	}
}

func shouldPurgeStatefulSet(cr api.ICustomResource, reconcileFailedObjs *model.Registry, m meta.Object) bool {
	if reconcileFailedObjs.HasStatefulSet(m) {
		return cr.GetReconcile().GetCleanup().GetReconcileFailedObjects().GetStatefulSet() == api.ObjectsCleanupDelete
	}
	return cr.GetReconcile().GetCleanup().GetUnknownObjects().GetStatefulSet() == api.ObjectsCleanupDelete
}

func shouldPurgePVC(cr api.ICustomResource, reconcileFailedObjs *model.Registry, m meta.Object) bool {
	if reconcileFailedObjs.HasPVC(m) {
		return cr.GetReconcile().GetCleanup().GetReconcileFailedObjects().GetPVC() == api.ObjectsCleanupDelete
	}
	return cr.GetReconcile().GetCleanup().GetUnknownObjects().GetPVC() == api.ObjectsCleanupDelete
}

func shouldPurgeConfigMap(cr api.ICustomResource, reconcileFailedObjs *model.Registry, m meta.Object) bool {
	if reconcileFailedObjs.HasConfigMap(m) {
		return cr.GetReconcile().GetCleanup().GetReconcileFailedObjects().GetConfigMap() == api.ObjectsCleanupDelete
	}
	return cr.GetReconcile().GetCleanup().GetUnknownObjects().GetConfigMap() == api.ObjectsCleanupDelete
}

func shouldPurgeService(cr api.ICustomResource, reconcileFailedObjs *model.Registry, m meta.Object) bool {
	if reconcileFailedObjs.HasService(m) {
		return cr.GetReconcile().GetCleanup().GetReconcileFailedObjects().GetService() == api.ObjectsCleanupDelete
	}
	return cr.GetReconcile().GetCleanup().GetUnknownObjects().GetService() == api.ObjectsCleanupDelete
}

func shouldPurgeSecret(cr api.ICustomResource, reconcileFailedObjs *model.Registry, m meta.Object) bool {
	if reconcileFailedObjs.HasSecret(m) {
		return cr.GetReconcile().GetCleanup().GetReconcileFailedObjects().GetSecret() == api.ObjectsCleanupDelete
	}
	return cr.GetReconcile().GetCleanup().GetUnknownObjects().GetSecret() == api.ObjectsCleanupDelete
}

func shouldPurgePDB(cr api.ICustomResource, reconcileFailedObjs *model.Registry, m meta.Object) bool {
	return true
}

// deleteCRProtocol purges all child resources owned by the CR.
func (w *worker) deleteCRProtocol(ctx context.Context, chk *apiChk.ClickHouseKeeperInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Delete CR protocol is aborted")
		return nil
	}

	// Normalize to obtain proper default settings (cleanup policy etc.)
	normalized := w.createTemplated(chk)
	w.newTask(normalized, nil)

	// Delete the CR-level service explicitly (may not appear in discovery)
	_ = w.c.deleteServiceCR(ctx, normalized)

	// Discover all existing owned objects and purge them.
	// Passing an empty reconcileFailedObjs registry means every discovered object is
	// treated as "unknown" and purged according to the UnknownObjects cleanup policy.
	// purgePVC additionally gates deletion on the reclaimPolicy label (Retain/Delete).
	objs := w.c.discovery(ctx, normalized)
	w.purge(ctx, normalized, objs, model.NewRegistry())
	return nil
}

// deleteCHK handles deletion of a CHK CR that has a non-zero DeletionTimestamp.
// Returns true if the CHK is being deleted (caller should stop reconciling).
func (w *worker) deleteCHK(ctx context.Context, chk *apiChk.ClickHouseKeeperInstallation) bool {
	if util.IsContextDone(ctx) {
		return false
	}
	if chk.GetDeletionTimestamp().IsZero() {
		return false
	}

	// Check whether the CRD itself is being deleted.
	// If the CRD is deleted, k8s cascades deletion to all CHKs — in this case we must
	// NOT purge child resources (especially PVCs), because the user did not request it.
	var purge bool
	crd, err := w.c.ExtClient.ApiextensionsV1().CustomResourceDefinitions().Get(
		ctx, "clickhousekeeperinstallations.clickhouse-keeper.altinity.com", controller.NewGetOptions())
	if err == nil {
		// CRD is in place
		if crd.GetDeletionTimestamp().IsZero() {
			// CRD is not being deleted. It is standard request to delete a CR only.
			// Operator can delete all child resources.
			w.a.V(1).M(chk).F().Info("CRD is not being deleted, operator will delete child resources")
			purge = true
		} else {
			// CRD is being deleted.
			// In most cases, users do not expect to delete all CRs with all their resources as along with CRD.
			// Operator should not delete child resources - especially storage, such as PVCs and PVs
			w.a.V(1).M(chk).F().Info("CRD BEING DELETED, operator will NOT delete child resources")
			purge = false
		}
	} else {
		// CRD not found — proceed with cleanup
		w.a.V(1).M(chk).F().Error("unable to get CRD, got error: %v", err)
		w.a.V(1).M(chk).F().Info("will delete CR with all resources: %s/%s", chk.Namespace, chk.Name)
		purge = true
	}

	if purge {
		if !util.InArray(FinalizerName, chk.GetFinalizers()) {
			// No finalizer found, unexpected behavior
			return false
		}

		_ = w.deleteCRProtocol(ctx, chk)
	}

	// We need to uninstall finalizer in order to allow k8s to delete CR resource
	w.a.V(2).M(chk).F().Info("uninstall finalizer")
	if err := w.c.uninstallFinalizer(ctx, chk); err != nil {
		w.a.V(1).M(chk).F().Error("unable to uninstall finalizer. err: %v", err)
	}

	// CR delete completed
	return true
}
