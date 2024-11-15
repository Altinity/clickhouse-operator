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

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/model"
	chkLabeler "github.com/altinity/clickhouse-operator/pkg/model/chk/tags/labeler"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func (w *worker) clean(ctx context.Context, cr api.ICustomResource) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	w.a.V(1).
		WithEvent(cr, common.EventActionReconcile, common.EventReasonReconcileInProgress).
		WithStatusAction(cr).
		M(cr).F().
		Info("remove items scheduled for deletion")

	// Remove deleted items
	w.a.V(1).M(cr).F().Info("List of objects which have failed to reconcile:\n%s", w.task.RegistryFailed)
	w.a.V(1).M(cr).F().Info("List of successfully reconciled objects:\n%s", w.task.RegistryReconciled)
	objs := w.c.discovery(ctx, cr)
	need := w.task.RegistryReconciled()
	w.a.V(1).M(cr).F().Info("Existing objects:\n%s", objs)
	objs.Subtract(need)
	w.a.V(1).M(cr).F().Info("Non-reconciled objects:\n%s", objs)
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
		log.V(2).Info("task is done")
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
		return cr.GetReconciling().GetCleanup().GetReconcileFailedObjects().GetStatefulSet() == api.ObjectsCleanupDelete
	}
	return cr.GetReconciling().GetCleanup().GetUnknownObjects().GetStatefulSet() == api.ObjectsCleanupDelete
}

func shouldPurgePVC(cr api.ICustomResource, reconcileFailedObjs *model.Registry, m meta.Object) bool {
	if reconcileFailedObjs.HasPVC(m) {
		return cr.GetReconciling().GetCleanup().GetReconcileFailedObjects().GetPVC() == api.ObjectsCleanupDelete
	}
	return cr.GetReconciling().GetCleanup().GetUnknownObjects().GetPVC() == api.ObjectsCleanupDelete
}

func shouldPurgeConfigMap(cr api.ICustomResource, reconcileFailedObjs *model.Registry, m meta.Object) bool {
	if reconcileFailedObjs.HasConfigMap(m) {
		return cr.GetReconciling().GetCleanup().GetReconcileFailedObjects().GetConfigMap() == api.ObjectsCleanupDelete
	}
	return cr.GetReconciling().GetCleanup().GetUnknownObjects().GetConfigMap() == api.ObjectsCleanupDelete
}

func shouldPurgeService(cr api.ICustomResource, reconcileFailedObjs *model.Registry, m meta.Object) bool {
	if reconcileFailedObjs.HasService(m) {
		return cr.GetReconciling().GetCleanup().GetReconcileFailedObjects().GetService() == api.ObjectsCleanupDelete
	}
	return cr.GetReconciling().GetCleanup().GetUnknownObjects().GetService() == api.ObjectsCleanupDelete
}

func shouldPurgeSecret(cr api.ICustomResource, reconcileFailedObjs *model.Registry, m meta.Object) bool {
	if reconcileFailedObjs.HasSecret(m) {
		return cr.GetReconciling().GetCleanup().GetReconcileFailedObjects().GetSecret() == api.ObjectsCleanupDelete
	}
	return cr.GetReconciling().GetCleanup().GetUnknownObjects().GetSecret() == api.ObjectsCleanupDelete
}

func shouldPurgePDB(cr api.ICustomResource, reconcileFailedObjs *model.Registry, m meta.Object) bool {
	return true
}
