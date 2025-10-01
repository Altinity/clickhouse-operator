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
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi/metrics"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/storage"
	"github.com/altinity/clickhouse-operator/pkg/model"
	chiLabeler "github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	"github.com/altinity/clickhouse-operator/pkg/model/common/action_plan"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
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

	cr.(*api.ClickHouseInstallation).EnsureStatus().SyncHostTablesCreated()
}

// dropReplicas cleans Zookeeper for replicas that are properly deleted - via AP
func (w *worker) dropReplicas(ctx context.Context, cr api.ICustomResource, ap *action_plan.ActionPlan) {
	w.a.V(1).M(cr).F().S().Info("drop replicas based on AP")
	cnt := 0
	ap.WalkRemoved(
		func(cluster api.ICluster) {
		},
		func(shard api.IShard) {
		},
		func(host *api.Host) {
			_ = w.dropReplica(ctx, host)
			cnt++
		},
	)
	w.a.V(1).M(cr).F().E().Info("processed replicas: %d", cnt)
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
		if chiLabeler.New(nil).GetReclaimPolicy(m) == api.PVCReclaimPolicyDelete {
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

// discoveryAndDeleteCR deletes all kubernetes resources related to chi *chop.ClickHouseInstallation
func (w *worker) discoveryAndDeleteCR(ctx context.Context, cr api.ICustomResource) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Discovery and delete is aborted. CR: %s ", cr.GetName())
		return nil
	}

	metrics.CHIUnregister(ctx, cr)

	objs := w.c.discovery(ctx, cr)
	if objs.NumStatefulSet() > 0 {
		cr.WalkHosts(func(host *api.Host) error {
			_ = w.ensureClusterSchemer(host).HostSyncTables(ctx, host)
			return nil
		})
	}
	w.purge(ctx, cr, objs, nil)
	return nil
}

// deleteCHIProtocol deletes all kubernetes resources related to chi *chop.ClickHouseInstallation
func (w *worker) deleteCHIProtocol(ctx context.Context, chi *api.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Delete CHI protocol is aborted")
		return nil
	}

	w.a.V(2).M(chi).S().P()
	defer w.a.V(2).M(chi).E().P()

	var err error
	chi, err = w.normalizer.CreateTemplated(chi, normalizer.NewOptions[api.ClickHouseInstallation]())
	if err != nil {
		w.a.WithEvent(chi, a.EventActionDelete, a.EventReasonDeleteFailed).
			WithError(chi).
			M(chi).F().
			Error("Delete CHI failed - unable to normalize: %q", err)
		return err
	}

	// Announce delete procedure
	w.a.V(1).
		WithEvent(chi, a.EventActionDelete, a.EventReasonDeleteStarted).
		WithAction(chi).
		M(chi).F().
		Info("Delete CHI started")

	chi.EnsureStatus().DeleteStart()
	if err := w.c.updateCRObjectStatus(ctx, chi, types.UpdateStatusOptions{
		TolerateAbsence: true,
		CopyStatusOptions: types.CopyStatusOptions{
			CopyStatusFieldGroup: types.CopyStatusFieldGroup{
				FieldGroupMain: true,
			},
		},
	}); err != nil {
		w.a.V(1).M(chi).F().Error("UNABLE to write normalized CHI. err: %q", err)
		return nil
	}

	// Start delete protocol

	// Exclude this CHI from monitoring
	w.c.deleteWatch(chi)

	// Delete Service
	_ = w.c.deleteServiceCR(ctx, chi)

	chi.WalkHosts(func(host *api.Host) error {
		_ = w.ensureClusterSchemer(host).HostSyncTables(ctx, host)
		return nil
	})

	// Delete all clusters
	chi.WalkClusters(func(cluster api.ICluster) error {
		return w.deleteCluster(ctx, chi, cluster.(*api.Cluster))
	})

	if util.IsContextDone(ctx) {
		log.V(1).Info("Delete is aborted")
		return nil
	}

	// Delete ConfigMap(s)
	_ = w.c.deleteConfigMapsCHI(ctx, chi)

	w.a.V(1).
		WithEvent(chi, a.EventActionDelete, a.EventReasonDeleteCompleted).
		WithAction(chi).
		M(chi).F().
		Info("Delete CHI completed")

	return nil
}

// canDropReplica
func (w *worker) canDropReplica(ctx context.Context, host *api.Host, opts ...*dropReplicaOptions) (can bool) {
	o := NewDropReplicaOptionsArr(opts...).First()

	if o.ForceDrop() {
		return true
	}

	can = true
	storage.NewStoragePVC(w.c.kube.Storage()).WalkDiscoveredPVCs(ctx, host, func(pvc *core.PersistentVolumeClaim) {
		// Replica's state has to be kept in Zookeeper for retained volumes.
		// ClickHouse expects to have state of the non-empty replica in-place when replica rejoins.
		if chiLabeler.New(nil).GetReclaimPolicy(pvc.GetObjectMeta()) == api.PVCReclaimPolicyRetain {
			w.a.V(1).F().Info("PVC: %s/%s blocks drop replica. Reclaim policy: %s", api.PVCReclaimPolicyRetain.String())
			can = false
		}
	})
	return can
}

type dropReplicaOptions struct {
	forceDrop bool
}

func (o *dropReplicaOptions) ForceDrop() bool {
	if o == nil {
		return false
	}

	return o.forceDrop
}

type dropReplicaOptionsArr []*dropReplicaOptions

// NewDropReplicaOptionsArr creates new dropReplicaOptions array
func NewDropReplicaOptionsArr(opts ...*dropReplicaOptions) (res dropReplicaOptionsArr) {
	return append(res, opts...)
}

// First gets first option
func (a dropReplicaOptionsArr) First() *dropReplicaOptions {
	if len(a) > 0 {
		return a[0]
	}
	return nil
}

// dropReplica drops replica's info from Zookeeper
func (w *worker) dropReplica(ctx context.Context, hostToDrop *api.Host, opts ...*dropReplicaOptions) error {
	if hostToDrop == nil {
		w.a.V(1).F().Error("FAILED to drop replica. Need to have host to drop. hostToDrop: %s", hostToDrop.GetName())
		return nil
	}

	if !w.canDropReplica(ctx, hostToDrop, opts...) {
		w.a.V(1).F().Warning("CAN NOT drop replica. hostToDrop: %s", hostToDrop.GetName())
		return nil
	}

	// Sometimes host to drop is already unavailable, so let's run SQL statement of the first replica in the shard
	var hostToRunOn *api.Host
	if shard := hostToDrop.GetShard(); shard != nil {
		hostToRunOn = shard.FirstHost()
	}

	if hostToRunOn == nil {
		w.a.V(1).F().Error("FAILED to drop replica. hostToRunOn: %s, hostToDrop: %s", hostToRunOn.GetName(), hostToDrop.GetName())
		return nil
	}

	err := w.ensureClusterSchemer(hostToRunOn).HostDropReplica(ctx, hostToRunOn, hostToDrop)

	if err == nil {
		w.a.V(1).
			WithEvent(hostToRunOn.GetCR(), a.EventActionDelete, a.EventReasonDeleteCompleted).
			WithAction(hostToRunOn.GetCR()).
			M(hostToRunOn).F().
			Info("Drop replica host: %s in cluster: %s", hostToDrop.GetName(), hostToDrop.Runtime.Address.ClusterName)
	} else {
		w.a.WithEvent(hostToRunOn.GetCR(), a.EventActionDelete, a.EventReasonDeleteFailed).
			WithError(hostToRunOn.GetCR()).
			M(hostToRunOn).F().
			Error("FAILED to drop replica on host: %s with error: %v", hostToDrop.GetName(), err)
	}

	return err
}

// deleteTables
func (w *worker) deleteTables(ctx context.Context, host *api.Host) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Delete tables. Host: %s ", host.GetName())
		return nil
	}

	if !w.c.pvcDeleter.HostCanDeleteAllPVCs(host) {
		return nil
	}
	err := w.ensureClusterSchemer(host).HostDropTables(ctx, host)

	if err == nil {
		w.a.V(1).
			WithEvent(host.GetCR(), a.EventActionDelete, a.EventReasonDeleteCompleted).
			WithAction(host.GetCR()).
			M(host).F().
			Info("Deleted tables on host: %s replica: %d to shard: %d in cluster: %s",
				host.GetName(), host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
	} else {
		w.a.WithEvent(host.GetCR(), a.EventActionDelete, a.EventReasonDeleteFailed).
			WithError(host.GetCR()).
			M(host).F().
			Error("FAILED to delete tables on host: %s with error: %v", host.GetName(), err)
	}

	return err
}

// deleteHost deletes all kubernetes resources related to a host
// chi is the new CHI in which there will be no more this host
func (w *worker) deleteHost(ctx context.Context, chi *api.ClickHouseInstallation, host *api.Host) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Delete host is aborted. Host: %s ", host.GetName())
		return nil
	}

	w.a.V(2).M(host).S().Info(host.Runtime.Address.HostName)
	defer w.a.V(2).M(host).E().Info(host.Runtime.Address.HostName)

	w.a.V(1).
		WithEvent(host.GetCR(), a.EventActionDelete, a.EventReasonDeleteStarted).
		WithAction(host.GetCR()).
		M(host).F().
		Info("Delete host: %s/%s - started", host.Runtime.Address.ClusterName, host.GetName())

	var err error
	if host.Runtime.CurStatefulSet, err = w.c.kube.STS().Get(ctx, host); err != nil {
		w.a.WithEvent(host.GetCR(), a.EventActionDelete, a.EventReasonDeleteCompleted).
			WithAction(host.GetCR()).
			M(host).F().
			Info("Delete host: %s/%s - completed StatefulSet not found - already deleted? err: %v",
				host.Runtime.Address.ClusterName, host.GetName(), err)
		return nil
	}

	// Each host consists of
	// 1. User-level objects - tables on the host
	//    We need to delete tables on the host in order to clean Zookeeper data.
	//    If just delete tables, Zookeeper will still keep track of non-existent tables
	// 2. Kubernetes-level objects - such as StatefulSet, PVC(s), ConfigMap(s), Service(s)
	// Need to delete all these items

	_ = w.deleteTables(ctx, host)
	err = w.c.deleteHost(ctx, host)

	// When deleting the whole CHI (not particular host), CHI may already be unavailable, so update CHI tolerantly
	chi.EnsureStatus().HostDeleted()
	_ = w.c.updateCRObjectStatus(ctx, chi, types.UpdateStatusOptions{
		TolerateAbsence: true,
		CopyStatusOptions: types.CopyStatusOptions{
			CopyStatusFieldGroup: types.CopyStatusFieldGroup{
				FieldGroupMain: true,
			},
		},
	})

	if err == nil {
		w.a.V(1).
			WithEvent(host.GetCR(), a.EventActionDelete, a.EventReasonDeleteCompleted).
			WithAction(host.GetCR()).
			M(host).F().
			Info("Delete host: %s/%s - completed", host.Runtime.Address.ClusterName, host.GetName())
	} else {
		w.a.WithEvent(host.GetCR(), a.EventActionDelete, a.EventReasonDeleteFailed).
			WithError(host.GetCR()).
			M(host).F().
			Error("FAILED Delete host: %s/%s - completed", host.Runtime.Address.ClusterName, host.GetName())
	}

	return err
}

// deleteShard deletes all kubernetes resources related to shard *chop.ChiShard
// chi is the new CHI in which there will be no more this shard
func (w *worker) deleteShard(ctx context.Context, chi *api.ClickHouseInstallation, shard *api.ChiShard) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Delete shard is aborted. shard: %s ", shard.GetName())
		return nil
	}

	w.a.V(2).M(shard).S().P()
	defer w.a.V(2).M(shard).E().P()

	w.a.V(1).
		WithEvent(shard.Runtime.CHI, a.EventActionDelete, a.EventReasonDeleteStarted).
		WithAction(shard.Runtime.CHI).
		M(shard).F().
		Info("Delete shard: %s/%s - started", shard.Runtime.Address.Namespace, shard.Name)

	// Delete Shard Service
	_ = w.c.deleteServiceShard(ctx, shard)

	// Delete all replicas
	shard.WalkHosts(func(host *api.Host) error {
		return w.deleteHost(ctx, chi, host)
	})

	w.a.V(1).
		WithEvent(shard.Runtime.CHI, a.EventActionDelete, a.EventReasonDeleteCompleted).
		WithAction(shard.Runtime.CHI).
		M(shard).F().
		Info("Delete shard: %s/%s - completed", shard.Runtime.Address.Namespace, shard.Name)

	return nil
}

// deleteCluster deletes all kubernetes resources related to cluster *chop.ChiCluster
// chi is the new CHI in which there will be no more this cluster
func (w *worker) deleteCluster(ctx context.Context, chi *api.ClickHouseInstallation, cluster *api.Cluster) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(cluster).S().P()
	defer w.a.V(2).M(cluster).E().P()

	w.a.V(1).
		WithEvent(cluster.Runtime.CHI, a.EventActionDelete, a.EventReasonDeleteStarted).
		WithAction(cluster.Runtime.CHI).
		M(cluster).F().
		Info("Delete cluster: %s/%s - started", cluster.Runtime.Address.Namespace, cluster.Name)

	// Delete ChkCluster Service
	_ = w.c.deleteServiceCluster(ctx, cluster)

	// Delete ChkCluster's Auto Secret
	if cluster.Secret.Source() == api.ClusterSecretSourceAuto {
		// Delete ChkCluster Secret
		_ = w.c.deleteSecretCluster(ctx, cluster)
	}

	// Delete all shards
	cluster.WalkShards(func(index int, shard api.IShard) error {
		return w.deleteShard(ctx, chi, shard.(*api.ChiShard))
	})

	w.a.V(1).
		WithEvent(cluster.Runtime.CHI, a.EventActionDelete, a.EventReasonDeleteCompleted).
		WithAction(cluster.Runtime.CHI).
		M(cluster).F().
		Info("Delete cluster: %s/%s - completed", cluster.Runtime.Address.Namespace, cluster.Name)

	return nil
}

// deleteCHI
func (w *worker) deleteCHI(ctx context.Context, old, new *api.ClickHouseInstallation) bool {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Delete CHI is aborted")
		return false
	}

	// Do we have pending request for CHI to be deleted?
	if new.GetDeletionTimestamp().IsZero() {
		// CHI is not being deleted and operator has not deleted anything.
		return false
	}

	w.a.V(3).M(new).S().P()
	defer w.a.V(3).M(new).E().P()

	// Ok, we have pending request for CHI to be deleted.
	// However, we need to decide, should CHI's child resources be deleted or not.
	// There is a curious situation, when CRD is deleted and k8s starts to delete all resources of the type,
	// described by CRD being deleted. This is may be unexpected and very painful situation,
	// so in this case we should agree to delete CHI itself, but has to keep all CHI's child resources.

	var purge bool
	crd, err := w.c.extClient.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, "clickhouseinstallations.clickhouse.altinity.com", controller.NewGetOptions())
	if err == nil {
		// CRD is in place
		if crd.GetObjectMeta().GetDeletionTimestamp().IsZero() {
			// CRD is not being deleted. It is standard request to delete a CHI.
			// Operator can delete all child resources.
			w.a.V(1).M(new).F().Info("CRD: %s/%s is not being deleted, operator will delete child resources", crd.Namespace, crd.Name)
			purge = true
		} else {
			// CRD is being deleted.
			// In most cases, users do not expect to delete all CHIs with all their resources as along with CRD.
			// Operator should not delete child resources - especially storage, such as PVCs and PVs
			w.a.V(1).M(new).F().Info("CRD: %s/%s BEING DELETED, operator will NOT delete child resources", crd.Namespace, crd.Name)
			purge = false
		}
	} else {
		// No CRD is available
		w.a.V(1).M(new).F().Error("unable to get CRD, got error: %v ", err)
		w.a.V(1).M(new).F().Info("will delete chi with all resources: %s/%s", new.Namespace, new.Name)
		purge = true
	}

	if purge {
		cur, err := w.c.chopClient.ClickhouseV1().ClickHouseInstallations(new.Namespace).Get(ctx, new.Name, controller.NewGetOptions())
		if cur == nil {
			return false
		}
		if err != nil {
			return false
		}

		if !util.InArray(FinalizerName, new.GetFinalizers()) {
			// No finalizer found, unexpected behavior
			return false
		}

		_ = w.deleteCHIProtocol(ctx, new)
	} else {
		new.GetRuntime().GetAttributes().SetSkipOwnerRef(true)
		_ = w.reconcileCR(ctx, old, new)
	}

	// We need to uninstall finalizer in order to allow k8s to delete CHI resource
	w.a.V(2).M(new).F().Info("uninstall finalizer")
	if err := w.c.uninstallFinalizer(ctx, new); err != nil {
		w.a.V(1).M(new).F().Error("unable to uninstall finalizer. err: %v", err)
	}

	// CHI delete completed
	return true
}
