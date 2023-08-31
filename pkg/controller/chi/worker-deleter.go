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

	coreV1 "k8s.io/api/core/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chiV1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopModel "github.com/altinity/clickhouse-operator/pkg/model"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func (w *worker) clean(ctx context.Context, chi *chiV1.ClickHouseInstallation) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	// Remove deleted items
	w.a.V(1).M(chi).F().Info("Failed to reconcile objects:\n%s", w.task.registryFailed)
	w.a.V(1).M(chi).F().Info("Reconciled objects:\n%s", w.task.registryReconciled)
	objs := w.c.discovery(ctx, chi)
	need := w.task.registryReconciled
	w.a.V(1).M(chi).F().Info("Existing objects:\n%s", objs)
	objs.Subtract(need)
	w.a.V(1).M(chi).F().Info("Non-reconciled objects:\n%s", objs)
	if w.purge(ctx, chi, objs, w.task.registryFailed) > 0 {
		w.c.enqueueObject(NewDropDns(&chi.ObjectMeta))
		util.WaitContextDoneOrTimeout(ctx, 1*time.Minute)
	}

	w.a.V(1).
		WithEvent(chi, eventActionReconcile, eventReasonReconcileInProgress).
		WithStatusAction(chi).
		M(chi).F().
		Info("remove items scheduled for deletion")

	chi.EnsureStatus().SyncHostTablesCreated()
}

// dropReplicas cleans Zookeeper for replicas that are properly deleted - via AP
func (w *worker) dropReplicas(ctx context.Context, chi *chiV1.ClickHouseInstallation, ap *chopModel.ActionPlan) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return
	}

	w.a.V(1).M(chi).F().S().Info("drop replicas based on AP")
	cnt := 0
	ap.WalkRemoved(
		func(cluster *chiV1.Cluster) {
		},
		func(shard *chiV1.ChiShard) {
		},
		func(host *chiV1.ChiHost) {
			_ = w.dropReplica(ctx, host)
			cnt++
		},
	)
	w.a.V(1).M(chi).F().E().Info("processed replicas: %d", cnt)
}

func shouldPurgeStatefulSet(chi *chiV1.ClickHouseInstallation, reconcileFailedObjs *chopModel.Registry, m metaV1.ObjectMeta) bool {
	if reconcileFailedObjs.HasStatefulSet(m) {
		return chi.GetReconciling().GetCleanup().GetReconcileFailedObjects().GetStatefulSet() == chiV1.ObjectsCleanupDelete
	}
	return chi.GetReconciling().GetCleanup().GetUnknownObjects().GetStatefulSet() == chiV1.ObjectsCleanupDelete
}

func shouldPurgePVC(chi *chiV1.ClickHouseInstallation, reconcileFailedObjs *chopModel.Registry, m metaV1.ObjectMeta) bool {
	if reconcileFailedObjs.HasPVC(m) {
		return chi.GetReconciling().GetCleanup().GetReconcileFailedObjects().GetPVC() == chiV1.ObjectsCleanupDelete
	}
	return chi.GetReconciling().GetCleanup().GetUnknownObjects().GetPVC() == chiV1.ObjectsCleanupDelete
}

func shouldPurgeConfigMap(chi *chiV1.ClickHouseInstallation, reconcileFailedObjs *chopModel.Registry, m metaV1.ObjectMeta) bool {
	if reconcileFailedObjs.HasConfigMap(m) {
		return chi.GetReconciling().GetCleanup().GetReconcileFailedObjects().GetConfigMap() == chiV1.ObjectsCleanupDelete
	}
	return chi.GetReconciling().GetCleanup().GetUnknownObjects().GetConfigMap() == chiV1.ObjectsCleanupDelete
}

func shouldPurgeService(chi *chiV1.ClickHouseInstallation, reconcileFailedObjs *chopModel.Registry, m metaV1.ObjectMeta) bool {
	if reconcileFailedObjs.HasService(m) {
		return chi.GetReconciling().GetCleanup().GetReconcileFailedObjects().GetService() == chiV1.ObjectsCleanupDelete
	}
	return chi.GetReconciling().GetCleanup().GetUnknownObjects().GetService() == chiV1.ObjectsCleanupDelete
}

func shouldPurgeSecret(chi *chiV1.ClickHouseInstallation, reconcileFailedObjs *chopModel.Registry, m metaV1.ObjectMeta) bool {
	if reconcileFailedObjs.HasSecret(m) {
		return chi.GetReconciling().GetCleanup().GetReconcileFailedObjects().GetSecret() == chiV1.ObjectsCleanupDelete
	}
	return chi.GetReconciling().GetCleanup().GetUnknownObjects().GetSecret() == chiV1.ObjectsCleanupDelete
}

func shouldPurgePDB(chi *chiV1.ClickHouseInstallation, reconcileFailedObjs *chopModel.Registry, m metaV1.ObjectMeta) bool {
	return true
}

func (w *worker) purgeStatefulSet(
	ctx context.Context,
	chi *chiV1.ClickHouseInstallation,
	reconcileFailedObjs *chopModel.Registry,
	m metaV1.ObjectMeta,
) int {
	if shouldPurgeStatefulSet(chi, reconcileFailedObjs, m) {
		w.a.V(1).M(m).F().Info("Delete StatefulSet %s/%s", m.Namespace, m.Name)
		if err := w.c.kubeClient.AppsV1().StatefulSets(m.Namespace).Delete(ctx, m.Name, newDeleteOptions()); err != nil {
			w.a.V(1).M(m).F().Error("FAILED to delete StatefulSet %s/%s, err: %v", m.Namespace, m.Name, err)
		}
		return 1
	}
	return 0
}

func (w *worker) purgePVC(
	ctx context.Context,
	chi *chiV1.ClickHouseInstallation,
	reconcileFailedObjs *chopModel.Registry,
	m metaV1.ObjectMeta,
) {
	if shouldPurgePVC(chi, reconcileFailedObjs, m) {
		if chopModel.GetReclaimPolicy(m) == chiV1.PVCReclaimPolicyDelete {
			w.a.V(1).M(m).F().Info("Delete PVC %s/%s", m.Namespace, m.Name)
			if err := w.c.kubeClient.CoreV1().PersistentVolumeClaims(m.Namespace).Delete(ctx, m.Name, newDeleteOptions()); err != nil {
				w.a.V(1).M(m).F().Error("FAILED to delete PVC %s/%s, err: %v", m.Namespace, m.Name, err)
			}
		}
	}
}

func (w *worker) purgeConfigMap(
	ctx context.Context,
	chi *chiV1.ClickHouseInstallation,
	reconcileFailedObjs *chopModel.Registry,
	m metaV1.ObjectMeta,
) {
	if shouldPurgeConfigMap(chi, reconcileFailedObjs, m) {
		w.a.V(1).M(m).F().Info("Delete ConfigMap %s/%s", m.Namespace, m.Name)
		if err := w.c.kubeClient.CoreV1().ConfigMaps(m.Namespace).Delete(ctx, m.Name, newDeleteOptions()); err != nil {
			w.a.V(1).M(m).F().Error("FAILED to delete ConfigMap %s/%s, err: %v", m.Namespace, m.Name, err)
		}
	}
}

func (w *worker) purgeService(
	ctx context.Context,
	chi *chiV1.ClickHouseInstallation,
	reconcileFailedObjs *chopModel.Registry,
	m metaV1.ObjectMeta,
) {
	if shouldPurgeService(chi, reconcileFailedObjs, m) {
		w.a.V(1).M(m).F().Info("Delete Service %s/%s", m.Namespace, m.Name)
		if err := w.c.kubeClient.CoreV1().Services(m.Namespace).Delete(ctx, m.Name, newDeleteOptions()); err != nil {
			w.a.V(1).M(m).F().Error("FAILED to delete Service %s/%s, err: %v", m.Namespace, m.Name, err)
		}
	}
}

func (w *worker) purgeSecret(
	ctx context.Context,
	chi *chiV1.ClickHouseInstallation,
	reconcileFailedObjs *chopModel.Registry,
	m metaV1.ObjectMeta,
) {
	if shouldPurgeSecret(chi, reconcileFailedObjs, m) {
		w.a.V(1).M(m).F().Info("Delete Secret %s/%s", m.Namespace, m.Name)
		if err := w.c.kubeClient.CoreV1().Secrets(m.Namespace).Delete(ctx, m.Name, newDeleteOptions()); err != nil {
			w.a.V(1).M(m).F().Error("FAILED to delete Secret %s/%s, err: %v", m.Namespace, m.Name, err)
		}
	}
}

func (w *worker) purgePDB(
	ctx context.Context,
	chi *chiV1.ClickHouseInstallation,
	reconcileFailedObjs *chopModel.Registry,
	m metaV1.ObjectMeta,
) {
	if shouldPurgePDB(chi, reconcileFailedObjs, m) {
		w.a.V(1).M(m).F().Info("Delete PDB %s/%s", m.Namespace, m.Name)
		if err := w.c.kubeClient.PolicyV1().PodDisruptionBudgets(m.Namespace).Delete(ctx, m.Name, newDeleteOptions()); err != nil {
			w.a.V(1).M(m).F().Error("FAILED to delete PDB %s/%s, err: %v", m.Namespace, m.Name, err)
		}
	}
}

// purge
func (w *worker) purge(
	ctx context.Context,
	chi *chiV1.ClickHouseInstallation,
	reg *chopModel.Registry,
	reconcileFailedObjs *chopModel.Registry,
) (cnt int) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return cnt
	}

	reg.Walk(func(entityType chopModel.EntityType, m metaV1.ObjectMeta) {
		switch entityType {
		case chopModel.StatefulSet:
			cnt += w.purgeStatefulSet(ctx, chi, reconcileFailedObjs, m)
		case chopModel.PVC:
			w.purgePVC(ctx, chi, reconcileFailedObjs, m)
		case chopModel.ConfigMap:
			w.purgeConfigMap(ctx, chi, reconcileFailedObjs, m)
		case chopModel.Service:
			w.purgeService(ctx, chi, reconcileFailedObjs, m)
		case chopModel.Secret:
			w.purgeSecret(ctx, chi, reconcileFailedObjs, m)
		case chopModel.PDB:
			w.purgePDB(ctx, chi, reconcileFailedObjs, m)
		}
	})
	return cnt
}

// discoveryAndDeleteCHI deletes all kubernetes resources related to chi *chop.ClickHouseInstallation
func (w *worker) discoveryAndDeleteCHI(ctx context.Context, chi *chiV1.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	objs := w.c.discovery(ctx, chi)
	if objs.NumStatefulSet() > 0 {
		chi.WalkHosts(func(host *chiV1.ChiHost) error {
			_ = w.ensureClusterSchemer(host).HostSyncTables(ctx, host)
			return nil
		})
	}
	w.purge(ctx, chi, objs, nil)
	return nil
}

// deleteCHIProtocol deletes all kubernetes resources related to chi *chop.ClickHouseInstallation
func (w *worker) deleteCHIProtocol(ctx context.Context, chi *chiV1.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(chi).S().P()
	defer w.a.V(2).M(chi).E().P()

	var err error
	chi, err = w.normalizer.CreateTemplatedCHI(chi, chopModel.NewNormalizerOptions())
	if err != nil {
		w.a.WithEvent(chi, eventActionDelete, eventReasonDeleteFailed).
			WithStatusError(chi).
			M(chi).F().
			Error("Delete CHI failed - unable to normalize: %q", err)
		return err
	}

	// Announce delete procedure
	w.a.V(1).
		WithEvent(chi, eventActionDelete, eventReasonDeleteStarted).
		WithStatusAction(chi).
		M(chi).F().
		Info("Delete CHI started")

	chi.EnsureStatus().DeleteStart()
	if err := w.c.updateCHIObjectStatus(ctx, chi, UpdateCHIStatusOptions{
		TolerateAbsence: true,
		CopyCHIStatusOptions: chiV1.CopyCHIStatusOptions{
			MainFields: true,
		},
	}); err != nil {
		w.a.V(1).M(chi).F().Error("UNABLE to write normalized CHI. err:%q", err)
		return nil
	}

	// Start delete protocol

	// Exclude this CHI from monitoring
	w.c.deleteWatch(chi)

	// Delete Service
	_ = w.c.deleteServiceCHI(ctx, chi)

	chi.WalkHosts(func(host *chiV1.ChiHost) error {
		_ = w.ensureClusterSchemer(host).HostSyncTables(ctx, host)
		return nil
	})

	// Delete all clusters
	chi.WalkClusters(func(cluster *chiV1.Cluster) error {
		return w.deleteCluster(ctx, chi, cluster)
	})

	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	// Delete ConfigMap(s)
	_ = w.c.deleteConfigMapsCHI(ctx, chi)

	w.a.V(1).
		WithEvent(chi, eventActionDelete, eventReasonDeleteCompleted).
		WithStatusAction(chi).
		M(chi).F().
		Info("Delete CHI completed")

	return nil
}

// canDropReplica
func (w *worker) canDropReplica(host *chiV1.ChiHost, opts ...*dropReplicaOptions) (can bool) {
	o := NewDropReplicaOptionsArr(opts...).First()

	if o.ForceDrop() {
		return true
	}

	can = true
	w.c.walkDiscoveredPVCs(host, func(pvc *coreV1.PersistentVolumeClaim) {
		// Replica's state has to be kept in Zookeeper for retained volumes.
		// ClickHouse expects to have state of the non-empty replica in-place when replica rejoins.
		if chopModel.GetReclaimPolicy(pvc.ObjectMeta) == chiV1.PVCReclaimPolicyRetain {
			w.a.V(1).F().Info("PVC %s/%s blocks drop replica. Reclaim policy: %s", chiV1.PVCReclaimPolicyRetain.String())
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

func NewDropReplicaOptionsArr(opts ...*dropReplicaOptions) (res dropReplicaOptionsArr) {
	return append(res, opts...)
}

func (a dropReplicaOptionsArr) First() *dropReplicaOptions {
	if len(a) > 0 {
		return a[0]
	}
	return nil
}

// dropReplica drops replica's info from Zookeeper
func (w *worker) dropReplica(ctx context.Context, hostToDrop *chiV1.ChiHost, opts ...*dropReplicaOptions) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	if hostToDrop == nil {
		w.a.V(1).F().Error("FAILED to drop replica. Need to have host to drop. hostToDrop:%s", hostToDrop.GetName())
		return nil
	}

	if !w.canDropReplica(hostToDrop, opts...) {
		w.a.V(1).F().Warning("CAN NOT drop replica. hostToDrop:%s", hostToDrop.GetName())
		return nil
	}

	// Sometimes host to drop is already unavailable, so let's run SQL statement of the first replica in the shard
	var hostToRunOn *chiV1.ChiHost
	if shard := hostToDrop.GetShard(); shard != nil {
		hostToRunOn = shard.FirstHost()
	}

	if hostToRunOn == nil {
		w.a.V(1).F().Error("FAILED to drop replica. hostToRunOn:%s, hostToDrop:%s", hostToRunOn.GetName(), hostToDrop.GetName())
		return nil
	}

	err := w.ensureClusterSchemer(hostToRunOn).HostDropReplica(ctx, hostToRunOn, hostToDrop)

	if err == nil {
		w.a.V(1).
			WithEvent(hostToRunOn.CHI, eventActionDelete, eventReasonDeleteCompleted).
			WithStatusAction(hostToRunOn.CHI).
			M(hostToRunOn).F().
			Info("Drop replica host %s in cluster %s", hostToDrop.GetName(), hostToDrop.Address.ClusterName)
	} else {
		w.a.WithEvent(hostToRunOn.CHI, eventActionDelete, eventReasonDeleteFailed).
			WithStatusError(hostToRunOn.CHI).
			M(hostToRunOn).F().
			Error("FAILED to drop replica on host %s with error %v", hostToDrop.GetName(), err)
	}

	return err
}

// deleteTables
func (w *worker) deleteTables(ctx context.Context, host *chiV1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	if !chopModel.HostCanDeleteAllPVCs(host) {
		return nil
	}
	err := w.ensureClusterSchemer(host).HostDropTables(ctx, host)

	if err == nil {
		w.a.V(1).
			WithEvent(host.CHI, eventActionDelete, eventReasonDeleteCompleted).
			WithStatusAction(host.CHI).
			M(host).F().
			Info("Deleted tables on host %s replica %d to shard %d in cluster %s",
				host.GetName(), host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
	} else {
		w.a.WithEvent(host.CHI, eventActionDelete, eventReasonDeleteFailed).
			WithStatusError(host.CHI).
			M(host).F().
			Error("FAILED to delete tables on host %s with error %v", host.GetName(), err)
	}

	return err
}

// deleteHost deletes all kubernetes resources related to a host
// chi is the new CHI in which there will be no more this host
func (w *worker) deleteHost(ctx context.Context, chi *chiV1.ClickHouseInstallation, host *chiV1.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(host).S().Info(host.Address.HostName)
	defer w.a.V(2).M(host).E().Info(host.Address.HostName)

	w.a.V(1).
		WithEvent(host.CHI, eventActionDelete, eventReasonDeleteStarted).
		WithStatusAction(host.CHI).
		M(host).F().
		Info("Delete host %s/%s - started", host.Address.ClusterName, host.GetName())

	var err error
	if host.CurStatefulSet, err = w.c.getStatefulSet(host); err != nil {
		w.a.WithEvent(host.CHI, eventActionDelete, eventReasonDeleteCompleted).
			WithStatusAction(host.CHI).
			M(host).F().
			Info("Delete host %s/%s - completed StatefulSet not found - already deleted? err: %v",
				host.Address.ClusterName, host.GetName(), err)
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
	_ = w.c.updateCHIObjectStatus(ctx, chi, UpdateCHIStatusOptions{
		TolerateAbsence: true,
		CopyCHIStatusOptions: chiV1.CopyCHIStatusOptions{
			MainFields: true,
		},
	})

	if err == nil {
		w.a.V(1).
			WithEvent(host.CHI, eventActionDelete, eventReasonDeleteCompleted).
			WithStatusAction(host.CHI).
			M(host).F().
			Info("Delete host %s/%s - completed", host.Address.ClusterName, host.GetName())
	} else {
		w.a.WithEvent(host.CHI, eventActionDelete, eventReasonDeleteFailed).
			WithStatusError(host.CHI).
			M(host).F().
			Error("FAILED Delete host %s/%s - completed", host.Address.ClusterName, host.GetName())
	}

	return err
}

// deleteShard deletes all kubernetes resources related to shard *chop.ChiShard
// chi is the new CHI in which there will be no more this shard
func (w *worker) deleteShard(ctx context.Context, chi *chiV1.ClickHouseInstallation, shard *chiV1.ChiShard) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(shard).S().P()
	defer w.a.V(2).M(shard).E().P()

	w.a.V(1).
		WithEvent(shard.CHI, eventActionDelete, eventReasonDeleteStarted).
		WithStatusAction(shard.CHI).
		M(shard).F().
		Info("Delete shard %s/%s - started", shard.Address.Namespace, shard.Name)

	// Delete Shard Service
	_ = w.c.deleteServiceShard(ctx, shard)

	// Delete all replicas
	shard.WalkHosts(func(host *chiV1.ChiHost) error {
		return w.deleteHost(ctx, chi, host)
	})

	w.a.V(1).
		WithEvent(shard.CHI, eventActionDelete, eventReasonDeleteCompleted).
		WithStatusAction(shard.CHI).
		M(shard).F().
		Info("Delete shard %s/%s - completed", shard.Address.Namespace, shard.Name)

	return nil
}

// deleteCluster deletes all kubernetes resources related to cluster *chop.ChiCluster
// chi is the new CHI in which there will be no more this cluster
func (w *worker) deleteCluster(ctx context.Context, chi *chiV1.ClickHouseInstallation, cluster *chiV1.Cluster) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	w.a.V(2).M(cluster).S().P()
	defer w.a.V(2).M(cluster).E().P()

	w.a.V(1).
		WithEvent(cluster.CHI, eventActionDelete, eventReasonDeleteStarted).
		WithStatusAction(cluster.CHI).
		M(cluster).F().
		Info("Delete cluster %s/%s - started", cluster.Address.Namespace, cluster.Name)

	// Delete Cluster Service
	_ = w.c.deleteServiceCluster(ctx, cluster)

	// Delete Cluster's Auto Secret
	if cluster.Secret.Source() == chiV1.ClusterSecretSourceAuto {
		// Delete Cluster Secret
		_ = w.c.deleteSecretCluster(ctx, cluster)
	}

	// Delete all shards
	cluster.WalkShards(func(index int, shard *chiV1.ChiShard) error {
		return w.deleteShard(ctx, chi, shard)
	})

	w.a.V(1).
		WithEvent(cluster.CHI, eventActionDelete, eventReasonDeleteCompleted).
		WithStatusAction(cluster.CHI).
		M(cluster).F().
		Info("Delete cluster %s/%s - completed", cluster.Address.Namespace, cluster.Name)

	return nil
}

// deleteCHI
func (w *worker) deleteCHI(ctx context.Context, old, new *chiV1.ClickHouseInstallation) bool {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return false
	}

	// Do we have pending request for CHI to be deleted?
	if new.ObjectMeta.DeletionTimestamp.IsZero() {
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

	var clear bool
	crd, err := w.c.extClient.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, "clickhouseinstallations.clickhouse.altinity.com", newGetOptions())
	if err == nil {
		if crd.ObjectMeta.DeletionTimestamp.IsZero() {
			// CRD is not being deleted and operator can delete all child resources.
			w.a.V(1).M(new).F().Info("CRD %s/%s is not being deleted, operator will delete child resources", crd.Namespace, crd.Name)
			clear = true
		} else {
			// CRD is being deleted. This may be a mistake, operator should not delete data
			w.a.V(1).M(new).F().Info("CRD %s/%s BEING DELETED, operator will NOT delete child resources", crd.Namespace, crd.Name)
			clear = false
		}
	} else {
		w.a.V(1).M(new).F().Error("unable to get CRD, got error: %v ", err)
		w.a.V(1).M(new).F().Info("will delete chi %s/%s", new.Namespace, new.Name)
		clear = true
	}

	if clear {
		cur, err := w.c.chopClient.ClickhouseV1().ClickHouseInstallations(new.Namespace).Get(ctx, new.Name, newGetOptions())
		if cur == nil {
			return false
		}
		if err != nil {
			return false
		}

		if !util.InArray(FinalizerName, new.ObjectMeta.Finalizers) {
			// No finalizer found, unexpected behavior
			return false
		}

		_ = w.deleteCHIProtocol(ctx, new)
	} else {
		new.Attributes.SkipOwnerRef = true
		_ = w.reconcileCHI(ctx, old, new)
	}

	// We need to uninstall finalizer in order to allow k8s to delete CHI resource
	w.a.V(2).M(new).F().Info("uninstall finalizer")
	if err := w.c.uninstallFinalizer(ctx, new); err != nil {
		w.a.V(1).M(new).F().Error("unable to uninstall finalizer: err:%v", err)
	}

	// CHI's child resources were deleted
	return true
}
