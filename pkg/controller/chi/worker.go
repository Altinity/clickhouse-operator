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
	"fmt"
	"github.com/juliangruber/go-intersect"
	"gopkg.in/d4l3k/messagediff.v1"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/util/workqueue"

	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopmodel "github.com/altinity/clickhouse-operator/pkg/model"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

const FinalizerName = "finalizer.clickhouseinstallation.altinity.com"

type worker struct {
	c          *Controller
	a          Announcer
	queue      workqueue.RateLimitingInterface
	normalizer *chopmodel.Normalizer
	schemer    *chopmodel.Schemer
	creator    *chopmodel.Creator
}

// newWorker
func (c *Controller) newWorker(queue workqueue.RateLimitingInterface) *worker {
	return &worker{
		c:          c,
		a:          NewAnnouncer(c),
		queue:      queue,
		normalizer: chopmodel.NewNormalizer(c.chop),
		schemer: chopmodel.NewSchemer(
			c.chop.Config().CHUsername,
			c.chop.Config().CHPassword,
			c.chop.Config().CHPort,
		),
		creator: nil,
	}
}

// run is an endless work loop, expected to be run in a thread
func (w *worker) run() {
	w.a.V(2).Info("run() - start")
	defer w.a.V(2).Info("run() - end")

	for {
		// Get() blocks until it can return an item
		item, shutdown := w.queue.Get()
		if shutdown {
			w.a.Info("shutdown request")
			return
		}

		if err := w.processItem(item); err != nil {
			// Item not processed
			// this code cannot return an error and needs to indicate error has been ignored
			utilruntime.HandleError(err)
		}

		// Forget indicates that an item is finished being retried.  Doesn't matter whether its for perm failing
		// or for success, we'll stop the rate limiter from tracking it.  This only clears the `rateLimiter`, you
		// still have to call `Done` on the queue.
		w.queue.Forget(item)

		// Remove item from processing set when processing completed
		w.queue.Done(item)
	}
}

// processWorkItem processes one work item according to its type
func (w *worker) processItem(item interface{}) error {
	w.a.V(3).Info("processItem() - start")
	defer w.a.V(3).Info("processItem() - end")

	switch item.(type) {

	case *ReconcileChi:
		reconcile, _ := item.(*ReconcileChi)
		switch reconcile.cmd {
		case reconcileAdd:
			return w.updateCHI(nil, reconcile.new)
		case reconcileUpdate:
			return w.updateCHI(reconcile.old, reconcile.new)
		case reconcileDelete:
			return w.deleteCHI(reconcile.old)
		}

		// Unknown item type, don't know what to do with it
		// Just skip it and behave like it never existed
		utilruntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", reconcile))
		return nil

	case *ReconcileChit:
		reconcile, _ := item.(*ReconcileChit)
		switch reconcile.cmd {
		case reconcileAdd:
			return w.c.addChit(reconcile.new)
		case reconcileUpdate:
			return w.c.updateChit(reconcile.old, reconcile.new)
		case reconcileDelete:
			return w.c.deleteChit(reconcile.old)
		}

		// Unknown item type, don't know what to do with it
		// Just skip it and behave like it never existed
		utilruntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", reconcile))
		return nil

	case *ReconcileChopConfig:
		reconcile, _ := item.(*ReconcileChopConfig)
		switch reconcile.cmd {
		case reconcileAdd:
			return w.c.addChopConfig(reconcile.new)
		case reconcileUpdate:
			return w.c.updateChopConfig(reconcile.old, reconcile.new)
		case reconcileDelete:
			return w.c.deleteChopConfig(reconcile.old)
		}

		// Unknown item type, don't know what to do with it
		// Just skip it and behave like it never existed
		utilruntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", reconcile))
		return nil

	case *DropDns:
		drop, _ := item.(*DropDns)
		if chi, err := w.createCHIFromObjectMeta(drop.initiator); err == nil {
			w.a.V(2).Info("endpointsInformer UpdateFunc(%s/%s) flushing DNS for CHI %s", drop.initiator.Namespace, drop.initiator.Name, chi.Name)
			_ = w.schemer.CHIDropDnsCache(chi)
		} else {
			w.a.Error("endpointsInformer UpdateFunc(%s/%s) unable to find CHI by %v", drop.initiator.Namespace, drop.initiator.Name, drop.initiator.Labels)
		}
		return nil
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilruntime.HandleError(fmt.Errorf("unexpected item in the queue - %#v", item))
	return nil
}

// normalize
func (w *worker) normalize(chi *chop.ClickHouseInstallation) *chop.ClickHouseInstallation {
	w.a.V(3).Info("normalize() - start")
	defer w.a.V(3).Info("normalize() - end")

	var withDefaultCluster bool

	if chi == nil {
		chi = &chop.ClickHouseInstallation{}
		withDefaultCluster = false
	} else {
		withDefaultCluster = true
	}

	chi, err := w.normalizer.CreateTemplatedCHI(chi, withDefaultCluster)
	if err != nil {
		w.a.WithEvent(chi, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusError(chi).
			Error("FAILED to normalize CHI : %v", err)
	}

	return chi
}

// updateCHI sync CHI which was already created earlier
func (w *worker) updateCHI(old, new *chop.ClickHouseInstallation) error {
	w.a.V(3).Info("updateCHI() - start")
	defer w.a.V(3).Info("updateCHI() - end")

	update := (old != nil) && (new != nil)

	if update && (old.ObjectMeta.ResourceVersion == new.ObjectMeta.ResourceVersion) {
		w.a.V(3).Info("updateCHI(%s/%s): ResourceVersion did not change: %s", new.Namespace, new.Name, new.ObjectMeta.ResourceVersion)
		// No need to react
		return nil
	}

	// Check DeletionTimestamp in order to understand, whether the object is being deleted
	if new.ObjectMeta.DeletionTimestamp.IsZero() {
		// The object is not being deleted
		if !util.InArray(FinalizerName, new.ObjectMeta.Finalizers) {
			// Install finalizer
			w.a.V(2).Info("updateCHI(%s/%s): install finalizer", new.Namespace, new.Name)

			if err := w.c.installFinalizer(new); err != nil {
				w.a.V(1).Info("updateCHI(%s/%s): unable to install finalizer: %v", new.Namespace, new.Name, err)
			}
		}

		w.a.V(3).Info("updateCHI(%s/%s): finalizer installed", new.Namespace, new.Name)
	} else {
		// The object is being deleted
		cur, err := w.c.chopClient.ClickhouseV1().ClickHouseInstallations(new.Namespace).Get(new.Name, newGetOptions())
		if (err == nil) && (cur != nil) {
			if util.InArray(FinalizerName, new.ObjectMeta.Finalizers) {
				// Delete CHI
				(&new.Status).DeleteStart()
				if err := w.c.updateCHIObjectStatus(new, true); err != nil {
					w.a.V(1).Info("UNABLE to write normalized CHI (%s/%s). It can trigger update action again. Error: %q", new.Namespace, new.Name, err)
					return nil
				}

				_ = w.deleteCHI(new)

				// Uninstall finalizer
				w.a.V(2).Info("updateCHI(%s/%s): uninstall finalizer", new.Namespace, new.Name)
				if err := w.c.uninstallFinalizer(new); err != nil {
					w.a.V(1).Info("updateCHI(%s/%s): unable to uninstall finalizer: %v", new.Namespace, new.Name, err)
				}
			}

			// Object can now be deleted by Kubernetes
			w.a.V(3).Info("updateCHI(%s/%s): finalizer uninstalled, object can be deleted", new.Namespace, new.Name)
		}

		return nil
	}

	old = w.normalize(old)
	new = w.normalize(new)

	actionPlan := NewActionPlan(old, new)

	if !actionPlan.HasActionsToDo() {
		// Nothing to do - no changes found - no need to react
		w.a.V(3).Info("updateCHI(%s/%s) - ResourceVersion changed, but no actual changes found", new.Namespace, new.Name)
		return nil
	}

	// Write desired normalized CHI with initialized .Status, so it would be possible to monitor progress
	(&new.Status).ReconcileStart(actionPlan.GetRemovedHostsNum())
	if err := w.c.updateCHIObjectStatus(new, false); err != nil {
		w.a.V(1).Info("UNABLE to write normalized CHI (%s/%s). It can trigger update action again. Error: %q", new.Namespace, new.Name, err)
		return nil
	}

	w.a.V(1).
		WithEvent(new, eventActionReconcile, eventReasonReconcileStarted).
		WithStatusAction(new).
		Info("updateCHI(%s/%s) reconcile started", new.Namespace, new.Name)
	w.a.V(2).Info("updateCHI(%s/%s) - action plan\n%s\n", new.Namespace, new.Name, actionPlan.String())

	if err := w.reconcile(new); err != nil {
		w.a.WithEvent(new, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusError(new).
			Error("FAILED update: %v", err)
		return nil
	}

	// Post-process added items
	w.a.V(1).
		WithEvent(new, eventActionReconcile, eventReasonReconcileInProgress).
		WithStatusAction(new).
		Info("updateCHI(%s/%s) remove scheduled for deletion items", new.Namespace, new.Name)
	actionPlan.WalkAdded(
		func(cluster *chop.ChiCluster) {
		},
		func(shard *chop.ChiShard) {
		},
		func(host *chop.ChiHost) {
			if update {
				w.a.V(1).
					WithEvent(new, eventActionCreate, eventReasonCreateStarted).
					WithStatusAction(new).
					Info("Adding tables on shard/host:%d/%d cluster:%s", host.Address.ShardIndex, host.Address.ReplicaIndex, host.Address.ClusterName)
				if err := w.schemer.HostCreateTables(host); err != nil {
					w.a.Error("ERROR create tables on host %s. err: %v", host.Name, err)
				}
			} else {
				w.a.V(1).
					Info("As CHI is just created, not need to add tables on host %d to shard %d in cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
			}
		},
	)

	// Remove deleted items
	w.a.V(1).
		WithEvent(new, eventActionReconcile, eventReasonReconcileInProgress).
		WithStatusAction(new).
		Info("updateCHI(%s/%s) remove scheduled for deletion items", new.Namespace, new.Name)
	actionPlan.WalkRemoved(
		func(cluster *chop.ChiCluster) {
			_ = w.deleteCluster(cluster)
		},
		func(shard *chop.ChiShard) {
			_ = w.deleteShard(shard)
		},
		func(host *chop.ChiHost) {
			_ = w.deleteHost(host)
		},
	)

	w.a.V(1).
		WithEvent(new, eventActionReconcile, eventReasonReconcileInProgress).
		WithStatusAction(new).
		Info("updateCHI(%s/%s) update monitoring list", new.Namespace, new.Name)
	w.c.updateWatch(new.Namespace, new.Name, chopmodel.CreatePodFQDNsOfCHI(new))

	// Update CHI object
	(&new.Status).ReconcileComplete()
	_ = w.c.updateCHIObjectStatus(new, false)

	w.a.V(1).
		WithEvent(new, eventActionReconcile, eventReasonReconcileCompleted).
		WithStatusActions(new).
		Info("updateCHI(%s/%s) reconcile completed", new.Namespace, new.Name)

	return nil
}

// reconcile reconciles ClickHouseInstallation
func (w *worker) reconcile(chi *chop.ClickHouseInstallation) error {
	w.a.V(2).Info("reconcile() - start")
	defer w.a.V(2).Info("reconcile() - end")

	w.creator = chopmodel.NewCreator(w.c.chop, chi)
	return chi.WalkTillError(
		w.reconcileCHI,
		w.reconcileCluster,
		w.reconcileShard,
		w.reconcileHost,
	)
}

// reconcileCHI reconciles CHI global objects
func (w *worker) reconcileCHI(chi *chop.ClickHouseInstallation) error {
	w.a.V(2).Info("reconcileCHI() - start")
	defer w.a.V(2).Info("reconcileCHI() - end")

	// 1. CHI Service
	service := w.creator.CreateServiceCHI()
	if err := w.reconcileService(chi, service); err != nil {
		w.a.WithEvent(chi, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			Error("Reconcile CHI %s failed to reconcile Service %s", chi.Name, service.Name)
		return err
	}

	// 2. CHI ConfigMaps

	// ConfigMap common for all resources in CHI
	// contains several sections, mapped as separated chopConfig files,
	// such as remote servers, zookeeper setup, etc
	configMapCommon := w.creator.CreateConfigMapCHICommon()
	if err := w.reconcileConfigMap(chi, configMapCommon); err != nil {
		w.a.WithEvent(chi, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			Error("Reconcile CHI %s failed to reconcile ConfigMap %s", chi.Name, configMapCommon.Name)
		return err
	}

	// ConfigMap common for all users resources in CHI
	configMapUsers := w.creator.CreateConfigMapCHICommonUsers()
	if err := w.reconcileConfigMap(chi, configMapUsers); err != nil {
		w.a.WithEvent(chi, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			Error("Reconcile CHI %s failed to reconcile ConfigMap %s", chi.Name, configMapUsers.Name)
		return err
	}

	// Add here other CHI components to be reconciled

	return nil
}

// reconcileCluster reconciles Cluster, excluding nested shards
func (w *worker) reconcileCluster(cluster *chop.ChiCluster) error {
	w.a.V(2).Info("reconcileCluster() - start")
	defer w.a.V(2).Info("reconcileCluster() - end")

	// Add Cluster's Service
	service := w.creator.CreateServiceCluster(cluster)
	if service == nil {
		// TODO
		// For somewhat reason Service is not created, this is an error, but not clear what to do about it
		return nil
	}
	return w.reconcileService(cluster.CHI, service)
}

// reconcileShard reconciles Shard, excluding nested replicas
func (w *worker) reconcileShard(shard *chop.ChiShard) error {
	w.a.V(2).Info("reconcileShard() - start")
	defer w.a.V(2).Info("reconcileShard() - end")

	// Add Shard's Service
	service := w.creator.CreateServiceShard(shard)
	if service == nil {
		// TODO
		// For somewhat reason Service is not created, this is an error, but not clear what to do about it
		return nil
	}
	return w.reconcileService(shard.CHI, service)
}

// reconcileHost reconciles ClickHouse host
func (w *worker) reconcileHost(host *chop.ChiHost) error {
	w.a.V(2).Info("reconcileHost() - start")
	defer w.a.V(2).Info("reconcileHost() - end")

	w.a.V(1).
		WithEvent(host.CHI, eventActionReconcile, eventReasonReconcileStarted).
		WithStatusAction(host.CHI).
		Info("Reconcile Host %s started", host.Name)

	// Reconcile host's ConfigMap
	configMap := w.creator.CreateConfigMapHost(host)
	if err := w.reconcileConfigMap(host.CHI, configMap); err != nil {
		w.a.WithEvent(host.CHI, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusAction(host.CHI).
			WithStatusError(host.CHI).
			Error("Reconcile Host %s failed to reconcile ConfigMap %s", host.Name, configMap.Name)
		return err
	}

	// Reconcile host's StatefulSet
	statefulSet := w.creator.CreateStatefulSet(host)
	if err := w.reconcileStatefulSet(statefulSet, host); err != nil {
		w.a.WithEvent(host.CHI, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusAction(host.CHI).
			WithStatusError(host.CHI).
			Error("Reconcile Host %s failed to reconcile StatefulSet %s", host.Name, statefulSet.Name)
		return err
	}

	// Reconcile host's Persistent Volumes
	w.reconcilePersistentVolumes(host)

	// Reconcile host's Service
	service := w.creator.CreateServiceHost(host)
	if err := w.reconcileService(host.CHI, service); err != nil {
		w.a.WithEvent(host.CHI, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusAction(host.CHI).
			WithStatusError(host.CHI).
			Error("Reconcile Host %s failed to reconcile Service %s", host.Name, service.Name)
		return err
	}

	w.a.V(1).
		WithEvent(host.CHI, eventActionReconcile, eventReasonReconcileCompleted).
		WithStatusAction(host.CHI).
		Info("Reconcile Host %s completed", host.Name)

	return nil
}

// deleteCHI deletes all kubernetes resources related to chi *chop.ClickHouseInstallation
func (w *worker) deleteCHI(chi *chop.ClickHouseInstallation) error {
	w.a.V(2).Info("deleteCHI() - start")
	defer w.a.V(2).Info("deleteCHI() - end")

	var err error

	w.a.V(1).
		WithEvent(chi, eventActionDelete, eventReasonDeleteStarted).
		WithStatusAction(chi).
		Info("Delete CHI %s/%s started", chi.Namespace, chi.Name)

	chi, err = w.normalizer.CreateTemplatedCHI(chi, true)
	if err != nil {
		w.a.WithEvent(chi, eventActionDelete, eventReasonDeleteFailed).
			WithStatusError(chi).
			Error("Delete CHI %s/%s failed - unable to normalize: %q", chi.Namespace, chi.Name, err)
		return err
	}

	// Delete all clusters
	chi.WalkClusters(func(cluster *chop.ChiCluster) error {
		return w.deleteCluster(cluster)
	})

	// Delete ConfigMap(s)
	err = w.c.deleteConfigMapsCHI(chi)

	// Delete Service
	err = w.c.deleteServiceCHI(chi)

	w.a.V(1).
		WithEvent(chi, eventActionDelete, eventReasonDeleteCompleted).
		WithStatusAction(chi).
		Info("Delete CHI %s/%s - completed", chi.Namespace, chi.Name)

	// Exclude this CHI from monitoring
	w.c.deleteWatch(chi.Namespace, chi.Name)

	return nil
}

// deleteTables
func (w *worker) deleteTables(host *chop.ChiHost) error {
	if !host.CanDeleteAllPVCs() {
		return nil
	}
	err := w.schemer.HostDeleteTables(host)

	if err == nil {
		w.a.V(1).
			WithEvent(host.CHI, eventActionDelete, eventReasonDeleteCompleted).
			WithStatusAction(host.CHI).
			Info("Deleted tables on host %s replica %d to shard %d in cluster %s",
				host.Name, host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
	} else {
		w.a.WithEvent(host.CHI, eventActionDelete, eventReasonDeleteFailed).
			WithStatusError(host.CHI).
			Error("FAILED to delete tables on host %s with error %v", host.Name, err)
	}

	return err
}

// deleteHost deletes all kubernetes resources related to replica *chop.ChiHost
func (w *worker) deleteHost(host *chop.ChiHost) error {
	w.a.V(2).Info("deleteHost() - start")
	defer w.a.V(2).Info("deleteHost() - end")

	w.a.V(1).
		WithEvent(host.CHI, eventActionDelete, eventReasonDeleteStarted).
		WithStatusAction(host.CHI).
		Info("Delete host %s/%s - started", host.Address.ClusterName, host.Name)

	if _, err := w.c.getStatefulSetByHost(host); err != nil {
		w.a.WithEvent(host.CHI, eventActionDelete, eventReasonDeleteCompleted).
			WithStatusAction(host.CHI).
			Info("Delete host %s/%s - completed StatefulSet not found - already deleted? err: %v",
				host.Address.ClusterName, host.Name, err)
		return nil
	}

	// Each host consists of
	// 1. User-level objects - tables on the host
	//    We need to delete tables on the host in order to clean Zookeeper data.
	//    If just delete tables, Zookeeper will still keep track of non-existent tables
	// 2. Kubernetes-level objects - such as StatefulSet, PVC(s), ConfigMap(s), Service(s)
	// Need to delete all these items

	var err error
	err = w.deleteTables(host)
	err = w.c.deleteHost(host)

	// When deleting the whole CHI (not particular host), CHI may already be unavailable, so update CHI tolerantly
	host.CHI.Status.DeletedHostsCount++
	_ = w.c.updateCHIObjectStatus(host.CHI, true)

	if err == nil {
		w.a.V(1).
			WithEvent(host.CHI, eventActionDelete, eventReasonDeleteCompleted).
			WithStatusAction(host.CHI).
			Info("Delete host %s/%s - completed", host.Address.ClusterName, host.Name)
	} else {
		w.a.WithEvent(host.CHI, eventActionDelete, eventReasonDeleteFailed).
			WithStatusError(host.CHI).
			Error("FAILED Delete host %s/%s - completed", host.Address.ClusterName, host.Name)
	}

	return err
}

// deleteShard deletes all kubernetes resources related to shard *chop.ChiShard
func (w *worker) deleteShard(shard *chop.ChiShard) error {
	w.a.V(2).Info("deleteShard() - start")
	defer w.a.V(2).Info("deleteShard() - end")

	w.a.V(1).
		WithEvent(shard.CHI, eventActionDelete, eventReasonDeleteStarted).
		WithStatusAction(shard.CHI).
		Info("Delete shard %s/%s - started", shard.Address.Namespace, shard.Name)

	// Delete all replicas
	shard.WalkHosts(w.deleteHost)

	// Delete Shard Service
	_ = w.c.deleteServiceShard(shard)

	w.a.V(1).
		WithEvent(shard.CHI, eventActionDelete, eventReasonDeleteCompleted).
		WithStatusAction(shard.CHI).
		Info("Delete shard %s/%s - completed", shard.Address.Namespace, shard.Name)

	return nil
}

// deleteCluster deletes all kubernetes resources related to cluster *chop.ChiCluster
func (w *worker) deleteCluster(cluster *chop.ChiCluster) error {
	w.a.V(2).Info("deleteCluster() - start")
	defer w.a.V(2).Info("deleteCluster() - end")

	w.a.V(1).
		WithEvent(cluster.CHI, eventActionDelete, eventReasonDeleteStarted).
		WithStatusAction(cluster.CHI).
		Info("Delete cluster %s/%s - started", cluster.Address.Namespace, cluster.Name)

	// Delete all shards
	cluster.WalkShards(func(index int, shard *chop.ChiShard) error {
		return w.deleteShard(shard)
	})

	// Delete Cluster Service
	_ = w.c.deleteServiceCluster(cluster)

	w.a.V(1).
		WithEvent(cluster.CHI, eventActionDelete, eventReasonDeleteCompleted).
		WithStatusAction(cluster.CHI).
		Info("Delete cluster %s/%s - completed", cluster.Address.Namespace, cluster.Name)

	return nil
}

// createCHIFromObjectMeta
func (w *worker) createCHIFromObjectMeta(objectMeta *meta.ObjectMeta) (*chop.ClickHouseInstallation, error) {
	w.a.V(3).Info("createCHIFromObjectMeta() - start")
	defer w.a.V(3).Info("createCHIFromObjectMeta() - end")

	chi, err := w.c.GetCHIByObjectMeta(objectMeta)
	if err != nil {
		return nil, err
	}

	chi, err = w.normalizer.NormalizeCHI(chi)
	if err != nil {
		return nil, err
	}

	return chi, nil
}

// createClusterFromObjectMeta
func (w *worker) createClusterFromObjectMeta(objectMeta *meta.ObjectMeta) (*chop.ChiCluster, error) {
	w.a.V(3).Info("createClusterFromObjectMeta() - start")
	defer w.a.V(3).Info("createClusterFromObjectMeta() - end")

	clusterName, err := chopmodel.GetClusterNameFromObjectMeta(objectMeta)
	if err != nil {
		return nil, fmt.Errorf("ObjectMeta %s does not generated by CHI %v", objectMeta.Name, err)
	}

	chi, err := w.createCHIFromObjectMeta(objectMeta)
	if err != nil {
		return nil, err
	}

	cluster := chi.FindCluster(clusterName)
	if cluster == nil {
		return nil, fmt.Errorf("can't find cluster %s in CHI %s", clusterName, chi.Name)
	}

	return cluster, nil
}

// updateConfigMap
func (w *worker) updateConfigMap(chi *chop.ClickHouseInstallation, configMap *core.ConfigMap) error {
	_, err := w.c.kubeClient.CoreV1().ConfigMaps(configMap.Namespace).Update(configMap)

	if err == nil {
		w.a.V(1).
			WithEvent(chi, eventActionUpdate, eventReasonUpdateCompleted).
			WithStatusAction(chi).
			Info("Update ConfigMap %s/%s", configMap.Namespace, configMap.Name)
	} else {
		w.a.WithEvent(chi, eventActionUpdate, eventReasonUpdateFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			Error("Update ConfigMap %s/%s failed with error %v", configMap.Namespace, configMap.Name, err)
	}

	return err
}

// createConfigMap
func (w *worker) createConfigMap(chi *chop.ClickHouseInstallation, configMap *core.ConfigMap) error {
	_, err := w.c.kubeClient.CoreV1().ConfigMaps(configMap.Namespace).Create(configMap)

	if err == nil {
		w.a.V(1).
			WithEvent(chi, eventActionCreate, eventReasonCreateCompleted).
			WithStatusAction(chi).
			Info("Create ConfigMap %s/%s", configMap.Namespace, configMap.Name)
	} else {
		w.a.WithEvent(chi, eventActionCreate, eventReasonCreateFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			Error("Create ConfigMap %s/%s failed with error %v", configMap.Namespace, configMap.Name, err)
	}

	return err
}

// reconcileConfigMap reconciles core.ConfigMap which belongs to specified CHI
func (w *worker) reconcileConfigMap(chi *chop.ClickHouseInstallation, configMap *core.ConfigMap) error {
	w.a.V(2).Info("reconcileConfigMap() - start")
	defer w.a.V(2).Info("reconcileConfigMap() - end")

	// Check whether this object already exists in k8s
	curConfigMap, err := w.c.getConfigMap(&configMap.ObjectMeta, false)

	if curConfigMap != nil {
		return w.updateConfigMap(chi, configMap)
	}

	if apierrors.IsNotFound(err) {
		return w.createConfigMap(chi, configMap)
	}

	return err
}

// updateService
func (w *worker) updateService(chi *chop.ClickHouseInstallation, curService, newService *core.Service) error {
	// Updating a Service is a complicated business

	// spec.resourceVersion is required in order to update object
	newService.ResourceVersion = curService.ResourceVersion

	// spec.clusterIP field is immutable, need to use already assigned value
	// From https://kubernetes.io/docs/concepts/services-networking/service/#defining-a-service
	// Kubernetes assigns this Service an IP address (sometimes called the “cluster IP”), which is used by the Service proxies
	// See also https://kubernetes.io/docs/concepts/services-networking/service/#virtual-ips-and-service-proxies
	// You can specify your own cluster IP address as part of a Service creation request. To do this, set the .spec.clusterIP
	newService.Spec.ClusterIP = curService.Spec.ClusterIP

	// spec.healthCheckNodePort field is used with ExternalTrafficPolicy=Local only and is immutable within ExternalTrafficPolicy=Local
	// In case ExternalTrafficPolicy is changed it seems to be irrelevant
	// https://kubernetes.io/docs/tasks/access-application-cluster/create-external-load-balancer/#preserving-the-client-source-ip
	if (curService.Spec.ExternalTrafficPolicy == core.ServiceExternalTrafficPolicyTypeLocal) &&
		(newService.Spec.ExternalTrafficPolicy == core.ServiceExternalTrafficPolicyTypeLocal) {
		newService.Spec.HealthCheckNodePort = curService.Spec.HealthCheckNodePort
	}

	// And only now we are ready to actually update the service with new version of the service
	_, err := w.c.kubeClient.CoreV1().Services(newService.Namespace).Update(newService)

	if err == nil {
		w.a.V(1).
			WithEvent(chi, eventActionUpdate, eventReasonUpdateCompleted).
			WithStatusAction(chi).
			Info("Update Service %s/%s", newService.Namespace, newService.Name)
	} else {
		w.a.WithEvent(chi, eventActionUpdate, eventReasonUpdateFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			Error("Update Service %s/%s failed with error %v", newService.Namespace, newService.Name, err)
	}

	return err
}

// createService
func (w *worker) createService(chi *chop.ClickHouseInstallation, service *core.Service) error {
	_, err := w.c.kubeClient.CoreV1().Services(service.Namespace).Create(service)

	if err == nil {
		w.a.V(1).
			WithEvent(chi, eventActionCreate, eventReasonCreateCompleted).
			WithStatusAction(chi).
			Info("Create Service %s/%s", service.Namespace, service.Name)
	} else {
		w.a.WithEvent(chi, eventActionCreate, eventReasonCreateFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			Error("Create Service %s/%s failed with error %v", service.Namespace, service.Name, err)
	}

	return err
}

// reconcileService reconciles core.Service
func (w *worker) reconcileService(chi *chop.ClickHouseInstallation, service *core.Service) error {
	w.a.V(2).Info("reconcileService() - start")
	defer w.a.V(2).Info("reconcileService() - end")

	// Check whether this object already exists
	curService, err := w.c.getService(&service.ObjectMeta, false)

	if curService != nil {
		return w.updateService(chi, curService, service)
	}

	if apierrors.IsNotFound(err) {
		return w.createService(chi, service)
	}

	return err
}

// reconcileStatefulSet reconciles apps.StatefulSet
func (w *worker) reconcileStatefulSet(newStatefulSet *apps.StatefulSet, host *chop.ChiHost) error {
	w.a.V(2).Info("reconcileStatefulSet() - start")
	defer w.a.V(2).Info("reconcileStatefulSet() - end")

	// Check whether this object already exists in k8s
	curStatefulSet, err := w.c.getStatefulSet(&newStatefulSet.ObjectMeta, false)

	if curStatefulSet != nil {
		return w.updateStatefulSet(curStatefulSet, newStatefulSet, host)
	}

	if apierrors.IsNotFound(err) {
		return w.createStatefulSet(newStatefulSet, host)
	}

	// Not create, not update - weird thing

	w.a.WithEvent(host.CHI, eventActionCreate, eventReasonCreateFailed).
		WithStatusAction(host.CHI).
		WithStatusError(host.CHI).
		Error("Create or Update StatefulSet %s/%s - UNEXPECTED FLOW", newStatefulSet.Namespace, newStatefulSet.Name)

	return err
}

func (w *worker) reconcilePersistentVolumes(host *chop.ChiHost) {
	w.c.walkPVs(host, func(pv *core.PersistentVolume) {
		pv = w.creator.PreparePersistentVolume(pv, host)
		_ = w.c.updatePersistentVolume(pv)
	})
}

// createStatefulSet
func (w *worker) createStatefulSet(statefulSet *apps.StatefulSet, host *chop.ChiHost) error {
	w.a.V(2).Info("createStatefulSet() - start")
	defer w.a.V(2).Info("createStatefulSet() - end")

	w.a.V(1).
		WithEvent(host.CHI, eventActionCreate, eventReasonCreateStarted).
		WithStatusAction(host.CHI).
		Info("Create StatefulSet %s/%s - started", statefulSet.Namespace, statefulSet.Name)

	err := w.c.createStatefulSet(statefulSet, host)

	host.CHI.Status.AddedHostsCount++
	_ = w.c.updateCHIObjectStatus(host.CHI, false)

	if err == nil {
		w.a.V(1).
			WithEvent(host.CHI, eventActionCreate, eventReasonCreateCompleted).
			WithStatusAction(host.CHI).
			Info("Create StatefulSet %s/%s - completed", statefulSet.Namespace, statefulSet.Name)
	} else {
		w.a.WithEvent(host.CHI, eventActionCreate, eventReasonCreateFailed).
			WithStatusAction(host.CHI).
			WithStatusError(host.CHI).
			Error("Create StatefulSet %s/%s - failed with error %v", statefulSet.Namespace, statefulSet.Name, err)
	}

	return err
}

// updateStatefulSet
func (w *worker) updateStatefulSet(curStatefulSet, newStatefulSet *apps.StatefulSet, host *chop.ChiHost) error {
	w.a.V(2).Info("updateStatefulSet() - start")
	defer w.a.V(2).Info("updateStatefulSet() - end")

	namespace := newStatefulSet.Namespace
	name := newStatefulSet.Name

	w.a.V(1).
		WithEvent(host.CHI, eventActionCreate, eventReasonCreateStarted).
		WithStatusAction(host.CHI).
		Info("Update StatefulSet(%s/%s) - started", namespace, name)

	err := w.c.updateStatefulSet(curStatefulSet, newStatefulSet)
	if err == nil {
		host.CHI.Status.UpdatedHostsCount++
		_ = w.c.updateCHIObjectStatus(host.CHI, false)
		w.a.V(1).
			WithEvent(host.CHI, eventActionUpdate, eventReasonUpdateCompleted).
			WithStatusAction(host.CHI).
			Info("Update StatefulSet(%s/%s) - completed", namespace, name)
		return nil
	}

	w.a.WithEvent(host.CHI, eventActionUpdate, eventReasonUpdateFailed).
		WithStatusAction(host.CHI).
		WithStatusError(host.CHI).
		Error("Update StatefulSet(%s/%s) - failed with error\n---\n%v\n--\nContinue with recreate", namespace, name, err)

	diff, equal := messagediff.DeepDiff(curStatefulSet.Spec, newStatefulSet.Spec)
	w.a.Info("StatefulSet.Spec diff:")
	w.a.Info(util.MessageDiffString(diff, equal))

	err = w.c.deleteStatefulSet(host)
	err = w.reconcilePVCs(host)
	return w.createStatefulSet(newStatefulSet, host)
}

// reconcilePVCs
func (w *worker) reconcilePVCs(host *chop.ChiHost) error {
	namespace := host.Address.Namespace
	w.a.V(2).Info("reconcilePVCs for host %s/%s - start", namespace, host.Name)
	defer w.a.V(2).Info("reconcilePVCs for host %s/%s - end", namespace, host.Name)

	host.WalkVolumeMounts(func(volumeMount *core.VolumeMount) {
		volumeClaimTemplateName := volumeMount.Name
		volumeClaimTemplate, ok := host.CHI.GetVolumeClaimTemplate(volumeClaimTemplateName)
		if !ok {
			// No this is not a reference to VolumeClaimTemplate
			return
		}

		pvcName := chopmodel.CreatePVCName(host, volumeMount, volumeClaimTemplate)
		w.a.V(2).Info("reconcile volumeMount (%s/%s/%s/%s) - start", namespace, host.Name, volumeMount.Name, pvcName)
		defer w.a.V(2).Info("reconcile volumeMount (%s/%s/%s/%s) - end", namespace, host.Name, volumeMount.Name, pvcName)

		pvc, err := w.c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(pvcName, newGetOptions())
		if err != nil {
			if apierrors.IsNotFound(err) {
				// This is not an error per se, means PVC is not created (yet)?
			} else {
				w.a.Error("ERROR unable to get PVC(%s/%s) err: %v", namespace, pvcName, err)
			}
			return
		}
		w.reconcileResources(pvc, volumeClaimTemplate)
	})

	return nil
}

// reconcileResources
func (w *worker) reconcileResources(pvc *core.PersistentVolumeClaim, template *chop.ChiVolumeClaimTemplate) {
	w.reconcileResourcesList(pvc, pvc.Spec.Resources.Requests, template.Spec.Resources.Requests)
}

// reconcileResourcesList
func (w *worker) reconcileResourcesList(pvc *core.PersistentVolumeClaim, pvcResourceList, desiredResourceList core.ResourceList) {
	var pvcResourceNames []core.ResourceName
	for resourceName := range pvcResourceList {
		pvcResourceNames = append(pvcResourceNames, resourceName)
	}
	var desiredResourceNames []core.ResourceName
	for resourceName := range desiredResourceList {
		desiredResourceNames = append(desiredResourceNames, resourceName)
	}

	//diff, equal := messagediff.DeepDiff(pvcResourceNames, desiredResourceNames)

	resourceNames := intersect.Simple(pvcResourceNames, desiredResourceNames)
	for _, resourceName := range resourceNames.([]interface{}) {
		w.reconcileResource(pvc, pvcResourceList, desiredResourceList, resourceName.(core.ResourceName))
	}
}

// reconcileResourcesList
func (w *worker) reconcileResource(
	pvc *core.PersistentVolumeClaim,
	pvcResourceList core.ResourceList,
	desiredResourceList core.ResourceList,
	resourceName core.ResourceName,
) {
	w.a.V(2).Info("reconcileResource(%s/%s/%s) - start", pvc.Namespace, pvc.Name, resourceName)
	defer w.a.V(2).Info("reconcileResource(%s/%s/%s) - end", pvc.Namespace, pvc.Name, resourceName)

	var ok bool
	if (pvcResourceList == nil) || (desiredResourceList == nil) {
		return
	}

	var pvcResourceQuantity resource.Quantity
	var desiredResourceQuantity resource.Quantity
	if pvcResourceQuantity, ok = pvcResourceList[resourceName]; !ok {
		return
	}
	if desiredResourceQuantity, ok = desiredResourceList[resourceName]; !ok {
		return
	}

	if pvcResourceQuantity.Equal(desiredResourceQuantity) {
		return
	}

	w.a.V(2).Info("reconcileResource(%s/%s/%s) - unequal requests, want to update", pvc.Namespace, pvc.Name, resourceName)
	pvcResourceList[resourceName] = desiredResourceList[resourceName]
	_, err := w.c.kubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(pvc)
	if err != nil {
		w.a.Error("unable to reconcileResource(%s/%s/%s) err: %v", pvc.Namespace, pvc.Name, resourceName, err)
		return
	}
}
