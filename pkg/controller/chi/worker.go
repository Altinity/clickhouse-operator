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
	"fmt"
	"time"

	"github.com/juliangruber/go-intersect"
	"gopkg.in/d4l3k/messagediff.v1"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	//"github.com/altinity/queue"

	"github.com/altinity/queue"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopmodel "github.com/altinity/clickhouse-operator/pkg/model"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

const FinalizerName = "finalizer.clickhouseinstallation.altinity.com"

// worker represents worker thread which runs reconcile tasks
type worker struct {
	c *Controller
	a Announcer
	//queue workqueue.RateLimitingInterface
	queue      queue.PriorityQueue
	normalizer *chopmodel.Normalizer
	schemer    *chopmodel.Schemer
	creator    *chopmodel.Creator
	start      time.Time
}

// newWorker
//func (c *Controller) newWorker(q workqueue.RateLimitingInterface) *worker {
func (c *Controller) newWorker(q queue.PriorityQueue) *worker {
	return &worker{
		c:          c,
		a:          NewAnnouncer().WithController(c),
		queue:      q,
		normalizer: chopmodel.NewNormalizer(c.chop),
		schemer: chopmodel.NewSchemer(
			c.chop.Config().CHUsername,
			c.chop.Config().CHPassword,
			c.chop.Config().CHPort,
		),
		creator: nil,
		start:   time.Now().Add(chop.DefaultReconcileThreadsWarmup),
	}
}

// run is an endless work loop, expected to be run in a thread
func (w *worker) run() {
	w.a.V(2).S().P()
	defer w.a.V(2).E().P()

	util.WaitContextDoneUntil(context.Background(), w.start)

	for {
		// Get() blocks until it can return an item
		item, ctx, ok := w.queue.Get()
		if !ok {
			w.a.Info("shutdown request")
			return
		}

		//item, shut := w.queue.Get()
		//ctx := context.Background()
		//if shut {
		//	w.a.Info("shutdown request")
		//	return
		//}

		if err := w.processItem(ctx, item); err != nil {
			// Item not processed
			// this code cannot return an error and needs to indicate error has been ignored
			utilruntime.HandleError(err)
		}

		// Forget indicates that an item is finished being retried.  Doesn't matter whether its for perm failing
		// or for success, we'll stop the rate limiter from tracking it.  This only clears the `rateLimiter`, you
		// still have to call `Done` on the queue.
		//w.queue.Forget(item)

		// Remove item from processing set when processing completed
		w.queue.Done(item)
	}
}

// processItem processes one work item according to its type
func (w *worker) processItem(ctx context.Context, item interface{}) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(3).S().P()
	defer w.a.V(3).E().P()

	switch command := item.(type) {

	case *ReconcileCHI:
		switch command.cmd {
		case reconcileAdd:
			return w.updateCHI(ctx, nil, command.new)
		case reconcileUpdate:
			return w.updateCHI(ctx, command.old, command.new)
		case reconcileDelete:
			return w.deleteCHI(ctx, command.old)
		}

		// Unknown item type, don't know what to do with it
		// Just skip it and behave like it never existed
		utilruntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", command))
		return nil

	case *ReconcileCHIT:
		switch command.cmd {
		case reconcileAdd:
			return w.c.addChit(command.new)
		case reconcileUpdate:
			return w.c.updateChit(command.old, command.new)
		case reconcileDelete:
			return w.c.deleteChit(command.old)
		}

		// Unknown item type, don't know what to do with it
		// Just skip it and behave like it never existed
		utilruntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", command))
		return nil

	case *ReconcileChopConfig:
		switch command.cmd {
		case reconcileAdd:
			return w.c.addChopConfig(command.new)
		case reconcileUpdate:
			return w.c.updateChopConfig(command.old, command.new)
		case reconcileDelete:
			return w.c.deleteChopConfig(command.old)
		}

		// Unknown item type, don't know what to do with it
		// Just skip it and behave like it never existed
		utilruntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", command))
		return nil

	case *DropDns:
		if chi, err := w.createCHIFromObjectMeta(command.initiator); err == nil {
			w.a.V(2).M(command.initiator).Info("flushing DNS for CHI %s", chi.Name)
			_ = w.schemer.CHIDropDnsCache(ctx, chi)
		} else {
			w.a.M(command.initiator).A().Error("unable to find CHI by %v", command.initiator.Labels)
		}
		return nil
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilruntime.HandleError(fmt.Errorf("unexpected item in the queue - %#v", item))
	return nil
}

// normalize
func (w *worker) normalize(c *chop.ClickHouseInstallation) *chop.ClickHouseInstallation {
	w.a.V(3).M(c).S().P()
	defer w.a.V(3).M(c).E().P()

	chi, err := w.normalizer.CreateTemplatedCHI(c)
	if err != nil {
		w.a.WithEvent(chi, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusError(chi).
			M(chi).A().
			Error("FAILED to normalize CHI : %v", err)
	}

	return chi
}

// ensureFinalizer
func (w *worker) ensureFinalizer(chi *chop.ClickHouseInstallation) bool {
	// Check whether finalizer is already listed in CHI
	if util.InArray(FinalizerName, chi.ObjectMeta.Finalizers) {
		w.a.V(2).M(chi).F().Info("finalizer already installed")
		return false
	}

	// No finalizer found - need to install it

	if err := w.c.installFinalizer(chi); err != nil {
		w.a.V(1).M(chi).A().Error("unable to install finalizer. err: %v", err)
		return false
	}

	w.a.V(3).M(chi).F().Info("finalizer installed")
	return true
}

// updateCHI sync CHI which was already created earlier
func (w *worker) updateCHI(ctx context.Context, old, new *chop.ClickHouseInstallation) error {
	w.a.V(3).M(new).S().P()
	defer w.a.V(3).M(new).E().P()

	update := (old != nil) && (new != nil)

	if update && (old.ObjectMeta.ResourceVersion == new.ObjectMeta.ResourceVersion) {
		// No need to react
		w.a.V(3).M(new).F().Info("ResourceVersion did not change: %s", new.ObjectMeta.ResourceVersion)
		return nil
	}

	ctx = chopmodel.NewReconcileContext(ctx)

	// Check DeletionTimestamp in order to understand, whether the object is being deleted
	if new.ObjectMeta.DeletionTimestamp.IsZero() {
		// The object is not being deleted
		// Need to ensure finalizer is in place
		if w.ensureFinalizer(new) {
			// Finalizer installed, let's restart reconcile cycle
			return nil
		}
	} else {
		// The object is being deleted
		return w.finalizeCHI(ctx, new)
	}

	//time.Sleep(5 * time.Second)
	//w.a.V(3).Info("--- Normalize ---")

	old = w.normalize(old)
	new = w.normalize(new)

	//time.Sleep(5 * time.Second)
	//w.a.V(3).Info("--- Action Plan ---")

	actionPlan := NewActionPlan(old, new)
	if !actionPlan.HasActionsToDo() {
		// Nothing to do - no changes found - no need to react
		w.a.V(3).M(new).F().Info("ResourceVersion changed, but no actual changes found")
		return nil
	}

	//time.Sleep(5 * time.Second)
	//w.a.V(3).Info("--- Reconcile Start 1 ---")

	// Write desired normalized CHI with initialized .Status, so it would be possible to monitor progress
	(&new.Status).ReconcileStart(actionPlan.GetRemovedHostsNum())
	_ = w.c.updateCHIObjectStatus(ctx, new, false)

	w.a.V(1).
		WithEvent(new, eventActionReconcile, eventReasonReconcileStarted).
		WithStatusAction(new).
		M(new).F().
		Info("reconcile started")
	w.a.V(2).M(new).F().Info("action plan\n%s\n", actionPlan.String())

	if new.IsStopped() {
		w.a.V(1).
			WithEvent(new, eventActionReconcile, eventReasonReconcileInProgress).
			WithStatusAction(new).
			M(new).F().
			Info("exclude CHI from monitoring")
		w.c.deleteWatch(new.Namespace, new.Name)
	}

	//time.Sleep(5 * time.Second)
	//w.a.V(3).Info("--- Reconcile Start 3 ---")

	actionPlan.WalkAdded(
		func(cluster *chop.ChiCluster) {
			cluster.WalkHosts(func(host *chop.ChiHost) error {
				(&host.ReconcileAttributes).SetAdd()
				return nil
			})
		},
		func(shard *chop.ChiShard) {
			shard.WalkHosts(func(host *chop.ChiHost) error {
				(&host.ReconcileAttributes).SetAdd()
				return nil
			})
		},
		func(host *chop.ChiHost) {
			(&host.ReconcileAttributes).SetAdd()
		},
	)

	actionPlan.WalkModified(
		func(cluster *chop.ChiCluster) {
		},
		func(shard *chop.ChiShard) {
		},
		func(host *chop.ChiHost) {
			(&host.ReconcileAttributes).SetModify()
		},
	)

	new.WalkHosts(func(host *chop.ChiHost) error {
		if host.ReconcileAttributes.IsAdd() {
			// Already added
		} else if host.ReconcileAttributes.IsModify() {
			// Already modified
		} else {
			// Not clear yet
			(&host.ReconcileAttributes).SetUnclear()
		}
		return nil
	})

	new.WalkHosts(func(host *chop.ChiHost) error {
		if host.ReconcileAttributes.IsAdd() {
			w.a.M(host).Info("ADD host: %s", host.Address.CompactString())
		} else if host.ReconcileAttributes.IsModify() {
			w.a.M(host).Info("MODIFY host: %s", host.Address.CompactString())
		} else if host.ReconcileAttributes.IsUnclear() {
			w.a.M(host).Info("UNCLEAR host: %s", host.Address.CompactString())
		} else {
			w.a.M(host).Info("UNTOUCHED host: %s", host.Address.CompactString())
		}
		return nil
	})

	//time.Sleep(5 * time.Second)
	//w.a.V(3).Info("--- Reconcile Start Real ---")

	if err := w.reconcile(ctx, new); err != nil {
		w.a.WithEvent(new, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusError(new).
			M(new).A().
			Error("FAILED update: %v", err)
		return nil
	}

	// Post-process added items
	w.a.V(1).
		WithEvent(new, eventActionReconcile, eventReasonReconcileInProgress).
		WithStatusAction(new).
		M(new).F().
		Info("remove items scheduled for deletion")
	actionPlan.WalkAdded(
		func(cluster *chop.ChiCluster) {
		},
		func(shard *chop.ChiShard) {
		},
		func(host *chop.ChiHost) {
			//
			//if update {
			//	w.a.V(1).
			//		WithEvent(new, eventActionCreate, eventReasonCreateStarted).
			//		WithStatusAction(new).
			//		Info("Adding tables on shard/host:%d/%d cluster:%s", host.Address.ShardIndex, host.Address.ReplicaIndex, host.Address.ClusterName)
			//	if err := w.schemer.HostCreateTables(host); err != nil {
			//		w.a.Error("ERROR create tables on host %s. err: %v", host.Name, err)
			//	}
			//} else {
			//	w.a.V(1).
			//		Info("As CHI is just created, not need to add tables on host %d to shard %d in cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
			//}
		},
	)

	// Remove deleted items
	w.a.V(1).
		WithEvent(new, eventActionReconcile, eventReasonReconcileInProgress).
		WithStatusAction(new).
		M(new).F().
		Info("remove items scheduled for deletion")

	objs := w.c.discovery(new)
	need := chopmodel.ReconcileContextGetRegistry(ctx)
	w.a.V(1).M(new).F().Info("Reconciled objects:\n%s", chopmodel.ReconcileContextGetRegistry(ctx))
	w.a.V(1).M(new).F().Info("Existing objects:\n%s", objs)
	objs.Subtract(need)
	w.a.V(1).M(new).F().Info("Delete objects:\n%s", objs)

	/*
		actionPlan.WalkRemoved(
			func(cluster *chop.ChiCluster) {
				_ = w.deleteCluster(ctx, new, cluster)
			},
			func(shard *chop.ChiShard) {
				_ = w.deleteShard(ctx, new, shard)
			},
			func(host *chop.ChiHost) {
				_ = w.deleteHost(ctx, new, host)
			},
		)
	*/

	objs.Walk(func(entityType chopmodel.EntityType, m meta.ObjectMeta) {
		switch entityType {
		case chopmodel.StatefulSet:
			w.c.kubeClient.AppsV1().StatefulSets(m.Namespace).Delete(m.Name, newDeleteOptions())
		case chopmodel.ConfigMap:
			w.c.kubeClient.CoreV1().ConfigMaps(m.Namespace).Delete(m.Name, newDeleteOptions())
		case chopmodel.Service:
			w.c.kubeClient.CoreV1().Services(m.Namespace).Delete(m.Name, newDeleteOptions())
		}
	})

	if !new.IsStopped() {
		w.a.V(1).
			WithEvent(new, eventActionReconcile, eventReasonReconcileInProgress).
			WithStatusAction(new).
			M(new).F().
			Info("add CHI to monitoring")
		w.c.updateWatch(new.Namespace, new.Name, chopmodel.CreatePodFQDNsOfCHI(new))
	}

	// Update CHI object
	(&new.Status).ReconcileComplete()
	_ = w.c.updateCHIObjectStatus(ctx, new, false)

	w.a.V(1).
		WithEvent(new, eventActionReconcile, eventReasonReconcileCompleted).
		WithStatusActions(new).
		M(new).F().
		Info("reconcile completed")

	return nil
}

// reconcile reconciles ClickHouseInstallation
func (w *worker) reconcile(ctx context.Context, chi *chop.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(chi).S().P()
	defer w.a.V(2).M(chi).E().P()

	w.creator = chopmodel.NewCreator(w.c.chop, chi)
	return chi.WalkTillError(
		ctx,
		w.reconcileCHIAuxObjectsPreliminary,
		w.reconcileCluster,
		w.reconcileShard,
		w.reconcileHost,
		w.reconcileCHIAuxObjectsFinal,
	)
}

// reconcileCHIAuxObjectsPreliminary reconciles CHI preliminary in order to ensure that ConfigMaps are in place
func (w *worker) reconcileCHIAuxObjectsPreliminary(ctx context.Context, chi *chop.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(chi).S().P()
	defer w.a.V(2).M(chi).E().P()

	// 1. CHI Service
	if chi.IsStopped() {
		// Stopped cluster must have no entry point
		_ = w.c.deleteServiceCHI(ctx, chi)
	} else {
		if service := w.creator.CreateServiceCHI(); service != nil {
			if err := w.reconcileService(ctx, chi, service); err != nil {
				return err
			}
			chopmodel.ReconcileContextGetRegistry(ctx).RegisterService(service.ObjectMeta)
		}
	}

	// 2. CHI common ConfigMap without update - create only
	if err := w.reconcileCHIConfigMapCommon(ctx, chi, nil, false); err != nil {
		w.a.A().Error("failed to reconcile config map common. err: %v", err)
	}
	// 3. CHI users ConfigMap
	if err := w.reconcileCHIConfigMapUsers(ctx, chi, nil, true); err != nil {
		w.a.A().Error("failed to reconcile config map users. err: %v", err)
	}

	return nil
}

// reconcileCHIAuxObjectsFinal reconciles CHI global objects
func (w *worker) reconcileCHIAuxObjectsFinal(ctx context.Context, chi *chop.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(chi).S().P()
	defer w.a.V(2).M(chi).E().P()

	// CHI ConfigMaps with update
	return w.reconcileCHIConfigMapCommon(ctx, chi, nil, true)
}

// reconcileCHIConfigMaps reconciles all CHI's ConfigMaps
func (w *worker) reconcileCHIConfigMaps(
	ctx context.Context,
	chi *chop.ClickHouseInstallation,
	options *chopmodel.ClickHouseConfigFilesGeneratorOptions,
	update bool,
) error {
	if util.IsContextDone(ctx) {
		return nil
	}

	if err := w.reconcileCHIConfigMapCommon(ctx, chi, options, update); err != nil {
		return err
	}

	if err := w.reconcileCHIConfigMapUsers(ctx, chi, options, update); err != nil {
		return err
	}

	return nil
}

// reconcileCHIConfigMapCommon reconciles all CHI's common ConfigMap
func (w *worker) reconcileCHIConfigMapCommon(
	ctx context.Context,
	chi *chop.ClickHouseInstallation,
	options *chopmodel.ClickHouseConfigFilesGeneratorOptions,
	update bool,
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	// ConfigMap common for all resources in CHI
	// contains several sections, mapped as separated chopConfig files,
	// such as remote servers, zookeeper setup, etc
	configMapCommon := w.creator.CreateConfigMapCHICommon(options)
	err := w.reconcileConfigMap(ctx, chi, configMapCommon, update)
	if err == nil {
		chopmodel.ReconcileContextGetRegistry(ctx).RegisterConfigMap(configMapCommon.ObjectMeta)
	}
	return err
}

// reconcileCHIConfigMapUsers reconciles all CHI's users ConfigMap
// ConfigMap common for all users resources in CHI
func (w *worker) reconcileCHIConfigMapUsers(
	ctx context.Context,
	chi *chop.ClickHouseInstallation,
	options *chopmodel.ClickHouseConfigFilesGeneratorOptions,
	update bool,
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	// ConfigMap common for all users resources in CHI
	configMapUsers := w.creator.CreateConfigMapCHICommonUsers()
	err := w.reconcileConfigMap(ctx, chi, configMapUsers, update)
	if err == nil {
		chopmodel.ReconcileContextGetRegistry(ctx).RegisterConfigMap(configMapUsers.ObjectMeta)
	}
	return err
}

// reconcileCluster reconciles Cluster, excluding nested shards
func (w *worker) reconcileCluster(ctx context.Context, cluster *chop.ChiCluster) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(cluster).S().P()
	defer w.a.V(2).M(cluster).E().P()

	// Add Cluster's Service
	service := w.creator.CreateServiceCluster(cluster)
	if service == nil {
		// This is not a problem, ServiceCluster may be omitted
		return nil
	}
	err := w.reconcileService(ctx, cluster.CHI, service)
	if err == nil {
		chopmodel.ReconcileContextGetRegistry(ctx).RegisterService(service.ObjectMeta)
	}
	return err
}

// reconcileShard reconciles Shard, excluding nested replicas
func (w *worker) reconcileShard(ctx context.Context, shard *chop.ChiShard) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(shard).S().P()
	defer w.a.V(2).M(shard).E().P()

	// Add Shard's Service
	service := w.creator.CreateServiceShard(shard)
	if service == nil {
		// This is not a problem, ServiceShard may be omitted
		return nil
	}
	err := w.reconcileService(ctx, shard.CHI, service)
	if err == nil {
		chopmodel.ReconcileContextGetRegistry(ctx).RegisterService(service.ObjectMeta)
	}
	return err
}

// reconcileHost reconciles ClickHouse host
func (w *worker) reconcileHost(ctx context.Context, host *chop.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(host).S().P()
	defer w.a.V(2).M(host).E().P()

	w.a.V(1).
		WithEvent(host.GetCHI(), eventActionReconcile, eventReasonReconcileStarted).
		WithStatusAction(host.GetCHI()).
		M(host).F().
		Info("Reconcile Host %s started", host.Name)

	// Create artifacts
	configMap := w.creator.CreateConfigMapHost(host)
	statefulSet := w.creator.CreateStatefulSet(host)
	service := w.creator.CreateServiceHost(host)
	(&host.ReconcileAttributes).SetStatus(w.getStatefulSetStatus(statefulSet, host))

	if err := w.excludeHost(ctx, host); err != nil {
		return err
	}

	// Reconcile host's ConfigMap
	if err := w.reconcileConfigMap(ctx, host.CHI, configMap, true); err != nil {
		return err
	}
	chopmodel.ReconcileContextGetRegistry(ctx).RegisterConfigMap(configMap.ObjectMeta)

	// Reconcile host's StatefulSet
	if err := w.reconcileStatefulSet(ctx, statefulSet, host); err != nil {
		return err
	}
	chopmodel.ReconcileContextGetRegistry(ctx).RegisterStatefulSet(statefulSet.ObjectMeta)

	// Reconcile host's Persistent Volumes
	w.reconcilePersistentVolumes(ctx, host)

	if service != nil {
		// Reconcile host's Service
		if err := w.reconcileService(ctx, host.CHI, service); err != nil {
			return err
		}
		chopmodel.ReconcileContextGetRegistry(ctx).RegisterService(service.ObjectMeta)
	}

	host.ReconcileAttributes.UnsetAdd()

	_ = w.migrateTables(ctx, host)

	if err := w.includeHost(ctx, host); err != nil {
		// If host is not ready - fallback
		return err
	}

	w.a.V(1).
		WithEvent(host.CHI, eventActionReconcile, eventReasonReconcileCompleted).
		WithStatusAction(host.CHI).
		M(host).F().
		Info("Reconcile Host %s completed", host.Name)

	return nil
}

func (w *worker) migrateTables(ctx context.Context, host *chop.ChiHost) error {
	if !w.shouldMigrateTables(host) {
		w.a.V(1).
			M(host).F().
			Info("No need to add tables on host %d to shard %d in cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
		return nil
	}

	// Need to migrate tables

	w.a.V(1).
		WithEvent(host.GetCHI(), eventActionCreate, eventReasonCreateStarted).
		WithStatusAction(host.GetCHI()).
		M(host).F().
		Info("Adding tables on shard/host:%d/%d cluster:%s", host.Address.ShardIndex, host.Address.ReplicaIndex, host.Address.ClusterName)
	err := w.schemer.HostCreateTables(ctx, host)
	if err != nil {
		w.a.M(host).A().Error("ERROR create tables on host %s. err: %v", host.Name, err)
	}
	return err
}

func (w *worker) shouldMigrateTables(host *chop.ChiHost) bool {
	if host.GetCHI().IsStopped() {
		return false
	}
	if host.ReconcileAttributes.GetStatus() == chop.StatefulSetStatusSame {
		return false
	}
	return true
}

// Exclude host from ClickHouse clusters if required
func (w *worker) excludeHost(ctx context.Context, host *chop.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	if w.shouldExcludeHost(host) {
		w.a.V(1).
			M(host).F().
			Info("Exclude from cluster host %d shard %d cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)

		w.excludeHostFromService(host)
		w.excludeHostFromClickHouseCluster(ctx, host)
	}
	return nil
}

// Always include host back to ClickHouse clusters
func (w *worker) includeHost(ctx context.Context, host *chop.ChiHost) error {
	w.a.V(1).
		M(host).F().
		Info("Include into cluster host %d shard %d cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)

	w.includeHostIntoClickHouseCluster(ctx, host)
	w.includeHostIntoService(host)

	return nil
}

func (w *worker) excludeHostFromService(host *chop.ChiHost) {
	w.c.deleteLabelReady(host)
}

func (w *worker) includeHostIntoService(host *chop.ChiHost) {
	w.c.appendLabelReady(host)
}

// excludeHostFromClickHouseCluster excludes host from ClickHouse configuration
func (w *worker) excludeHostFromClickHouseCluster(ctx context.Context, host *chop.ChiHost) {
	// Specify in options to exclude host from ClickHouse config file
	options := chopmodel.NewClickHouseConfigFilesGeneratorOptions().
		SetRemoteServersGeneratorOptions(
			chopmodel.NewRemoteServersGeneratorOptions().
				ExcludeHost(host).
				ExcludeReconcileAttributes(
					chop.NewChiHostReconcileAttributes().SetAdd(),
				),
		)

	// Remove host from cluster config and wait for ClickHouse to pick-up the change
	if w.waitExcludeHost(host) {
		_ = w.reconcileCHIConfigMapCommon(ctx, host.GetCHI(), options, true)
		_ = w.waitHostNotInCluster(ctx, host)
	}
}

// includeHostIntoClickHouseCluster includes host to ClickHouse configuration
func (w *worker) includeHostIntoClickHouseCluster(ctx context.Context, host *chop.ChiHost) {
	options := chopmodel.NewClickHouseConfigFilesGeneratorOptions().
		SetRemoteServersGeneratorOptions(chopmodel.NewRemoteServersGeneratorOptions().
			ExcludeReconcileAttributes(
				chop.NewChiHostReconcileAttributes().SetAdd(),
			),
		)
		// Add host to the cluster config (always) and wait for ClickHouse to pick-up the change
	_ = w.reconcileCHIConfigMapCommon(ctx, host.GetCHI(), options, true)
	if w.waitIncludeHost(host) {
		_ = w.waitHostInCluster(ctx, host)
	}
}

// shouldExcludeHost determines whether host to be excluded from cluster
func (w *worker) shouldExcludeHost(host *chop.ChiHost) bool {
	status := host.ReconcileAttributes.GetStatus()
	if (status == chop.StatefulSetStatusNew) || (status == chop.StatefulSetStatusSame) {
		// No need to exclude for new and non-modified StatefulSets
		return false
	}

	if host.GetShard().HostsCount() == 1 {
		// In case shard where current host is located has only one host (means no replication), no need to exclude
		return false
	}

	return true
}

// determines whether reconciler should wait for host to be excluded from cluster
func (w *worker) waitExcludeHost(host *chop.ChiHost) bool {
	// Check CHI settings
	switch {
	case host.GetCHI().IsReconcilingPolicyWait():
		return true
	case host.GetCHI().IsReconcilingPolicyNoWait():
		return false
	}

	// Fallback to operator's settings
	return w.c.chop.Config().ReconcileWaitExclude
}

// determines whether reconciler should wait for host to be included into cluster
func (w *worker) waitIncludeHost(host *chop.ChiHost) bool {
	status := host.ReconcileAttributes.GetStatus()
	if (status == chop.StatefulSetStatusNew) || (status == chop.StatefulSetStatusSame) {
		return false
	}

	if host.GetShard().HostsCount() == 1 {
		// In case shard where current host is located has only one host (means no replication), no need to wait
		return false
	}

	// Check CHI settings
	switch {
	case host.GetCHI().IsReconcilingPolicyWait():
		return true
	case host.GetCHI().IsReconcilingPolicyNoWait():
		return false
	}

	// Fallback to operator's settings
	return w.c.chop.Config().ReconcileWaitInclude
}

func (w *worker) waitHostInCluster(ctx context.Context, host *chop.ChiHost) error {
	return w.c.pollHostContext(ctx, host, nil, w.schemer.IsHostInCluster)
}

func (w *worker) waitHostNotInCluster(ctx context.Context, host *chop.ChiHost) error {
	return w.c.pollHostContext(ctx, host, nil, func(ctx context.Context, host *chop.ChiHost) bool {
		return !w.schemer.IsHostInCluster(ctx, host)
	})
}

// finalizeCHI
func (w *worker) finalizeCHI(ctx context.Context, chi *chop.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(3).M(chi).S().P()
	defer w.a.V(3).M(chi).E().P()

	cur, err := w.c.chopClient.ClickhouseV1().ClickHouseInstallations(chi.Namespace).Get(chi.Name, newGetOptions())
	if (err != nil) || (cur == nil) {
		return nil
	}

	if !util.InArray(FinalizerName, chi.ObjectMeta.Finalizers) {
		// No finalizer found, unexpected behavior
		return nil
	}

	// Delete CHI
	(&chi.Status).DeleteStart()
	if err := w.c.updateCHIObjectStatus(ctx, chi, true); err != nil {
		w.a.V(1).M(chi).A().Error("UNABLE to write normalized CHI. err:%q", err)
		return nil
	}

	_ = w.deleteCHI(ctx, chi)

	// Uninstall finalizer
	w.a.V(2).M(chi).F().Info("uninstall finalizer")
	if err := w.c.uninstallFinalizer(chi); err != nil {
		w.a.V(1).M(chi).A().Error("unable to uninstall finalizer: err:%v", err)
	}

	return nil
}

// deleteCHI deletes all kubernetes resources related to chi *chop.ClickHouseInstallation
func (w *worker) deleteCHI(ctx context.Context, chi *chop.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(chi).S().P()
	defer w.a.V(2).M(chi).E().P()

	var err error

	w.a.V(1).
		WithEvent(chi, eventActionDelete, eventReasonDeleteStarted).
		WithStatusAction(chi).
		M(chi).F().
		Info("Delete CHI started")

	chi, err = w.normalizer.CreateTemplatedCHI(chi)
	if err != nil {
		w.a.WithEvent(chi, eventActionDelete, eventReasonDeleteFailed).
			WithStatusError(chi).
			M(chi).A().
			Error("Delete CHI failed - unable to normalize: %q", err)
		return err
	}

	// Exclude this CHI from monitoring
	w.c.deleteWatch(chi.Namespace, chi.Name)

	// Delete all clusters
	chi.WalkClusters(func(cluster *chop.ChiCluster) error {
		return w.deleteCluster(ctx, chi, cluster)
	})

	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	// Delete ConfigMap(s)
	err = w.c.deleteConfigMapsCHI(ctx, chi)

	// Delete Service
	err = w.c.deleteServiceCHI(ctx, chi)

	w.a.V(1).
		WithEvent(chi, eventActionDelete, eventReasonDeleteCompleted).
		WithStatusAction(chi).
		M(chi).F().
		Info("Delete CHI completed")

	return nil
}

// deleteTables
// chi is the new CHI in which there will be no more this tabled
func (w *worker) deleteTables(ctx context.Context, host *chop.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	if !host.CanDeleteAllPVCs() {
		return nil
	}
	err := w.schemer.HostDeleteTables(ctx, host)

	if err == nil {
		w.a.V(1).
			WithEvent(host.CHI, eventActionDelete, eventReasonDeleteCompleted).
			WithStatusAction(host.CHI).
			M(host).F().
			Info("Deleted tables on host %s replica %d to shard %d in cluster %s",
				host.Name, host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
	} else {
		w.a.WithEvent(host.CHI, eventActionDelete, eventReasonDeleteFailed).
			WithStatusError(host.CHI).
			M(host).A().
			Error("FAILED to delete tables on host %s with error %v", host.Name, err)
	}

	return err
}

// deleteHost deletes all kubernetes resources related to replica *chop.ChiHost
// chi is the new CHI in which there will be no more this host
func (w *worker) deleteHost(ctx context.Context, chi *chop.ClickHouseInstallation, host *chop.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(host).S().Info(host.Address.HostName)
	defer w.a.V(2).M(host).E().Info(host.Address.HostName)

	w.a.V(1).
		WithEvent(host.CHI, eventActionDelete, eventReasonDeleteStarted).
		WithStatusAction(host.CHI).
		M(host).F().
		Info("Delete host %s/%s - started", host.Address.ClusterName, host.Name)

	if _, err := w.c.getStatefulSet(host); err != nil {
		w.a.WithEvent(host.CHI, eventActionDelete, eventReasonDeleteCompleted).
			WithStatusAction(host.CHI).
			M(host).F().
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
	err = w.deleteTables(ctx, host)
	err = w.c.deleteHost(ctx, host)

	// When deleting the whole CHI (not particular host), CHI may already be unavailable, so update CHI tolerantly
	chi.Status.DeletedHostsCount++
	_ = w.c.updateCHIObjectStatus(ctx, chi, true)

	if err == nil {
		w.a.V(1).
			WithEvent(host.CHI, eventActionDelete, eventReasonDeleteCompleted).
			WithStatusAction(host.CHI).
			M(host).F().
			Info("Delete host %s/%s - completed", host.Address.ClusterName, host.Name)
	} else {
		w.a.WithEvent(host.CHI, eventActionDelete, eventReasonDeleteFailed).
			WithStatusError(host.CHI).
			M(host).F().
			Error("FAILED Delete host %s/%s - completed", host.Address.ClusterName, host.Name)
	}

	return err
}

// deleteShard deletes all kubernetes resources related to shard *chop.ChiShard
// chi is the new CHI in which there will be no more this shard
func (w *worker) deleteShard(ctx context.Context, chi *chop.ClickHouseInstallation, shard *chop.ChiShard) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(shard).S().P()
	defer w.a.V(2).M(shard).E().P()

	w.a.V(1).
		WithEvent(shard.CHI, eventActionDelete, eventReasonDeleteStarted).
		WithStatusAction(shard.CHI).
		M(shard).F().
		Info("Delete shard %s/%s - started", shard.Address.Namespace, shard.Name)

	// Delete all replicas
	shard.WalkHosts(func(host *chop.ChiHost) error {
		return w.deleteHost(ctx, chi, host)
	})

	// Delete Shard Service
	_ = w.c.deleteServiceShard(ctx, shard)

	w.a.V(1).
		WithEvent(shard.CHI, eventActionDelete, eventReasonDeleteCompleted).
		WithStatusAction(shard.CHI).
		M(shard).F().
		Info("Delete shard %s/%s - completed", shard.Address.Namespace, shard.Name)

	return nil
}

// deleteCluster deletes all kubernetes resources related to cluster *chop.ChiCluster
// chi is the new CHI in which there will be no more this cluster
func (w *worker) deleteCluster(ctx context.Context, chi *chop.ClickHouseInstallation, cluster *chop.ChiCluster) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(cluster).S().P()
	defer w.a.V(2).M(cluster).E().P()

	w.a.V(1).
		WithEvent(cluster.CHI, eventActionDelete, eventReasonDeleteStarted).
		WithStatusAction(cluster.CHI).
		M(cluster).F().
		Info("Delete cluster %s/%s - started", cluster.Address.Namespace, cluster.Name)

	// Delete all shards
	cluster.WalkShards(func(index int, shard *chop.ChiShard) error {
		return w.deleteShard(ctx, chi, shard)
	})

	// Delete Cluster Service
	_ = w.c.deleteServiceCluster(ctx, cluster)

	w.a.V(1).
		WithEvent(cluster.CHI, eventActionDelete, eventReasonDeleteCompleted).
		WithStatusAction(cluster.CHI).
		M(cluster).F().
		Info("Delete cluster %s/%s - completed", cluster.Address.Namespace, cluster.Name)

	return nil
}

// createCHIFromObjectMeta
func (w *worker) createCHIFromObjectMeta(objectMeta *meta.ObjectMeta) (*chop.ClickHouseInstallation, error) {
	w.a.V(3).M(objectMeta).S().P()
	defer w.a.V(3).M(objectMeta).E().P()

	chi, err := w.c.GetCHIByObjectMeta(objectMeta)
	if err != nil {
		return nil, err
	}

	chi, err = w.normalizer.CreateTemplatedCHI(chi)
	if err != nil {
		return nil, err
	}

	return chi, nil
}

// createClusterFromObjectMeta
func (w *worker) createClusterFromObjectMeta(objectMeta *meta.ObjectMeta) (*chop.ChiCluster, error) {
	w.a.V(3).M(objectMeta).S().P()
	defer w.a.V(3).M(objectMeta).E().P()

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
func (w *worker) updateConfigMap(ctx context.Context, chi *chop.ClickHouseInstallation, configMap *core.ConfigMap) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}
	_, err := w.c.kubeClient.CoreV1().ConfigMaps(configMap.Namespace).Update(configMap)

	if err == nil {
		w.a.V(1).
			WithEvent(chi, eventActionUpdate, eventReasonUpdateCompleted).
			WithStatusAction(chi).
			M(chi).F().
			Info("Update ConfigMap %s/%s", configMap.Namespace, configMap.Name)
	} else {
		w.a.WithEvent(chi, eventActionUpdate, eventReasonUpdateFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).A().
			Error("Update ConfigMap %s/%s failed with error %v", configMap.Namespace, configMap.Name, err)
	}

	return err
}

// createConfigMap
func (w *worker) createConfigMap(ctx context.Context, chi *chop.ClickHouseInstallation, configMap *core.ConfigMap) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	_, err := w.c.kubeClient.CoreV1().ConfigMaps(configMap.Namespace).Create(configMap)

	if err == nil {
		w.a.V(1).
			WithEvent(chi, eventActionCreate, eventReasonCreateCompleted).
			WithStatusAction(chi).
			M(chi).F().
			Info("Create ConfigMap %s/%s", configMap.Namespace, configMap.Name)
	} else {
		w.a.WithEvent(chi, eventActionCreate, eventReasonCreateFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).A().
			Error("Create ConfigMap %s/%s failed with error %v", configMap.Namespace, configMap.Name, err)
	}

	return err
}

// reconcileConfigMap reconciles core.ConfigMap which belongs to specified CHI
func (w *worker) reconcileConfigMap(
	ctx context.Context,
	chi *chop.ClickHouseInstallation,
	configMap *core.ConfigMap,
	update bool,
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(chi).S().P()
	defer w.a.V(2).M(chi).E().P()

	// Check whether this object already exists in k8s
	curConfigMap, err := w.c.getConfigMap(&configMap.ObjectMeta, false)

	if curConfigMap != nil {
		// We have ConfigMap - try to update it
		if !update {
			return nil
		}
		err = w.updateConfigMap(ctx, chi, configMap)
	}

	if apierrors.IsNotFound(err) {
		// ConfigMap not found - even during Update process - try to create it
		err = w.createConfigMap(ctx, chi, configMap)
	}

	if err != nil {
		w.a.WithEvent(chi, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).A().
			Error("FAILED to reconcile ConfigMap: %s CHI: %s ", configMap.Name, chi.Name)
	}

	return err
}

// updateService
func (w *worker) updateService(
	ctx context.Context,
	chi *chop.ClickHouseInstallation,
	curService *core.Service,
	newService *core.Service,
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	// Updating a Service is a complicated business

	// spec.resourceVersion is required in order to update object
	newService.ResourceVersion = curService.ResourceVersion

	// The port on each node on which this service is exposed when type=NodePort or LoadBalancer.
	// Usually assigned by the system. If specified, it will be allocated to the service
	// if unused or else creation of the service will fail.
	// Default is to auto-allocate a port if the ServiceType of this Service requires one.
	// More info: https://kubernetes.io/docs/concepts/services-networking/service/#type-nodeport
	if ((curService.Spec.Type == core.ServiceTypeNodePort) && (newService.Spec.Type == core.ServiceTypeNodePort)) ||
		((curService.Spec.Type == core.ServiceTypeLoadBalancer) && (newService.Spec.Type == core.ServiceTypeLoadBalancer)) {
		// No changes in service type and service type assumes NodePort to be allocated.
		// !!! IMPORTANT !!!
		// The same exposed port details can not be changed. This is important limitation
		for i := range newService.Spec.Ports {
			newPort := &newService.Spec.Ports[i]
			for j := range curService.Spec.Ports {
				curPort := &curService.Spec.Ports[j]
				if newPort.Port == curPort.Port {
					// Already have this port specified - reuse all internals,
					// due to limitations with auto-assigned values
					*newPort = *curPort
					w.a.M(chi).F().Info("reuse Port %d values", newPort.Port)
					break
				}
			}
		}
	}

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

	newService.ObjectMeta.Labels = util.MergeStringMapsPreserve(newService.ObjectMeta.Labels, curService.ObjectMeta.Labels)
	newService.ObjectMeta.Annotations = util.MergeStringMapsPreserve(newService.ObjectMeta.Annotations, curService.ObjectMeta.Annotations)
	newService.ObjectMeta.Finalizers = util.MergeStringArrays(newService.ObjectMeta.Finalizers, curService.ObjectMeta.Finalizers)

	// And only now we are ready to actually update the service with new version of the service
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}
	_, err := w.c.kubeClient.CoreV1().Services(newService.Namespace).Update(newService)

	if err == nil {
		w.a.V(1).
			WithEvent(chi, eventActionUpdate, eventReasonUpdateCompleted).
			WithStatusAction(chi).
			M(chi).F().
			Info("Update Service %s/%s", newService.Namespace, newService.Name)
	} else {
		w.a.WithEvent(chi, eventActionUpdate, eventReasonUpdateFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).A().
			Error("Update Service %s/%s failed with error %v", newService.Namespace, newService.Name, err)
	}

	return err
}

// createService
func (w *worker) createService(ctx context.Context, chi *chop.ClickHouseInstallation, service *core.Service) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	_, err := w.c.kubeClient.CoreV1().Services(service.Namespace).Create(service)

	if err == nil {
		w.a.V(1).
			WithEvent(chi, eventActionCreate, eventReasonCreateCompleted).
			WithStatusAction(chi).
			M(chi).F().
			Info("Create Service %s/%s", service.Namespace, service.Name)
	} else {
		w.a.WithEvent(chi, eventActionCreate, eventReasonCreateFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).A().
			Error("Create Service %s/%s failed with error %v", service.Namespace, service.Name, err)
	}

	return err
}

// reconcileService reconciles core.Service
func (w *worker) reconcileService(ctx context.Context, chi *chop.ClickHouseInstallation, service *core.Service) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(chi).S().Info(service.Name)
	defer w.a.V(2).M(chi).E().Info(service.Name)

	// Check whether this object already exists
	curService, err := w.c.getService(&service.ObjectMeta, false)

	if curService != nil {
		// We have Service - try to update it
		err = w.updateService(ctx, chi, curService, service)
	}

	if err != nil {
		// Service not found or not updated. Try to recreate
		_ = w.c.deleteServiceIfExists(ctx, service.Namespace, service.Name)
		err = w.createService(ctx, chi, service)
	}

	if err != nil {
		w.a.WithEvent(chi, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusAction(chi).
			WithStatusError(chi).
			M(chi).A().
			Error("FAILED to reconcile Service: %s CHI: %s ", service.Name, chi.Name)
	}

	return err
}

func (w *worker) getStatefulSetStatus(statefulSet *apps.StatefulSet, host *chop.ChiHost) chop.StatefulSetStatus {
	w.a.V(2).M(host).S().Info(util.NamespaceNameString(statefulSet.ObjectMeta))
	defer w.a.V(2).M(host).E().Info(util.NamespaceNameString(statefulSet.ObjectMeta))

	// Check whether this object already exists in k8s
	curStatefulSet, err := w.c.getStatefulSet(&statefulSet.ObjectMeta, false)

	if curStatefulSet != nil {
		// Try to perform label-based comparison
		curLabel, curHasLabel := w.creator.GetStatefulSetVersion(curStatefulSet)
		newLabel, newHasLabel := w.creator.GetStatefulSetVersion(statefulSet)
		if curHasLabel && newHasLabel {
			if curLabel == newLabel {
				w.a.M(host).F().Info("INFO StatefulSet ARE EQUAL based on labels no reconcile is actually needed %s", util.NamespaceNameString(statefulSet.ObjectMeta))
				return chop.StatefulSetStatusSame
			} else {
				//if diff, equal := messagediff.DeepDiff(curStatefulSet.Spec, statefulSet.Spec); equal {
				//	w.a.Info("INFO StatefulSet ARE EQUAL based on diff no reconcile is actually needed")
				//	//					return chop.StatefulSetStatusSame
				//} else {
				//	w.a.Info("INFO StatefulSet ARE DIFFERENT based on diff reconcile is required: a:%v m:%v r:%v", diff.Added, diff.Modified, diff.Removed)
				//	//					return chop.StatefulSetStatusModified
				//}
				w.a.M(host).F().Info("INFO StatefulSet ARE DIFFERENT based on labels reconcile needed %s", util.NamespaceNameString(statefulSet.ObjectMeta))
				return chop.StatefulSetStatusModified
			}
		}
		// No labels to compare, we can not say for sure what exactly is going on
		return chop.StatefulSetStatusUnknown
	}

	// No cur StatefulSet available

	if apierrors.IsNotFound(err) {
		return chop.StatefulSetStatusNew
	}

	return chop.StatefulSetStatusUnknown
}

// reconcileStatefulSet reconciles apps.StatefulSet
func (w *worker) reconcileStatefulSet(ctx context.Context, newStatefulSet *apps.StatefulSet, host *chop.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(host).S().Info(util.NamespaceNameString(newStatefulSet.ObjectMeta))
	defer w.a.V(2).M(host).E().Info(util.NamespaceNameString(newStatefulSet.ObjectMeta))

	if host.ReconcileAttributes.GetStatus() == chop.StatefulSetStatusSame {
		defer w.a.V(2).M(host).F().Info("no need to reconcile the same StatefulSet %s", util.NamespaceNameString(newStatefulSet.ObjectMeta))
		return nil
	}

	// Check whether this object already exists in k8s
	curStatefulSet, err := w.c.getStatefulSet(&newStatefulSet.ObjectMeta, false)

	if curStatefulSet != nil {
		// We have StatefulSet - try to update it
		err = w.updateStatefulSet(ctx, curStatefulSet, newStatefulSet, host)
	}

	if apierrors.IsNotFound(err) {
		// StatefulSet not found - even during Update process - try to create it
		err = w.createStatefulSet(ctx, newStatefulSet, host)
	}

	if err != nil {
		w.a.WithEvent(host.CHI, eventActionReconcile, eventReasonReconcileFailed).
			WithStatusAction(host.CHI).
			WithStatusError(host.CHI).
			M(host).A().
			Error("FAILED to reconcile StatefulSet: %s CHI: %s ", newStatefulSet.Name, host.CHI.Name)
	}

	return err
}

// createStatefulSet
func (w *worker) createStatefulSet(ctx context.Context, statefulSet *apps.StatefulSet, host *chop.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(host).S().Info(util.NamespaceNameString(statefulSet.ObjectMeta))
	defer w.a.V(2).M(host).E().Info(util.NamespaceNameString(statefulSet.ObjectMeta))

	w.a.V(1).
		WithEvent(host.CHI, eventActionCreate, eventReasonCreateStarted).
		WithStatusAction(host.CHI).
		M(host).F().
		Info("Create StatefulSet %s/%s - started", statefulSet.Namespace, statefulSet.Name)

	err := w.c.createStatefulSet(ctx, statefulSet, host)

	host.CHI.Status.AddedHostsCount++
	_ = w.c.updateCHIObjectStatus(ctx, host.CHI, false)

	if err == nil {
		w.a.V(1).
			WithEvent(host.CHI, eventActionCreate, eventReasonCreateCompleted).
			WithStatusAction(host.CHI).
			M(host).F().
			Info("Create StatefulSet %s/%s - completed", statefulSet.Namespace, statefulSet.Name)
	} else {
		w.a.WithEvent(host.CHI, eventActionCreate, eventReasonCreateFailed).
			WithStatusAction(host.CHI).
			WithStatusError(host.CHI).
			M(host).A().
			Error("Create StatefulSet %s/%s - failed with error %v", statefulSet.Namespace, statefulSet.Name, err)
	}

	return err
}

// updateStatefulSet
func (w *worker) updateStatefulSet(ctx context.Context, curStatefulSet, newStatefulSet *apps.StatefulSet, host *chop.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	w.a.V(2).M(host).S().Info(newStatefulSet.Name)
	defer w.a.V(2).M(host).E().Info(newStatefulSet.Name)

	namespace := newStatefulSet.Namespace
	name := newStatefulSet.Name

	w.a.V(1).
		WithEvent(host.CHI, eventActionCreate, eventReasonCreateStarted).
		WithStatusAction(host.CHI).
		M(host).F().
		Info("Update StatefulSet(%s/%s) - started", namespace, name)

	err := w.c.updateStatefulSet(ctx, curStatefulSet, newStatefulSet, host)
	if err == nil {
		host.CHI.Status.UpdatedHostsCount++
		_ = w.c.updateCHIObjectStatus(ctx, host.CHI, false)
		w.a.V(1).
			WithEvent(host.CHI, eventActionUpdate, eventReasonUpdateCompleted).
			WithStatusAction(host.CHI).
			M(host).F().
			Info("Update StatefulSet(%s/%s) - completed", namespace, name)
		return nil
	}

	w.a.WithEvent(host.CHI, eventActionUpdate, eventReasonUpdateFailed).
		WithStatusAction(host.CHI).
		WithStatusError(host.CHI).
		M(host).A().
		Error("Update StatefulSet(%s/%s) - failed with error\n---\n%v\n--\nContinue with recreate", namespace, name, err)

	diff, equal := messagediff.DeepDiff(curStatefulSet.Spec, newStatefulSet.Spec)
	w.a.M(host).Info("StatefulSet.Spec diff:")
	w.a.M(host).Info(util.MessageDiffString(diff, equal))

	err = w.c.deleteStatefulSet(ctx, host)
	err = w.reconcilePersistentVolumeClaims(ctx, host)
	return w.createStatefulSet(ctx, newStatefulSet, host)
}

// reconcilePersistentVolumes
func (w *worker) reconcilePersistentVolumes(ctx context.Context, host *chop.ChiHost) {
	if util.IsContextDone(ctx) {
		return
	}

	w.c.walkPVs(host, func(pv *core.PersistentVolume) {
		pv = w.creator.PreparePersistentVolume(pv, host)
		_ = w.c.updatePersistentVolume(ctx, pv)
	})
}

// reconcilePersistentVolumeClaims
func (w *worker) reconcilePersistentVolumeClaims(ctx context.Context, host *chop.ChiHost) error {
	if util.IsContextDone(ctx) {
		return nil
	}

	namespace := host.Address.Namespace
	w.a.V(2).M(host).S().Info("host %s/%s", namespace, host.Name)
	defer w.a.V(2).M(host).E().Info("host %s/%s", namespace, host.Name)

	host.WalkVolumeMounts(func(volumeMount *core.VolumeMount) {
		if util.IsContextDone(ctx) {
			return
		}

		volumeClaimTemplateName := volumeMount.Name
		volumeClaimTemplate, ok := host.CHI.GetVolumeClaimTemplate(volumeClaimTemplateName)
		if !ok {
			// No this is not a reference to VolumeClaimTemplate
			return
		}

		pvcName := chopmodel.CreatePVCName(host, volumeMount, volumeClaimTemplate)
		w.a.V(2).M(host).Info("reconcile volumeMount (%s/%s/%s/%s) - start", namespace, host.Name, volumeMount.Name, pvcName)
		defer w.a.V(2).M(host).Info("reconcile volumeMount (%s/%s/%s/%s) - end", namespace, host.Name, volumeMount.Name, pvcName)

		pvc, err := w.c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(pvcName, newGetOptions())
		if err != nil {
			if apierrors.IsNotFound(err) {
				// This is not an error per se, means PVC is not created (yet)?
			} else {
				w.a.M(host).A().Error("ERROR unable to get PVC(%s/%s) err: %v", namespace, pvcName, err)
			}
			return
		}
		w.reconcileResources(ctx, pvc, volumeClaimTemplate)
	})

	return nil
}

// reconcileResources
func (w *worker) reconcileResources(ctx context.Context, pvc *core.PersistentVolumeClaim, template *chop.ChiVolumeClaimTemplate) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return
	}
	w.reconcileResourcesList(ctx, pvc, pvc.Spec.Resources.Requests, template.Spec.Resources.Requests)
}

// reconcileResourcesList
func (w *worker) reconcileResourcesList(
	ctx context.Context,
	pvc *core.PersistentVolumeClaim,
	pvcResourceList core.ResourceList,
	desiredResourceList core.ResourceList,
) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return
	}

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
		w.reconcileResource(ctx, pvc, pvcResourceList, desiredResourceList, resourceName.(core.ResourceName))
	}
}

// reconcileResources
func (w *worker) reconcileResource(
	ctx context.Context,
	pvc *core.PersistentVolumeClaim,
	pvcResourceList core.ResourceList,
	desiredResourceList core.ResourceList,
	resourceName core.ResourceName,
) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return
	}

	w.a.V(2).M(pvc).F().S().Info("%s/%s/%s", pvc.Namespace, pvc.Name, resourceName)
	defer w.a.V(2).M(pvc).F().E().Info("%s/%s/%s", pvc.Namespace, pvc.Name, resourceName)

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

	w.a.V(2).M(pvc).F().Info("%s/%s/%s - unequal requests, want to update", pvc.Namespace, pvc.Name, resourceName)
	pvcResourceList[resourceName] = desiredResourceList[resourceName]
	_, err := w.c.kubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(pvc)
	if err != nil {
		w.a.M(pvc).A().Error("unable to reconcile resource %s/%s/%s err: %v", pvc.Namespace, pvc.Name, resourceName, err)
		return
	}
}
