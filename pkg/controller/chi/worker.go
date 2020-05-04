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

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/util/workqueue"

	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopmodel "github.com/altinity/clickhouse-operator/pkg/model"
)

type worker struct {
	c          *Controller
	a          Announcer
	queue      workqueue.RateLimitingInterface
	normalizer *chopmodel.Normalizer
	schemer    *chopmodel.Schemer
	creator    *chopmodel.Creator
}

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

// run is an endless work loop, expected to be runin a thread
func (w *worker) run() {
	for {
		// Get() blocks until it can return an item
		item, shutdown := w.queue.Get()
		if shutdown {
			w.a.Info("runWorker(): shutdown request")
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

func (w *worker) normalize(chi *chop.ClickHouseInstallation) *chop.ClickHouseInstallation {
	if chi == nil {
		chi, _ = w.normalizer.CreateTemplatedCHI(&chop.ClickHouseInstallation{}, false)
	} else {
		chi, _ = w.normalizer.CreateTemplatedCHI(chi, true)
	}

	return chi
}

// updateCHI sync CHI which was already created earlier
func (w *worker) updateCHI(old, new *chop.ClickHouseInstallation) error {

	if (old != nil) && (new != nil) && (old.ObjectMeta.ResourceVersion == new.ObjectMeta.ResourceVersion) {
		w.a.V(2).Info("updateCHI(%s/%s): ResourceVersion did not change: %s", new.Namespace, new.Name, new.ObjectMeta.ResourceVersion)
		// No need to react
		return nil
	}

	old = w.normalize(old)
	new = w.normalize(new)

	actionPlan := NewActionPlan(old, new)

	if !actionPlan.HasActionsToDo() {
		// Nothing to do - no changes found - no need to react
		w.a.V(2).Info("updateCHI(%s/%s) - no changes found", new.Namespace, new.Name)
		return nil
	}

	// Write desired normalized CHI with initialized .Status, so it would be possible to monitor progress
	new.Status.Status = chop.StatusInProgress
	new.Status.UpdatedHostsCount = 0
	new.Status.AddedHostsCount = 0
	new.Status.DeletedHostsCount = 0
	new.Status.DeleteHostsCount = actionPlan.GetRemovedHostsNum()
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
		w.a.WithEvent(new, eventActionReconcile, eventReasonReconcileFailed).WithStatusError(new).Error("FAILED update: %v", err)
		return nil
	}

	// Post-process added items
	actionPlan.WalkAdded(
		func(cluster *chop.ChiCluster) {
			w.a.V(1).WithEvent(new, eventActionCreate, eventReasonCreateCompleted).
				WithStatusAction(new).
				Info("Added cluster %s", cluster.Name)
		},
		func(shard *chop.ChiShard) {
			w.a.V(1).WithEvent(new, eventActionCreate, eventReasonCreateCompleted).
				WithStatusAction(new).
				Info("Added shard %d to cluster %s", shard.Address.ShardIndex, shard.Address.ClusterName)
		},
		func(host *chop.ChiHost) {
			w.a.V(1).WithEvent(new, eventActionCreate, eventReasonCreateCompleted).
				WithStatusAction(new).
				Info("Added replica %d to shard %d in cluster %s",
					host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)

			if err := w.schemer.HostCreateTables(host); err != nil {
				w.a.WithEvent(new, eventActionUpdate, eventReasonUpdateFailed).WithStatusError(new).
					Error("FAILED to create tables on host %s with error %v", host.Name, err)
			}
		},
	)

	// Remove deleted items
	actionPlan.WalkRemoved(
		func(cluster *chop.ChiCluster) {
			w.a.V(1).WithEvent(new, eventActionDelete, eventReasonDeleteStarted).
				WithStatusAction(new).
				Info("delete cluster %s started", cluster.Name)
			if err := w.deleteCluster(cluster); err != nil {
				w.a.WithEvent(new, eventActionDelete, eventReasonDeleteFailed).WithStatusError(new).
					Error("FAILED to delete cluster %s with error %v", cluster.Name, err)
			} else {
				w.a.V(1).WithEvent(new, eventActionDelete, eventReasonDeleteCompleted).
					WithStatusAction(new).
					Info("delete cluster %s completed", cluster.Name)
			}
		},
		func(shard *chop.ChiShard) {
			w.a.V(1).WithEvent(new, eventActionDelete, eventReasonDeleteStarted).
				WithStatusAction(new).
				Info("delete shard %d in cluster %s started", shard.Address.ShardIndex, shard.Address.ClusterName)
			if err := w.deleteShard(shard); err != nil {
				w.a.WithEvent(new, eventActionDelete, eventReasonDeleteFailed).WithStatusError(new).
					Error("FAILED to delete shard %d in cluster %s with error %v",
						shard.Address.ShardIndex, shard.Address.ClusterName, err)
			} else {
				w.a.V(1).WithEvent(new, eventActionDelete, eventReasonDeleteCompleted).
					WithStatusAction(new).
					Info("delete shard %d in cluster %s completed", shard.Address.ShardIndex, shard.Address.ClusterName)
			}
		},
		func(host *chop.ChiHost) {
			w.a.V(1).WithEvent(new, eventActionDelete, eventReasonDeleteStarted).
				WithStatusAction(new).
				Info("delete replica %d from shard %d in cluster %s started",
					host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
			if err := w.deleteHost(host); err != nil {
				w.a.WithEvent(new, eventActionDelete, eventReasonDeleteFailed).WithStatusError(new).
					Error("FAILED to delete replica %d from shard %d in cluster %s with error %v",
						host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName, err)
			} else {
				w.a.V(1).WithEvent(new, eventActionDelete, eventReasonDeleteCompleted).
					WithStatusAction(new).
					Info("delete replica %d from shard %d in cluster %s completed",
						host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
			}
		},
	)

	// Update CHI object
	new.Status.Status = chop.StatusCompleted
	_ = w.c.updateCHIObjectStatus(new, false)

	w.c.updateWatch(new.Namespace, new.Name, chopmodel.CreatePodFQDNsOfChi(new))

	w.a.V(1).
		WithEvent(new, eventActionReconcile, eventReasonReconcileCompleted).
		WithStatusAction(new).
		Info("updateCHI(%s/%s) reconcile completed", new.Namespace, new.Name)

	return nil
}

// reconcile reconciles ClickHouseInstallation
func (w *worker) reconcile(chi *chop.ClickHouseInstallation) error {
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
	// 1. CHI Service
	service := w.creator.CreateServiceCHI()
	if err := w.ReconcileService(chi, service); err != nil {
		return err
	}

	// 2. CHI ConfigMaps

	// ConfigMap common for all resources in CHI
	// contains several sections, mapped as separated chopConfig files,
	// such as remote servers, zookeeper setup, etc
	configMapCommon := w.creator.CreateConfigMapCHICommon()
	if err := w.ReconcileConfigMap(chi, configMapCommon); err != nil {
		return err
	}

	// ConfigMap common for all users resources in CHI
	configMapUsers := w.creator.CreateConfigMapCHICommonUsers()
	if err := w.ReconcileConfigMap(chi, configMapUsers); err != nil {
		return err
	}

	// Add here other CHI components to be reconciled

	return nil
}

// reconcileCluster reconciles Cluster, excluding nested shards
func (w *worker) reconcileCluster(cluster *chop.ChiCluster) error {
	// Add Cluster's Service
	if service := w.creator.CreateServiceCluster(cluster); service != nil {
		return w.ReconcileService(cluster.CHI, service)
	} else {
		return nil
	}
}

// reconcileShard reconciles Shard, excluding nested replicas
func (w *worker) reconcileShard(shard *chop.ChiShard) error {
	// Add Shard's Service
	if service := w.creator.CreateServiceShard(shard); service != nil {
		return w.ReconcileService(shard.CHI, service)
	} else {
		return nil
	}
}

// reconcileHost reconciles ClickHouse host
func (w *worker) reconcileHost(host *chop.ChiHost) error {

	// Add host's ConfigMap
	configMap := w.creator.CreateConfigMapHost(host)
	if err := w.ReconcileConfigMap(host.CHI, configMap); err != nil {
		return err
	}

	// Add host's StatefulSet
	statefulSet := w.creator.CreateStatefulSet(host)
	if err := w.ReconcileStatefulSet(statefulSet, host); err != nil {
		return err
	}

	// Add host's Service
	service := w.creator.CreateServiceHost(host)
	if err := w.ReconcileService(host.CHI, service); err != nil {
		return err
	}

	return nil
}

// deleteCHI deletes all kubernetes resources related to chi *chop.ClickHouseInstallation
func (w *worker) deleteCHI(chi *chop.ClickHouseInstallation) error {
	var err error

	w.a.V(1).
		WithEvent(chi, eventActionDelete, eventReasonDeleteStarted).
		WithStatusAction(chi).
		Info("Start delete CHI %s/%s", chi.Namespace, chi.Name)

	chi, err = w.normalizer.CreateTemplatedCHI(chi, true)
	if err != nil {
		w.a.WithEvent(chi, eventActionDelete, eventReasonDeleteFailed).
			WithStatusError(chi).
			Error("ClickHouseInstallation (%q): unable to normalize: %q", chi.Name, err)
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
		Info("End delete CHI %s/%s", chi.Namespace, chi.Name)

	// Exclude this CHI from monitoring
	w.c.deleteWatch(chi.Namespace, chi.Name)

	return nil
}

// deleteHost deletes all kubernetes resources related to replica *chop.ChiHost
func (w *worker) deleteHost(host *chop.ChiHost) error {
	w.a.V(1).
		WithEvent(host.CHI, eventActionDelete, eventReasonDeleteStarted).
		WithStatusAction(host.CHI).
		Info("Worker delete host %s/%s", host.Address.ClusterName, host.Name)

	if _, err := w.c.getStatefulSetByHost(host); err != nil {
		w.a.WithEvent(host.CHI, eventActionDelete, eventReasonDeleteFailed).
			WithStatusAction(host.CHI).
			Error("Worker delete host %s/%s - StatefulSet not found - already deleted? %v",
				host.Address.ClusterName, host.Name, err)
		return nil
	}

	w.a.V(1).
		WithEvent(host.CHI, eventActionDelete, eventReasonDeleteInProgress).
		Info("Worker delete host %s/%s - StatefulSet found - start delete process", host.Address.ClusterName, host.Name)

	// Each host consists of
	// 1. Tables on host - we need to delete tables on the host in order to clean Zookeeper data
	// 2. StatefulSet
	// 3. PersistentVolumeClaim
	// 4. ConfigMap
	// 5. Service
	// Need to delete all these item

	if host.CanDeleteAllPVCs() {
		_ = w.schemer.HostDeleteTables(host)
	}

	return w.c.deleteHost(host)
}

// deleteShard deletes all kubernetes resources related to shard *chop.ChiShard
func (w *worker) deleteShard(shard *chop.ChiShard) error {
	w.a.V(1).
		WithEvent(shard.CHI, eventActionDelete, eventReasonDeleteStarted).
		WithStatusAction(shard.CHI).
		Info("Start delete shard %s/%s", shard.Address.Namespace, shard.Name)

	// Delete all replicas
	shard.WalkHosts(w.deleteHost)

	// Delete Shard Service
	_ = w.c.deleteServiceShard(shard)
	w.a.V(1).
		WithEvent(shard.CHI, eventActionDelete, eventReasonDeleteCompleted).
		WithStatusAction(shard.CHI).
		Info("End delete shard %s/%s", shard.Address.Namespace, shard.Name)

	return nil
}

// deleteCluster deletes all kubernetes resources related to cluster *chop.ChiCluster
func (w *worker) deleteCluster(cluster *chop.ChiCluster) error {
	w.a.V(1).
		WithEvent(cluster.CHI, eventActionDelete, eventReasonDeleteStarted).
		WithStatusAction(cluster.CHI).
		Info("Start delete cluster %s/%s", cluster.Address.Namespace, cluster.Name)

	// Delete all shards
	cluster.WalkShards(func(index int, shard *chop.ChiShard) error {
		return w.deleteShard(shard)
	})

	// Delete Cluster Service
	_ = w.c.deleteServiceCluster(cluster)
	w.a.V(1).
		WithEvent(cluster.CHI, eventActionDelete, eventReasonDeleteCompleted).
		WithStatusAction(cluster.CHI).
		Info("End delete cluster %s/%s", cluster.Address.Namespace, cluster.Name)

	return nil
}

func (w *worker) createCHIFromObjectMeta(objectMeta *meta.ObjectMeta) (*chop.ClickHouseInstallation, error) {
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

func (w *worker) createClusterFromObjectMeta(objectMeta *meta.ObjectMeta) (*chop.ChiCluster, error) {
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

// ReconcileConfigMap reconciles core.ConfigMap which belongs to specified CHI
func (w *worker) ReconcileConfigMap(chi *chop.ClickHouseInstallation, configMap *core.ConfigMap) error {
	// Check whether this object already exists in k8s
	curConfigMap, err := w.c.getConfigMap(&configMap.ObjectMeta, false)

	if curConfigMap != nil {
		// Object found - update it
		_, err := w.c.kubeClient.CoreV1().ConfigMaps(configMap.Namespace).Update(configMap)

		w.a.V(1).
			WithEvent(chi, eventActionUpdate, eventReasonUpdateCompleted).
			WithStatusAction(chi).
			Info("Update ConfigMap %s/%s", configMap.Namespace, configMap.Name)

		return err
	}

	if apierrors.IsNotFound(err) {
		// Object not found - create it
		_, err := w.c.kubeClient.CoreV1().ConfigMaps(configMap.Namespace).Create(configMap)

		w.a.V(1).
			WithEvent(chi, eventActionCreate, eventReasonCreateCompleted).
			WithStatusAction(chi).
			Info("Create ConfigMap %s/%s", configMap.Namespace, configMap.Name)

		return err
	}

	return err
}

// ReconcileService reconciles core.Service
func (w *worker) ReconcileService(chi *chop.ClickHouseInstallation, service *core.Service) error {
	// Check whether this object already exists in k8s
	curService, err := w.c.getService(&service.ObjectMeta, false)

	if curService != nil {
		// Object found - update it

		// Updating Service is a complicated business

		// spec.resourceVersion is required in order to update object
		service.ResourceVersion = curService.ResourceVersion

		// spec.clusterIP field is immutable, need to use already assigned value
		// From https://kubernetes.io/docs/concepts/services-networking/service/#defining-a-service
		// Kubernetes assigns this Service an IP address (sometimes called the “cluster IP”), which is used by the Service proxies
		// See also https://kubernetes.io/docs/concepts/services-networking/service/#virtual-ips-and-service-proxies
		// You can specify your own cluster IP address as part of a Service creation request. To do this, set the .spec.clusterIP
		service.Spec.ClusterIP = curService.Spec.ClusterIP

		// spec.healthCheckNodePort field is used with ExternalTrafficPolicy=Local only and is immutable within ExternalTrafficPolicy=Local
		// In case ExternalTrafficPolicy is changed it seems to be irrelevant
		// https://kubernetes.io/docs/tasks/access-application-cluster/create-external-load-balancer/#preserving-the-client-source-ip
		if (curService.Spec.ExternalTrafficPolicy == core.ServiceExternalTrafficPolicyTypeLocal) &&
			(service.Spec.ExternalTrafficPolicy == core.ServiceExternalTrafficPolicyTypeLocal) {
			service.Spec.HealthCheckNodePort = curService.Spec.HealthCheckNodePort
		}

		// And only now we are ready to actually update the service with new version of the service
		_, err := w.c.kubeClient.CoreV1().Services(service.Namespace).Update(service)

		w.a.V(1).
			WithEvent(chi, eventActionUpdate, eventReasonUpdateCompleted).
			WithStatusAction(chi).
			Info("Update Service %s/%s", service.Namespace, service.Name)

		return err
	}

	if apierrors.IsNotFound(err) {
		// Object not found - create it
		_, err := w.c.kubeClient.CoreV1().Services(service.Namespace).Create(service)

		w.a.V(1).
			WithEvent(chi, eventActionCreate, eventReasonCreateCompleted).
			WithStatusAction(chi).
			Info("Create Service %s/%s", service.Namespace, service.Name)

		return err
	}

	return err
}

// ReconcileStatefulSet reconciles apps.StatefulSet
func (w *worker) ReconcileStatefulSet(newStatefulSet *apps.StatefulSet, host *chop.ChiHost) error {
	// Check whether this object already exists in k8s
	curStatefulSet, err := w.c.getStatefulSet(&newStatefulSet.ObjectMeta, false)

	if curStatefulSet != nil {
		// Object found - update it
		err := w.c.updateStatefulSet(curStatefulSet, newStatefulSet)

		host.CHI.Status.UpdatedHostsCount++
		_ = w.c.updateCHIObjectStatus(host.CHI, false)

		w.a.V(1).
			WithEvent(host.CHI, eventActionUpdate, eventReasonUpdateCompleted).
			WithStatusAction(host.CHI).
			Info("Update StatefulSet %s/%s", newStatefulSet.Namespace, newStatefulSet.Name)

		return err
	}

	if apierrors.IsNotFound(err) {
		// Object not found - create it
		err := w.c.createStatefulSet(newStatefulSet, host)

		host.CHI.Status.AddedHostsCount++
		_ = w.c.updateCHIObjectStatus(host.CHI, false)

		w.a.V(1).
			WithEvent(host.CHI, eventActionCreate, eventReasonCreateCompleted).
			WithStatusAction(host.CHI).
			Info("Create StatefulSet %s/%s", newStatefulSet.Namespace, newStatefulSet.Name)

		return err
	}

	return err
}
