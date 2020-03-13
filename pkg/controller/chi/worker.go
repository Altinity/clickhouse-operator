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
	log "github.com/golang/glog"
	// log "k8s.io/klog"

	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopmodels "github.com/altinity/clickhouse-operator/pkg/model"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

type worker struct {
	c          *Controller
	normalizer *chopmodels.Normalizer
	schemer    *chopmodels.Schemer
	creator    *chopmodels.Creator
}

func (c *Controller) newWorker() *worker {
	return &worker{
		c:          c,
		normalizer: chopmodels.NewNormalizer(c.chop),
		schemer: chopmodels.NewSchemer(
			c.chop.Config().CHUsername,
			c.chop.Config().CHPassword,
			c.chop.Config().CHPort,
		),
		creator: nil,
	}
}

// processWorkItem processes one work item according to its type
func (w *worker) processItem(item interface{}) error {
	switch item.(type) {

	case *ReconcileChi:
		reconcile, _ := item.(*ReconcileChi)
		switch reconcile.cmd {
		case reconcileAdd:
			return w.addCHI(reconcile.new)
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
			log.V(1).Infof("endpointsInformer UpdateFunc(%s/%s) flushing DNS for CHI %s", drop.initiator.Namespace, drop.initiator.Name, chi.Name)
			_ = w.schemer.ChiDropDnsCache(chi)
		} else {
			log.V(1).Infof("endpointsInformer UpdateFunc(%s/%s) unable to find CHI by %v", drop.initiator.Namespace, drop.initiator.Name, drop.initiator.Labels)
		}
		return nil
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilruntime.HandleError(fmt.Errorf("unexpected item in the queue - %#v", item))
	return nil
}

// addCHI normalize CHI - updates CHI object to normalized
func (w *worker) addCHI(new *chop.ClickHouseInstallation) error {
	// CHI is a new one - need to create normalized CHI
	// Operator receives CHI struct partially filled by data from .yaml file provided by user
	// We need to create full normalized specification
	log.V(1).Infof("addCHI(%s/%s)", new.Namespace, new.Name)
	w.c.eventCHI(new, eventTypeNormal, eventActionCreate, eventReasonCreateCompleted, fmt.Sprintf("ClickHouseInstallation (%s): start add process", new.Name))

	return w.updateCHI(nil, new)
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
		log.V(2).Infof("updateCHI(%s/%s): ResourceVersion did not change: %s", new.Namespace, new.Name, new.ObjectMeta.ResourceVersion)
		// No need to react
		return nil
	}

	old = w.normalize(old)
	new = w.normalize(new)

	actionPlan := NewActionPlan(old, new)

	if !actionPlan.HasActionsToDo() {
		// Nothing to do - no changes found - no need to react
		log.V(2).Infof("updateCHI(%s/%s) - no changes found", new.Namespace, new.Name)
		return nil
	}

	log.V(1).Infof("updateCHI(%s/%s) - start reconcile with action plan >>>\n%s",
		new.Namespace, new.Name, actionPlan.String(),
	)

	// Write desired normalized CHI with initialized .Status, so it would be possible to monitor progress
	new.Status.Status = chop.StatusInProgress
	new.Status.UpdatedHostsCount = 0
	new.Status.AddedHostsCount = 0
	new.Status.DeletedHostsCount = 0
	new.Status.DeleteHostsCount = actionPlan.GetRemovedHostsNum()
	if err := w.c.updateCHIObjectStatus(new, false); err != nil {
		log.V(1).Infof("UNABLE to write normalized CHI (%s/%s). It can trigger update action again. Error: %q", new.Namespace, new.Name, err)
		return nil
	}

	if err := w.reconcile(new); err != nil {
		str := fmt.Sprintf("FAILED update: %v", err)
		log.V(1).Info(str)
		w.c.eventCHI(new, eventTypeError, eventActionUpdate, eventReasonUpdateFailed, str)
		return nil
	}

	// Post-process added items
	actionPlan.WalkAdded(
		func(cluster *chop.ChiCluster) {
			str := fmt.Sprintf("Added cluster %s", cluster.Name)
			log.V(1).Info(str)
			w.c.eventCHI(new, eventTypeNormal, eventActionUpdate, eventReasonUpdateInProgress, str)
		},
		func(shard *chop.ChiShard) {
			str := fmt.Sprintf("Added shard %d to cluster %s", shard.Address.ShardIndex, shard.Address.ClusterName)
			log.V(1).Info(str)
			w.c.eventCHI(new, eventTypeNormal, eventActionUpdate, eventReasonUpdateInProgress, str)

			_ = w.createTablesOnShard(new, shard)
		},
		func(host *chop.ChiHost) {
			str := fmt.Sprintf("Added replica %d to shard %d in cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
			log.V(1).Info(str)
			w.c.eventCHI(new, eventTypeNormal, eventActionUpdate, eventReasonUpdateInProgress, str)

			_ = w.createTablesOnHost(new, host)
		},
	)

	// Remove deleted items
	actionPlan.WalkRemoved(
		func(cluster *chop.ChiCluster) {
			w.c.eventCHI(old, eventTypeNormal, eventActionUpdate, eventReasonUpdateInProgress, fmt.Sprintf("delete cluster %s", cluster.Name))
			_ = w.deleteCluster(cluster)
		},
		func(shard *chop.ChiShard) {
			w.c.eventCHI(old, eventTypeNormal, eventActionUpdate, eventReasonUpdateInProgress, fmt.Sprintf("delete shard %d in cluster %s", shard.Address.ShardIndex, shard.Address.ClusterName))
			_ = w.deleteShard(shard)
		},
		func(host *chop.ChiHost) {
			w.c.eventCHI(old, eventTypeNormal, eventActionUpdate, eventReasonUpdateInProgress, fmt.Sprintf("delete replica %d from shard %d in cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName))
			_ = w.deleteHost(host)
		},
	)

	// Update CHI object
	new.Status.Status = chop.StatusCompleted
	_ = w.c.updateCHIObjectStatus(new, false)

	w.c.updateWatch(new.Namespace, new.Name, chopmodels.CreatePodFQDNsOfChi(new))

	log.V(1).Infof("updateCHI(%s/%s) - complete reconcile <<<", new.Namespace, new.Name)

	return nil
}

// reconcile reconciles ClickHouseInstallation
func (w *worker) reconcile(chi *chop.ClickHouseInstallation) error {
	w.creator = chopmodels.NewCreator(w.c.chop, chi)
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
	if err := w.c.ReconcileService(service); err != nil {
		return err
	}

	// 2. CHI ConfigMaps

	// ConfigMap common for all resources in CHI
	// contains several sections, mapped as separated chopConfig files,
	// such as remote servers, zookeeper setup, etc
	configMapCommon := w.creator.CreateConfigMapCHICommon()
	if err := w.c.ReconcileConfigMap(configMapCommon); err != nil {
		return err
	}

	// ConfigMap common for all users resources in CHI
	configMapUsers := w.creator.CreateConfigMapCHICommonUsers()
	if err := w.c.ReconcileConfigMap(configMapUsers); err != nil {
		return err
	}

	// Add here other CHI components to be reconciled

	return nil
}

// reconcileCluster reconciles Cluster, excluding nested shards
func (w *worker) reconcileCluster(cluster *chop.ChiCluster) error {
	// Add Cluster's Service
	if service := w.creator.CreateServiceCluster(cluster); service != nil {
		return w.c.ReconcileService(service)
	} else {
		return nil
	}
}

// reconcileShard reconciles Shard, excluding nested replicas
func (w *worker) reconcileShard(shard *chop.ChiShard) error {
	// Add Shard's Service
	if service := w.creator.CreateServiceShard(shard); service != nil {
		return w.c.ReconcileService(service)
	} else {
		return nil
	}
}

// reconcileHost reconciles ClickHouse host
func (w *worker) reconcileHost(host *chop.ChiHost) error {

	// Add host's ConfigMap
	configMap := w.creator.CreateConfigMapHost(host)
	if err := w.c.ReconcileConfigMap(configMap); err != nil {
		return err
	}

	// Add host's StatefulSet
	statefulSet := w.creator.CreateStatefulSet(host)
	if err := w.c.ReconcileStatefulSet(statefulSet, host); err != nil {
		return err
	}

	// Add host's Service
	service := w.creator.CreateServiceHost(host)
	if err := w.c.ReconcileService(service); err != nil {
		return err
	}

	return nil
}

// createTablesOnHost
// TODO move this into Schemer
func (w *worker) createTablesOnHost(chi *chop.ClickHouseInstallation, host *chop.ChiHost) error {
	cluster := &chi.Spec.Configuration.Clusters[host.Address.ClusterIndex]

	names, createSQLs, _ := w.schemer.GetCreateReplicatedObjects(chi, cluster, host)
	log.V(1).Infof("Creating replicated objects: %v", names)
	_ = w.schemer.HostApplySQLs(host, createSQLs, true)

	names, createSQLs, _ = w.schemer.ClusterGetCreateDistributedObjects(chi, cluster)
	log.V(1).Infof("Creating distributed objects: %v", names)
	_ = w.schemer.HostApplySQLs(host, createSQLs, true)

	return nil
}

// createTablesOnShard
// TODO move this into Schemer
func (w *worker) createTablesOnShard(chi *chop.ClickHouseInstallation, shard *chop.ChiShard) error {
	cluster := &chi.Spec.Configuration.Clusters[shard.Address.ClusterIndex]

	names, createSQLs, _ := w.schemer.ClusterGetCreateDistributedObjects(chi, cluster)
	log.V(1).Infof("Creating distributed objects: %v", names)
	_ = w.schemer.ShardApplySQLs(shard, createSQLs, true)

	return nil
}

// deleteTablesOnHost deletes ClickHouse tables on a host
// TODO move this into Schemer
func (w *worker) deleteTablesOnHost(host *chop.ChiHost) error {
	// Delete tables on replica
	tableNames, dropTableSQLs, _ := w.schemer.HostGetDropTables(host)
	log.V(1).Infof("Drop tables: %v as %v", tableNames, dropTableSQLs)
	_ = w.schemer.HostApplySQLs(host, dropTableSQLs, false)

	return nil
}

// deleteCHI deletes all kubernetes resources related to chi *chop.ClickHouseInstallation
func (w *worker) deleteCHI(chi *chop.ClickHouseInstallation) error {
	var err error

	log.V(1).Infof("Start delete CHI %s/%s", chi.Namespace, chi.Name)

	chi, err = w.normalizer.CreateTemplatedCHI(chi, true)
	if err != nil {
		log.V(1).Infof("ClickHouseInstallation (%q): unable to normalize: %q", chi.Name, err)
		return err
	}

	// Delete all clusters
	chi.WalkClusters(func(cluster *chop.ChiCluster) error {
		return w.deleteCluster(cluster)
	})

	// Delete ConfigMap(s)
	err = w.c.deleteConfigMapsChi(chi)

	// Delete Service
	err = w.c.deleteServiceCHI(chi)

	w.c.eventCHI(chi, eventTypeNormal, eventActionDelete, eventReasonDeleteCompleted, "deleted")

	w.c.deleteWatch(chi.Namespace, chi.Name)

	log.V(1).Infof("End delete CHI %s/%s", chi.Namespace, chi.Name)

	return nil
}

// deleteHost deletes all kubernetes resources related to replica *chop.ChiHost
func (w *worker) deleteHost(host *chop.ChiHost) error {
	// Each host consists of
	// 1. Tables on host - we need to delete tables on the host in order to clean Zookeeper data
	// 2. StatefulSet
	// 3. PersistentVolumeClaim
	// 4. ConfigMap
	// 5. Service
	// Need to delete all these item
	log.V(1).Infof("Worker delete host %s/%s", host.Address.ClusterName, host.Name)

	_ = w.deleteTablesOnHost(host)

	return w.c.deleteHost(host)
}

// deleteShard deletes all kubernetes resources related to shard *chop.ChiShard
func (w *worker) deleteShard(shard *chop.ChiShard) error {
	log.V(1).Infof("Start delete shard %s/%s", shard.Address.Namespace, shard.Name)

	// Delete all replicas
	shard.WalkHosts(w.deleteHost)

	// Delete Shard Service
	_ = w.c.deleteServiceShard(shard)
	log.V(1).Infof("End delete shard %s/%s", shard.Address.Namespace, shard.Name)

	return nil
}

// deleteCluster deletes all kubernetes resources related to cluster *chop.ChiCluster
func (w *worker) deleteCluster(cluster *chop.ChiCluster) error {
	log.V(1).Infof("Start delete cluster %s/%s", cluster.Address.Namespace, cluster.Name)

	// Delete all shards
	cluster.WalkShards(func(index int, shard *chop.ChiShard) error {
		return w.deleteShard(shard)
	})

	// Delete Cluster Service
	_ = w.c.deleteServiceCluster(cluster)
	log.V(1).Infof("End delete cluster %s/%s", cluster.Address.Namespace, cluster.Name)

	return nil
}

func (w *worker) createCHIFromObjectMeta(objectMeta *meta.ObjectMeta) (*chop.ClickHouseInstallation, error) {
	chi, err := w.c.GetChiByObjectMeta(objectMeta)
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
	clusterName, err := chopmodels.GetClusterNameFromObjectMeta(objectMeta)
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
