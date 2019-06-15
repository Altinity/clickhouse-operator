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
	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopmetrics "github.com/altinity/clickhouse-operator/pkg/apis/metrics"
	chopclientset "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	chopclientsetscheme "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned/scheme"
	chopinformers "github.com/altinity/clickhouse-operator/pkg/client/informers/externalversions/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/config"
	chopmodels "github.com/altinity/clickhouse-operator/pkg/model"
	"gopkg.in/d4l3k/messagediff.v1"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	kube "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	"github.com/golang/glog"
)

// NewController creates instance of Controller
func NewController(
	version string,
	runtimeParams map[string]string,
	chopConfig *config.Config,
	chopClient chopclientset.Interface,
	kubeClient kube.Interface,
	chiInformer chopinformers.ClickHouseInstallationInformer,
	serviceInformer coreinformers.ServiceInformer,
	endpointsInformer coreinformers.EndpointsInformer,
	configMapInformer coreinformers.ConfigMapInformer,
	statefulSetInformer appsinformers.StatefulSetInformer,
	podInformer coreinformers.PodInformer,
	chopMetricsExporter *chopmetrics.Exporter,
) *Controller {

	// Initializations
	chopclientsetscheme.AddToScheme(scheme.Scheme)
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(
		&typedcore.EventSinkImpl{
			Interface: kubeClient.CoreV1().Events(""),
		},
	)
	recorder := eventBroadcaster.NewRecorder(
		scheme.Scheme,
		core.EventSource{
			Component: componentName,
		},
	)

	// Create Controller instance
	controller := &Controller{
		version:                 version,
		runtimeParams:           runtimeParams,
		normalizer:              chopmodels.NewNormalizer(chopConfig),
		schemer:                 chopmodels.NewSchemer(chopConfig.ChUsername, chopConfig.ChPassword, chopConfig.ChPort),
		chopConfig:              chopConfig,
		kubeClient:              kubeClient,
		chopClient:              chopClient,
		chiLister:               chiInformer.Lister(),
		chiListerSynced:         chiInformer.Informer().HasSynced,
		serviceLister:           serviceInformer.Lister(),
		serviceListerSynced:     serviceInformer.Informer().HasSynced,
		endpointsLister:         endpointsInformer.Lister(),
		endpointsListerSynced:   endpointsInformer.Informer().HasSynced,
		configMapLister:         configMapInformer.Lister(),
		configMapListerSynced:   configMapInformer.Informer().HasSynced,
		statefulSetLister:       statefulSetInformer.Lister(),
		statefulSetListerSynced: statefulSetInformer.Informer().HasSynced,
		podLister:               podInformer.Lister(),
		podListerSynced:         podInformer.Informer().HasSynced,
		queue:                   workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "chi"),
		recorder:                recorder,
		metricsExporter:         chopMetricsExporter,
	}

	return controller
}

func (c *Controller) AddEventHandlers(
	chiInformer chopinformers.ClickHouseInstallationInformer,
	serviceInformer coreinformers.ServiceInformer,
	endpointsInformer coreinformers.EndpointsInformer,
	configMapInformer coreinformers.ConfigMapInformer,
	statefulSetInformer appsinformers.StatefulSetInformer,
	podInformer coreinformers.PodInformer,
) {
	chiInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			chi := obj.(*chop.ClickHouseInstallation)
			if !c.chopConfig.IsWatchedNamespace(chi.Namespace) {
				return
			}
			//glog.V(1).Infof("chiInformer.AddFunc - %s/%s added", chi.Namespace, chi.Name)
			c.enqueueObject(NewReconcileChi(reconcileAdd, nil, chi))
		},
		UpdateFunc: func(old, new interface{}) {
			newChi := new.(*chop.ClickHouseInstallation)
			oldChi := old.(*chop.ClickHouseInstallation)
			if !c.chopConfig.IsWatchedNamespace(newChi.Namespace) {
				return
			}
			//glog.V(1).Info("chiInformer.UpdateFunc")
			c.enqueueObject(NewReconcileChi(reconcileUpdate, oldChi, newChi))
		},
		DeleteFunc: func(obj interface{}) {
			chi := obj.(*chop.ClickHouseInstallation)
			if !c.chopConfig.IsWatchedNamespace(chi.Namespace) {
				return
			}
			//glog.V(1).Infof("chiInformer.DeleteFunc - CHI %s/%s deleted", chi.Namespace, chi.Name)
			c.enqueueObject(NewReconcileChi(reconcileDelete, chi, nil))
		},
	})

	serviceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			service := obj.(*core.Service)
			if !c.isTrackedObject(&service.ObjectMeta) {
				return
			}
			//glog.V(1).Infof("serviceInformer AddFunc %s/%s", service.Namespace, service.Name)
		},
		UpdateFunc: func(old, new interface{}) {
			oldService := old.(*core.Service)
			newService := new.(*core.Service)
			if !c.isTrackedObject(&oldService.ObjectMeta) {
				return
			}

			diff, equal := messagediff.DeepDiff(oldService, newService)
			if equal {
				//glog.V(1).Infof("onUpdateService(%s/%s): no changes found", oldService.Namespace, oldService.Name)
				// No need tor react
				return
			}

			for path := range diff.Added {
				glog.V(1).Infof("onUpdateService(%s/%s): added %v", oldService.Namespace, oldService.Name, path)
			}
			for path := range diff.Removed {
				glog.V(1).Infof("onUpdateService(%s/%s): removed %v", oldService.Namespace, oldService.Name, path)
			}
			for path := range diff.Modified {
				glog.V(1).Infof("onUpdateService(%s/%s): modified %v", oldService.Namespace, oldService.Name, path)
			}

			if (oldService.Spec.ClusterIP == "") && (newService.Spec.ClusterIP != "") {
				// Internal IP address assigned
				// Pod restart completed?
				glog.V(1).Infof("serviceInformer UpdateFunc(%s/%s) IP ASSIGNED %s:%s", newService.Namespace, newService.Name, newService.Spec.Type, newService.Spec.ClusterIP)
				if cluster, err := c.createClusterFromObjectMeta(&newService.ObjectMeta); err != nil {
					glog.V(1).Infof("serviceInformer UpdateFunc(%s/%s) flushing DNS for cluster %s", newService.Namespace, newService.Name, cluster.Name)
					//chopmodels.ClusterDropDnsCache(cluster)
				} else {
					glog.V(1).Infof("serviceInformer UpdateFunc(%s/%s) unable to find cluster cluster", newService.Namespace, newService.Name)
				}
			} else if (oldService.Spec.ClusterIP != "") && (newService.Spec.ClusterIP == "") {
				// Internal IP address lost
				// Pod restart?
				glog.V(1).Infof("serviceInformer UpdateFunc(%s/%s) IP LOST %s:%s", oldService.Namespace, oldService.Name, oldService.Spec.Type, oldService.Spec.ClusterIP)
			} else {
				glog.V(1).Infof("serviceInformer UpdateFunc(%s/%s)", oldService.Namespace, oldService.Name)
			}
		},
		DeleteFunc: func(obj interface{}) {
			service := obj.(*core.Service)
			if !c.isTrackedObject(&service.ObjectMeta) {
				return
			}
			//glog.V(1).Infof("serviceInformer DeleteFunc %s/%s", service.Namespace, service.Name)
		},
	})

	endpointsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			endpoints := obj.(*core.Endpoints)
			if !c.isTrackedObject(&endpoints.ObjectMeta) {
				return
			}
			//glog.V(1).Infof("endpointsInformer AddFunc %s/%s", endpoints.Namespace, endpoints.Name)
		},
		UpdateFunc: func(old, new interface{}) {
			oldEndpoints := old.(*core.Endpoints)
			newEndpoints := new.(*core.Endpoints)
			if !c.isTrackedObject(&oldEndpoints.ObjectMeta) {
				return
			}

			diff, equal := messagediff.DeepDiff(oldEndpoints, newEndpoints)
			if equal {
				//glog.V(1).Infof("onUpdateEndpoints(%s/%s): no changes found", oldEndpoints.Namespace, oldEndpoints.Name)
				// No need to react
				return
			}

			added := false
			for path := range diff.Added {
				glog.V(1).Infof("onUpdateEndpoints(%s/%s): added %v", oldEndpoints.Namespace, oldEndpoints.Name, path)
				for _, pathnode := range *path {
					s := pathnode.String()
					if s == ".Addresses" {
						added = true
					}
				}
			}
			for path := range diff.Removed {
				glog.V(1).Infof("onUpdateEndpoints(%s/%s): removed %v", oldEndpoints.Namespace, oldEndpoints.Name, path)
			}
			for path := range diff.Modified {
				glog.V(1).Infof("onUpdateEndpoints(%s/%s): modified %v", oldEndpoints.Namespace, oldEndpoints.Name, path)
				for _, pathnode := range *path {
					s := pathnode.String()
					if s == ".Addresses" {
						added = true
					}
				}
			}

			if added {
				glog.V(1).Infof("endpointsInformer UpdateFunc(%s/%s) IP ASSIGNED %v", newEndpoints.Namespace, newEndpoints.Name, newEndpoints.Subsets)
				if chi, err := c.createChiFromObjectMeta(&newEndpoints.ObjectMeta); err == nil {
					glog.V(1).Infof("endpointsInformer UpdateFunc(%s/%s) flushing DNS for CHI %s", newEndpoints.Namespace, newEndpoints.Name, chi.Name)
					_ = c.schemer.ChiDropDnsCache(chi)
				} else {
					glog.V(1).Infof("endpointsInformer UpdateFunc(%s/%s) unable to find CHI by %v", newEndpoints.Namespace, newEndpoints.Name, newEndpoints.ObjectMeta.Labels)
				}
			}
		},
		DeleteFunc: func(obj interface{}) {
			endpoints := obj.(*core.Endpoints)
			if !c.isTrackedObject(&endpoints.ObjectMeta) {
				return
			}
			//glog.V(1).Infof("endpointsInformer DeleteFunc %s/%s", endpoints.Namespace, endpoints.Name)
		},
	})

	configMapInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			configMap := obj.(*core.ConfigMap)
			if !c.isTrackedObject(&configMap.ObjectMeta) {
				return
			}
			//glog.V(1).Infof("configMapInformer AddFunc %s/%s", configMap.Namespace, configMap.Name)
		},
		UpdateFunc: func(old, new interface{}) {
			configMap := old.(*core.ConfigMap)
			if !c.isTrackedObject(&configMap.ObjectMeta) {
				return
			}
			//glog.V(1).Infof("configMapInformer UpdateFunc %s/%s", configMap.Namespace, configMap.Name)
		},
		DeleteFunc: func(obj interface{}) {
			configMap := obj.(*core.ConfigMap)
			if !c.isTrackedObject(&configMap.ObjectMeta) {
				return
			}
			//glog.V(1).Infof("configMapInformer DeleteFunc %s/%s", configMap.Namespace, configMap.Name)
		},
	})

	statefulSetInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			statefulSet := obj.(*apps.StatefulSet)
			if !c.isTrackedObject(&statefulSet.ObjectMeta) {
				return
			}
			//glog.V(1).Infof("statefulSetInformer AddFunc %s/%s", statefulSet.Namespace, statefulSet.Name)
			//controller.handleObject(obj)
		},
		UpdateFunc: func(old, new interface{}) {
			statefulSet := old.(*apps.StatefulSet)
			if !c.isTrackedObject(&statefulSet.ObjectMeta) {
				return
			}
			//glog.V(1).Infof("statefulSetInformer UpdateFunc %s/%s", statefulSet.Namespace, statefulSet.Name)
		},
		DeleteFunc: func(obj interface{}) {
			statefulSet := obj.(*apps.StatefulSet)
			if !c.isTrackedObject(&statefulSet.ObjectMeta) {
				return
			}
			//glog.V(1).Infof("statefulSetInformer DeleteFunc %s/%s", statefulSet.Namespace, statefulSet.Name)
			//controller.handleObject(obj)
		},
	})

	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*core.Pod)
			if !c.isTrackedObject(&pod.ObjectMeta) {
				return
			}
			//glog.V(1).Infof("podInformer AddFunc %s/%s", pod.Namespace, pod.Name)
		},
		UpdateFunc: func(old, new interface{}) {
			pod := old.(*core.Pod)
			if !c.isTrackedObject(&pod.ObjectMeta) {
				return
			}
			//glog.V(1).Infof("podInformer UpdateFunc %s/%s", pod.Namespace, pod.Name)
		},
		DeleteFunc: func(obj interface{}) {
			pod := obj.(*core.Pod)
			if !c.isTrackedObject(&pod.ObjectMeta) {
				return
			}
			//glog.V(1).Infof("podInformer DeleteFunc %s/%s", pod.Namespace, pod.Name)
		},
	})
}

// isTrackedObject checks whether operator is interested in changes of this object
func (c *Controller) isTrackedObject(objectMeta *meta.ObjectMeta) bool {
	return c.chopConfig.IsWatchedNamespace(objectMeta.Namespace) && chopmodels.IsChopGeneratedObject(objectMeta)
}

// Run syncs caches, starts workers
func (c *Controller) Run(ctx context.Context, threadiness int) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	glog.V(1).Info("Starting ClickHouseInstallation controller")
	if !waitForCacheSync(
		"ClickHouseInstallation",
		ctx.Done(),
		c.chiListerSynced,
		c.statefulSetListerSynced,
		c.configMapListerSynced,
		c.serviceListerSynced,
	) {
		// Unable to sync
		return
	}

	// Label controller runtime objects with proper labels
	c.labelMyObjectsTree()

	glog.V(1).Info("ClickHouseInstallation controller: starting workers")
	for i := 0; i < threadiness; i++ {
		glog.V(1).Infof("ClickHouseInstallation controller: starting worker %d with poll period %d", i+1, runWorkerPeriod)
		go wait.Until(c.runWorker, runWorkerPeriod, ctx.Done())
	}
	defer glog.V(1).Info("ClickHouseInstallation controller: shutting down workers")

	glog.V(1).Info("ClickHouseInstallation controller: workers started")
	<-ctx.Done()
}

// runWorker is a convenience wrap over processNextWorkItem()
func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem processes objects from the workqueue
func (c *Controller) processNextWorkItem() bool {
	item, shutdown := c.queue.Get()
	if shutdown {
		glog.V(1).Info("processNextWorkItem(): shutdown request")
		return false
	}

	err := func(item interface{}) error {
		defer c.queue.Done(item)

		reconcile, ok := item.(*ReconcileChi)
		if !ok {
			// Item is impossible to process, no more retries
			c.queue.Forget(item)
			utilruntime.HandleError(fmt.Errorf("unexpected item in the queue - %#v", item))
			return nil
		}

		// Main reconcile loop function sync an item
		if err := c.syncChi(reconcile); err != nil {
			// Item will be retried later
			return fmt.Errorf("unable to sync an object %v", err.Error())
		}

		// Item is processed, no more retries
		c.queue.Forget(item)

		return nil
	}(item)
	if err != nil {
		utilruntime.HandleError(err)
	}

	// Continue iteration
	return true
}

// syncChi is the main reconcile loop function - reconcile CHI object identified by `key`
func (c *Controller) syncChi(reconcile *ReconcileChi) error {
	glog.V(1).Infof("syncChi(%s) start", reconcile.cmd)

	// Check CHI object already in sync
	switch reconcile.cmd {
	case reconcileAdd:
		return c.onAddChi(reconcile.new)
	case reconcileUpdate:
		return c.onUpdateChi(reconcile.old, reconcile.new)
	case reconcileDelete:
		return c.onDeleteChi(reconcile.old)
	}

	return nil
}

// onAddChi sync new CHI - creates all its resources
func (c *Controller) onAddChi(chi *chop.ClickHouseInstallation) error {
	// CHI is a new one - need to create all its objects
	// Operator receives CHI struct partially filled by data from .yaml file provided by user
	// We need to create all resources that are needed to run user's .yaml specification
	glog.V(1).Infof("onAddChi(%s/%s)", chi.Namespace, chi.Name)

	c.eventChi(chi, eventTypeNormal, eventActionCreate, eventReasonCreateStarted, fmt.Sprintf("onAddChi(%s/%s)", chi.Namespace, chi.Name))
	chi, err := c.normalizer.CreateTemplatedChi(chi)
	if err != nil {
		glog.V(1).Infof("ClickHouseInstallation (%q): unable to normalize: %q", chi.Name, err)
		c.eventChi(chi, eventTypeWarning, eventActionCreate, eventReasonCreateFailed, fmt.Sprintf("ClickHouseInstallation (%s): unable to normalize", chi.Name))
		return err
	}

	c.eventChi(chi, eventTypeNormal, eventActionCreate, eventReasonCreateInProgress, fmt.Sprintf("onAddChi(%s/%s) create objects", chi.Namespace, chi.Name))
	err = c.reconcile(chi)
	if err != nil {
		glog.V(1).Infof("ClickHouseInstallation (%q): unable to create controlled resources: %q", chi.Name, err)
		c.eventChi(chi, eventTypeWarning, eventActionCreate, eventReasonCreateFailed, fmt.Sprintf("ClickHouseInstallation (%s): unable to create", chi.Name))
		return err
	}

	// Update CHI status in k8s
	c.eventChi(chi, eventTypeNormal, eventActionCreate, eventReasonCreateInProgress, fmt.Sprintf("onAddChi(%s/%s) create CHI", chi.Namespace, chi.Name))
	if err := c.updateCHIResource(chi); err != nil {
		glog.V(1).Infof("ClickHouseInstallation (%q): unable to update status of CHI resource: %q", chi.Name, err)
		c.eventChi(chi, eventTypeWarning, eventActionCreate, eventReasonCreateFailed, fmt.Sprintf("ClickHouseInstallation (%s): unable to update CHI Resource", chi.Name))
		return err
	}

	glog.V(1).Infof("ClickHouseInstallation (%q): controlled resources are synced (created)", chi.Name)
	c.eventChi(chi, eventTypeNormal, eventActionCreate, eventReasonCreateCompleted, fmt.Sprintf("onAddChi(%s/%s)", chi.Namespace, chi.Name))

	// Check hostnames of the Pods from current CHI object included into chopmetrics.Exporter state
	c.metricsExporter.EnsureControlledValues(chi.Name, chopmodels.CreatePodFQDNsOfChi(chi))

	return nil
}

// onUpdateChi sync CHI which was already created earlier
func (c *Controller) onUpdateChi(old, new *chop.ClickHouseInstallation) error {
	glog.V(1).Infof("onUpdateChi(%s/%s):", old.Namespace, old.Name)

	if old.ObjectMeta.ResourceVersion == new.ObjectMeta.ResourceVersion {
		glog.V(1).Infof("onUpdateChi(%s/%s): ResourceVersion did not change: %s", old.Namespace, old.Name, old.ObjectMeta.ResourceVersion)
		// No need to react
		return nil
	}

	if !old.IsKnown() && new.IsKnown() {
		glog.V(1).Infof("onUpdateChi(%s/%s): This `update` event triggered by `save` filled CHI action", old.Namespace, old.Name)
		// This `update` event triggered by `save` filled CHI action
		// No need to react
		return nil
	}

	if !old.IsNormalized() {
		old, _ = c.normalizer.CreateTemplatedChi(old)
	}

	if !new.IsNormalized() {
		new, _ = c.normalizer.CreateTemplatedChi(new)
	}

	diff, equal := messagediff.DeepDiff(old, new)

	if equal {
		glog.V(1).Infof("onUpdateChi(%s/%s): no changes found", old.Namespace, old.Name)
		// No need tor react
		return nil
	}

	c.eventChi(old, eventTypeNormal, eventActionUpdate, eventReasonUpdateStarted, fmt.Sprintf("onUpdateChi(%s/%s):", old.Namespace, old.Name))

	// Deal with removed items
	// TODO refactor to map[string]object handling, instead of slice
	for path := range diff.Removed {
		switch diff.Removed[path].(type) {
		case chop.ChiCluster:
			cluster := diff.Removed[path].(chop.ChiCluster)
			c.eventChi(old, eventTypeNormal, eventActionUpdate, eventReasonUpdateInProgress, fmt.Sprintf("delete cluster %s", cluster.Name))
			c.deleteCluster(&cluster)
		case chop.ChiShard:
			shard := diff.Removed[path].(chop.ChiShard)
			c.eventChi(old, eventTypeNormal, eventActionUpdate, eventReasonUpdateInProgress, fmt.Sprintf("delete shard %d", shard.Address.ShardIndex))
			c.deleteShard(&shard)
		case chop.ChiReplica:
			replica := diff.Removed[path].(chop.ChiReplica)
			c.eventChi(old, eventTypeNormal, eventActionUpdate, eventReasonUpdateInProgress, fmt.Sprintf("delete replica %d", replica.Address.ReplicaIndex))
			_ = c.deleteReplica(&replica)
		}
	}

	// Deal with added/updated items
	//	c.listStatefulSetResources(chi)
	c.eventChi(old, eventTypeNormal, eventActionUpdate, eventReasonUpdateInProgress, fmt.Sprintf("onUpdateChi(%s/%s) update resources", old.Namespace, old.Name))
	if err := c.reconcile(new); err != nil {
		glog.V(1).Infof("reconcileChi() FAILED: %v", err)
		c.eventChi(old, eventTypeWarning, eventActionUpdate, eventReasonUpdateFailed, fmt.Sprintf("onUpdateChi(%s/%s) update resources failed", old.Namespace, old.Name))
	} else {
		c.eventChi(old, eventTypeNormal, eventActionUpdate, eventReasonUpdateInProgress, fmt.Sprintf("onUpdateChi(%s/%s) migrate schema", old.Namespace, old.Name))
		new.WalkClusters(func(cluster *chop.ChiCluster) error {
			dbNames, createDatabaseSQLs, _ := c.schemer.ClusterGetCreateDatabases(new, cluster)
			glog.V(1).Infof("Creating databases: %v", dbNames)
			_ = c.schemer.ClusterApplySQLs(cluster, createDatabaseSQLs, true)

			tableNames, createTableSQLs, _ := c.schemer.ClusterGetCreateTables(new, cluster)
			glog.V(1).Infof("Creating tables: %v", tableNames)
			_ = c.schemer.ClusterApplySQLs(cluster, createTableSQLs, true)
			return nil
		})
		_ = c.updateCHIResource(new)

		c.eventChi(old, eventTypeNormal, eventActionUpdate, eventReasonUpdateCompleted, fmt.Sprintf("onUpdateChi(%s/%s) completed", old.Namespace, old.Name))
	}

	// Check hostnames of the Pods from current CHI object included into chopmetrics.Exporter state
	c.metricsExporter.EnsureControlledValues(new.Name, chopmodels.CreatePodFQDNsOfChi(new))

	return nil
}

func (c *Controller) onDeleteChi(chi *chop.ClickHouseInstallation) error {
	chi, err := c.normalizer.CreateTemplatedChi(chi)
	if err != nil {
		glog.V(1).Infof("ClickHouseInstallation (%q): unable to normalize: %q", chi.Name, err)
		return err
	}

	c.eventChi(chi, eventTypeNormal, eventActionDelete, eventReasonDeleteStarted, fmt.Sprintf("onDeleteChi(%s/%s) started", chi.Namespace, chi.Name))
	c.deleteChi(chi)
	c.eventChi(chi, eventTypeNormal, eventActionDelete, eventReasonDeleteCompleted, fmt.Sprintf("onDeleteChi(%s/%s) completed", chi.Namespace, chi.Name))

	return nil
}

// updateCHIResource updates ClickHouseInstallation resource
func (c *Controller) updateCHIResource(chi *chop.ClickHouseInstallation) error {
	_, err := c.chopClient.ClickhouseV1().ClickHouseInstallations(chi.Namespace).Update(chi)

	return err
}

// enqueueObject adds ClickHouseInstallation object to the workqueue
func (c *Controller) enqueueObject(obj interface{}) {
	c.queue.AddRateLimited(obj)
}

// handleObject enqueues CHI which is owner of `obj` into reconcile loop
func (c *Controller) handleObject(obj interface{}) {
	object, ok := obj.(meta.Object)
	if !ok {
		ts, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf(messageUnableToDecode))
			return
		}
		object, ok = ts.Obj.(meta.Object)
		if !ok {
			utilruntime.HandleError(fmt.Errorf(messageUnableToDecode))
			return
		}
	}

	// object is an instance of meta.Object

	// Checking that we control current StatefulSet Object
	ownerRef := meta.GetControllerOf(object)
	if ownerRef == nil {
		// No owner
		return
	}

	// Ensure owner is of a proper kind
	if ownerRef.Kind != chop.ClickHouseInstallationCRDResourceKind {
		return
	}

	glog.V(1).Infof("Processing object: %s", object.GetName())

	// Get owner - it is expected to be CHI
	chi, err := c.chiLister.ClickHouseInstallations(object.GetNamespace()).Get(ownerRef.Name)

	if err != nil {
		glog.V(1).Infof("ignoring orphaned object '%s' of ClickHouseInstallation '%s'", object.GetSelfLink(), ownerRef.Name)
		return
	}

	// Add CHI object into reconcile loop
	c.enqueueObject(chi)
}

// waitForCacheSync is a logger-wrapper over cache.WaitForCacheSync() and it waits for caches to populate
func waitForCacheSync(name string, stopCh <-chan struct{}, cacheSyncs ...cache.InformerSynced) bool {
	glog.V(1).Infof("Syncing caches for %s controller", name)
	if !cache.WaitForCacheSync(stopCh, cacheSyncs...) {
		utilruntime.HandleError(fmt.Errorf(messageUnableToSync, name))
		return false
	}
	glog.V(1).Infof("Caches are synced for %s controller", name)
	return true
}
