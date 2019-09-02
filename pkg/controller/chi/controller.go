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
	"github.com/altinity/clickhouse-operator/pkg/apis/metrics"
	chopclientset "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	chopclientsetscheme "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned/scheme"
	chopinformers "github.com/altinity/clickhouse-operator/pkg/client/informers/externalversions"
	"github.com/altinity/clickhouse-operator/pkg/config"
	chopmodels "github.com/altinity/clickhouse-operator/pkg/model"
	"gopkg.in/d4l3k/messagediff.v1"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeinformers "k8s.io/client-go/informers"
	kube "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"os"

	"github.com/golang/glog"
)

// NewController creates instance of Controller
func NewController(
	version string,
	runtimeParams map[string]string,
	chopConfigManager *config.ConfigManager,
	chopConfig *chop.Config,
	chopClient chopclientset.Interface,
	kubeClient kube.Interface,
	chopInformerFactory chopinformers.SharedInformerFactory,
	kubeInformerFactory kubeinformers.SharedInformerFactory,
) *Controller {

	// Initializations
	_ = chopclientsetscheme.AddToScheme(scheme.Scheme)

	// Setup events
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
		creator:                 nil,
		chopConfigManager:       chopConfigManager,
		chopConfig:              chopConfig,
		kubeClient:              kubeClient,
		chopClient:              chopClient,
		chiLister:               chopInformerFactory.Clickhouse().V1().ClickHouseInstallations().Lister(),
		chiListerSynced:         chopInformerFactory.Clickhouse().V1().ClickHouseInstallations().Informer().HasSynced,
		chitLister:              chopInformerFactory.Clickhouse().V1().ClickHouseInstallationTemplates().Lister(),
		chitListerSynced:        chopInformerFactory.Clickhouse().V1().ClickHouseInstallationTemplates().Informer().HasSynced,
		serviceLister:           kubeInformerFactory.Core().V1().Services().Lister(),
		serviceListerSynced:     kubeInformerFactory.Core().V1().Services().Informer().HasSynced,
		endpointsLister:         kubeInformerFactory.Core().V1().Endpoints().Lister(),
		endpointsListerSynced:   kubeInformerFactory.Core().V1().Endpoints().Informer().HasSynced,
		configMapLister:         kubeInformerFactory.Core().V1().ConfigMaps().Lister(),
		configMapListerSynced:   kubeInformerFactory.Core().V1().ConfigMaps().Informer().HasSynced,
		statefulSetLister:       kubeInformerFactory.Apps().V1().StatefulSets().Lister(),
		statefulSetListerSynced: kubeInformerFactory.Apps().V1().StatefulSets().Informer().HasSynced,
		podLister:               kubeInformerFactory.Core().V1().Pods().Lister(),
		podListerSynced:         kubeInformerFactory.Core().V1().Pods().Informer().HasSynced,
		queue:                   workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "chi"),
		recorder:                recorder,
	}

	controller.addEventHandlers(chopInformerFactory, kubeInformerFactory)

	return controller
}

func (c *Controller) addEventHandlers(
	chopInformerFactory chopinformers.SharedInformerFactory,
	kubeInformerFactory kubeinformers.SharedInformerFactory,
) {
	chopInformerFactory.Clickhouse().V1().ClickHouseInstallations().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
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

	chopInformerFactory.Clickhouse().V1().ClickHouseInstallationTemplates().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			chit := obj.(*chop.ClickHouseInstallationTemplate)
			if !c.chopConfig.IsWatchedNamespace(chit.Namespace) {
				return
			}
			//glog.V(1).Infof("chitInformer.AddFunc - %s/%s added", chit.Namespace, chit.Name)
			c.enqueueObject(NewReconcileChit(reconcileAdd, nil, chit))
		},
		UpdateFunc: func(old, new interface{}) {
			newChit := new.(*chop.ClickHouseInstallationTemplate)
			oldChit := old.(*chop.ClickHouseInstallationTemplate)
			if !c.chopConfig.IsWatchedNamespace(newChit.Namespace) {
				return
			}
			//glog.V(1).Infof("chitInformer.UpdateFunc - %s/%s", newChit.Namespace, newChit.Name)
			c.enqueueObject(NewReconcileChit(reconcileUpdate, oldChit, newChit))
		},
		DeleteFunc: func(obj interface{}) {
			chit := obj.(*chop.ClickHouseInstallationTemplate)
			if !c.chopConfig.IsWatchedNamespace(chit.Namespace) {
				return
			}
			//glog.V(1).Infof("chitInformer.DeleteFunc - %s/%s deleted", chit.Namespace, chit.Name)
			c.enqueueObject(NewReconcileChit(reconcileDelete, chit, nil))
		},
	})

	chopInformerFactory.Clickhouse().V1().ClickHouseOperatorConfigurations().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			chopConfig := obj.(*chop.ClickHouseOperatorConfiguration)
			if !c.chopConfig.IsWatchedNamespace(chopConfig.Namespace) {
				return
			}
			//glog.V(1).Infof("chitInformer.AddFunc - %s/%s added", chit.Namespace, chit.Name)
			c.enqueueObject(NewReconcileChopConfig(reconcileAdd, nil, chopConfig))
		},
		UpdateFunc: func(old, new interface{}) {
			newChopConfig := new.(*chop.ClickHouseOperatorConfiguration)
			oldChopConfig := old.(*chop.ClickHouseOperatorConfiguration)
			if !c.chopConfig.IsWatchedNamespace(newChopConfig.Namespace) {
				return
			}
			//glog.V(1).Infof("chitInformer.UpdateFunc - %s/%s", newChit.Namespace, newChit.Name)
			c.enqueueObject(NewReconcileChopConfig(reconcileUpdate, oldChopConfig, newChopConfig))
		},
		DeleteFunc: func(obj interface{}) {
			chopConfig := obj.(*chop.ClickHouseOperatorConfiguration)
			if !c.chopConfig.IsWatchedNamespace(chopConfig.Namespace) {
				return
			}
			//glog.V(1).Infof("chitInformer.DeleteFunc - %s/%s deleted", chit.Namespace, chit.Name)
			c.enqueueObject(NewReconcileChopConfig(reconcileDelete, chopConfig, nil))
		},
	})

	kubeInformerFactory.Core().V1().Services().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
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

	kubeInformerFactory.Core().V1().Endpoints().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
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
				glog.V(2).Infof("onUpdateEndpoints(%s/%s): added %v", oldEndpoints.Namespace, oldEndpoints.Name, path)
				for _, pathnode := range *path {
					s := pathnode.String()
					if s == ".Addresses" {
						added = true
					}
				}
			}
			for path := range diff.Removed {
				glog.V(2).Infof("onUpdateEndpoints(%s/%s): removed %v", oldEndpoints.Namespace, oldEndpoints.Name, path)
			}
			for path := range diff.Modified {
				glog.V(2).Infof("onUpdateEndpoints(%s/%s): modified %v", oldEndpoints.Namespace, oldEndpoints.Name, path)
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

	kubeInformerFactory.Core().V1().ConfigMaps().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
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

	kubeInformerFactory.Apps().V1().StatefulSets().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
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

	kubeInformerFactory.Core().V1().Pods().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
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

	//
	// Start threads
	//
	glog.V(1).Info("ClickHouseInstallation controller: starting workers")
	for i := 0; i < threadiness; i++ {
		glog.V(1).Infof("ClickHouseInstallation controller: starting worker %d with poll period %d", i+1, runWorkerPeriod)
		go wait.Until(c.runWorker, runWorkerPeriod, ctx.Done())
	}
	defer glog.V(1).Info("ClickHouseInstallation controller: shutting down workers")

	glog.V(1).Info("ClickHouseInstallation controller: workers started")
	<-ctx.Done()
}

// runWorker is an endless work loop running in a thread
func (c *Controller) runWorker() {
	for {
		// Get() blocks until it can return an item
		item, shutdown := c.queue.Get()
		if shutdown {
			glog.V(1).Info("runWorker(): shutdown request")
			return
		}

		if err := c.processWorkItem(item); err == nil {
			// Item processed
			// Forget indicates that an item is finished being retried.  Doesn't matter whether its for perm failing
			// or for success, we'll stop the rate limiter from tracking it.  This only clears the `rateLimiter`, you
			// still have to call `Done` on the queue.
			c.queue.Forget(item)
		} else {
			// Item not processed
			// this code cannot return an error and needs to indicate error has been ignored
			utilruntime.HandleError(err)
		}

		// Must call Done(item) when processing completed
		c.queue.Done(item)
	}
}

// processWorkItem processes one work item according to its type
func (c *Controller) processWorkItem(item interface{}) error {
	switch item.(type) {

	case *ReconcileChi:
		reconcile, _ := item.(*ReconcileChi)
		switch reconcile.cmd {
		case reconcileAdd:
			return c.addChi(reconcile.new)
		case reconcileUpdate:
			return c.updateChi(reconcile.old, reconcile.new)
		case reconcileDelete:
			return c.deleteChi(reconcile.old)
		}

		// Unknown item type, don't know what to do with it
		// Just skip it and behave like it never existed
		utilruntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", reconcile))
		return nil

	case *ReconcileChit:
		reconcile, _ := item.(*ReconcileChit)
		switch reconcile.cmd {
		case reconcileAdd:
			return c.addChit(reconcile.new)
		case reconcileUpdate:
			return c.updateChit(reconcile.old, reconcile.new)
		case reconcileDelete:
			return c.deleteChit(reconcile.old)
		}

		// Unknown item type, don't know what to do with it
		// Just skip it and behave like it never existed
		utilruntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", reconcile))
		return nil

	case *ReconcileChopConfig:
		reconcile, _ := item.(*ReconcileChopConfig)
		switch reconcile.cmd {
		case reconcileAdd:
			return c.addChopConfig(reconcile.new)
		case reconcileUpdate:
			return c.updateChopConfig(reconcile.old, reconcile.new)
		case reconcileDelete:
			return c.deleteChopConfig(reconcile.old)
		}

		// Unknown item type, don't know what to do with it
		// Just skip it and behave like it never existed
		utilruntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", reconcile))
		return nil
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilruntime.HandleError(fmt.Errorf("unexpected item in the queue - %#v", item))
	return nil
}

// addChi normalize CHI - updates CHI object to normalized
func (c *Controller) addChi(chi *chop.ClickHouseInstallation) error {
	// CHI is a new one - need to create normalized CHI
	// Operator receives CHI struct partially filled by data from .yaml file provided by user
	// We need to create full normalized specification
	glog.V(1).Infof("addChi(%s/%s)", chi.Namespace, chi.Name)

	chi, err := c.normalizer.CreateTemplatedChi(chi)
	if err != nil {
		glog.V(1).Infof("ClickHouseInstallation (%s): unable to normalize: %q", chi.Name, err)
		c.eventChi(chi, eventTypeError, eventActionCreate, eventReasonCreateFailed, "unable to normalize configuration")
		return err
	}

	// Update CHI object
	if err := c.updateCHIResource(chi); err != nil {
		glog.V(1).Infof("ClickHouseInstallation (%s): unable to update CHI object: %q", chi.Name, err)
		c.eventChi(chi, eventTypeWarning, eventActionCreate, eventReasonCreateFailed, "Unable to update CHI Resource")
		return err
	}

	// Report CHI normalized
	glog.V(1).Infof("ClickHouseInstallation (%s): created", chi.Name)
	c.eventChi(chi, eventTypeNormal, eventActionCreate, eventReasonCreateCompleted, fmt.Sprintf("ClickHouseInstallation (%s): added", chi.Name))

	return nil
}

// updateChi sync CHI which was already created earlier
func (c *Controller) updateChi(old, new *chop.ClickHouseInstallation) error {

	if !old.IsNormalized() {
		old, _ = c.normalizer.CreateTemplatedChi(old)
	}

	if !new.IsNormalized() {
		new, _ = c.normalizer.CreateTemplatedChi(new)
	}

	if old.ObjectMeta.ResourceVersion == new.ObjectMeta.ResourceVersion {
		glog.V(2).Infof("updateChi(%s/%s): ResourceVersion did not change: %s", old.Namespace, old.Name, old.ObjectMeta.ResourceVersion)
		// No need to react

		// Update hostnames list for monitor
		c.updateWatch(new.Namespace, new.Name, chopmodels.CreatePodFQDNsOfChi(new))

		return nil
	}

	glog.V(1).Infof("updateChi(%s/%s)", old.Namespace, old.Name)

	diff, equal := messagediff.DeepDiff(old, new)

	if equal {
		glog.V(1).Infof("updateChi(%s/%s): no changes found", old.Namespace, old.Name)
		// No need to react
		return nil
	}

	if err := c.reconcile(new); err != nil {
		log := fmt.Sprintf("Update of resources has FAILED: %v", err)
		glog.V(1).Info(log)
		c.eventChi(old, eventTypeError, eventActionUpdate, eventReasonUpdateFailed, log)
		return nil
	}

	// Post-process added items
	for path := range diff.Added {
		switch diff.Added[path].(type) {
		case chop.ChiCluster:
			cluster := diff.Added[path].(chop.ChiCluster)

			log := fmt.Sprintf("Added cluster %s", cluster.Name)
			glog.V(1).Info(log)
			c.eventChi(old, eventTypeNormal, eventActionUpdate, eventReasonUpdateInProgress, log)
		case chop.ChiShard:
			shard := diff.Added[path].(chop.ChiShard)

			log := fmt.Sprintf("Added shard %d to cluster %s", shard.Address.ShardIndex, shard.Address.ClusterName)
			glog.V(1).Info(log)
			c.eventChi(old, eventTypeNormal, eventActionUpdate, eventReasonUpdateInProgress, log)

			_ = c.createTablesOnShard(new, &shard)
		case chop.ChiHost:
			host := diff.Added[path].(chop.ChiHost)

			log := fmt.Sprintf("Added replica %d to shard %d in cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName)
			glog.V(1).Info(log)
			c.eventChi(old, eventTypeNormal, eventActionUpdate, eventReasonUpdateInProgress, log)

			_ = c.createTablesOnHost(new, &host)
		}
	}

	// Remove deleted items
	// TODO refactor to map[string]object handling, instead of slice
	for path := range diff.Removed {
		switch diff.Removed[path].(type) {
		case chop.ChiCluster:
			cluster := diff.Removed[path].(chop.ChiCluster)
			c.eventChi(old, eventTypeNormal, eventActionUpdate, eventReasonUpdateInProgress, fmt.Sprintf("delete cluster %s", cluster.Name))

			_ = c.deleteCluster(&cluster)
		case chop.ChiShard:
			shard := diff.Removed[path].(chop.ChiShard)
			c.eventChi(old, eventTypeNormal, eventActionUpdate, eventReasonUpdateInProgress, fmt.Sprintf("delete shard %d in cluster %s", shard.Address.ShardIndex, shard.Address.ClusterName))

			_ = c.deleteShard(&shard)
		case chop.ChiHost:
			host := diff.Removed[path].(chop.ChiHost)
			c.eventChi(old, eventTypeNormal, eventActionUpdate, eventReasonUpdateInProgress, fmt.Sprintf("delete replica %d from shard %d in cluster %s", host.Address.ReplicaIndex, host.Address.ShardIndex, host.Address.ClusterName))

			_ = c.deleteHost(&host)
		}
	}

	// Update CHI object
	_ = c.updateCHIResource(new)

	//c.metricsExporter.UpdateWatch(new.Namespace, new.Name, chopmodels.CreatePodFQDNsOfChi(new))
	c.updateWatch(new.Namespace, new.Name, chopmodels.CreatePodFQDNsOfChi(new))

	return nil
}

func (c *Controller) updateWatch(namespace, name string, hostnames []string) {
	go c.updateWatchAsync(namespace, name, hostnames)
}

func (c *Controller) updateWatchAsync(namespace, name string, hostnames []string) {
	if err := metrics.UpdateWatchREST(namespace, name, hostnames); err != nil {
		glog.V(1).Infof("FAIL update watch (%s/%s): %q", namespace, name, err)
	} else {
		glog.V(1).Infof("OK update watch (%s/%s)", namespace, name)
	}
}

// addChit sync new CHIT - creates all its resources
func (c *Controller) addChit(chit *chop.ClickHouseInstallationTemplate) error {
	glog.V(1).Infof("addChit(%s/%s)", chit.Namespace, chit.Name)
	c.chopConfig.AddChiTemplate((*chop.ClickHouseInstallation)(chit))
	return nil
}

// updateChit sync CHIT which was already created earlier
func (c *Controller) updateChit(old, new *chop.ClickHouseInstallationTemplate) error {
	if old.ObjectMeta.ResourceVersion == new.ObjectMeta.ResourceVersion {
		glog.V(2).Infof("updateChit(%s/%s): ResourceVersion did not change: %s", old.Namespace, old.Name, old.ObjectMeta.ResourceVersion)
		// No need to react
		return nil
	}

	glog.V(2).Infof("updateChit(%s/%s):", new.Namespace, new.Name)
	c.chopConfig.UpdateChiTemplate((*chop.ClickHouseInstallation)(new))
	return nil
}

// deleteChit deletes CHIT
func (c *Controller) deleteChit(chit *chop.ClickHouseInstallationTemplate) error {
	glog.V(2).Infof("deleteChit(%s/%s):", chit.Namespace, chit.Name)
	c.chopConfig.DeleteChiTemplate((*chop.ClickHouseInstallation)(chit))
	return nil
}

// addChopConfig
func (c *Controller) addChopConfig(chopConfig *chop.ClickHouseOperatorConfiguration) error {
	if c.chopConfigManager.IsConfigListed(chopConfig) {
		glog.V(1).Infof("addChopConfig(%s/%s) known config", chopConfig.Namespace, chopConfig.Name)
	} else {
		glog.V(1).Infof("addChopConfig(%s/%s) UNKNOWN CONFIG", chopConfig.Namespace, chopConfig.Name)
		os.Exit(0)
	}

	return nil
}

// updateChopConfig
func (c *Controller) updateChopConfig(old, new *chop.ClickHouseOperatorConfiguration) error {
	if old.ObjectMeta.ResourceVersion == new.ObjectMeta.ResourceVersion {
		glog.V(2).Infof("updateChopConfig(%s/%s): ResourceVersion did not change: %s", old.Namespace, old.Name, old.ObjectMeta.ResourceVersion)
		// No need to react
		return nil
	}

	glog.V(2).Infof("updateChopConfig(%s/%s):", new.Namespace, new.Name)
	os.Exit(0)

	return nil
}

// deleteChit deletes CHIT
func (c *Controller) deleteChopConfig(chopConfig *chop.ClickHouseOperatorConfiguration) error {
	glog.V(2).Infof("deleteChopConfig(%s/%s):", chopConfig.Namespace, chopConfig.Name)
	os.Exit(0)

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
