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

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/metrics"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	chopclientset "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	chopclientsetscheme "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned/scheme"
	chopinformers "github.com/altinity/clickhouse-operator/pkg/client/informers/externalversions"
	chopmodels "github.com/altinity/clickhouse-operator/pkg/model"
	"github.com/altinity/clickhouse-operator/pkg/util"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
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
	// log "k8s.io/klog"
)

// NewController creates instance of Controller
func NewController(
	chop *chop.CHOp,
	chopClient chopclientset.Interface,
	kubeClient kube.Interface,
	chopInformerFactory chopinformers.SharedInformerFactory,
	kubeInformerFactory kubeinformers.SharedInformerFactory,
) *Controller {

	// Initializations
	_ = chopclientsetscheme.AddToScheme(scheme.Scheme)

	// Setup events
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(log.Info)
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
		chop:                    chop,
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
		recorder:                recorder,
	}
	controller.initQueues()
	controller.addEventHandlers(chopInformerFactory, kubeInformerFactory)

	return controller
}

// initQueues
func (c *Controller) initQueues() {
	for i := 0; i < c.chop.Config().ReconcileThreadsNumber+chi.DefaultReconcileSystemThreadsNumber; i++ {
		c.queues = append(
			c.queues,
			workqueue.NewNamedRateLimitingQueue(
				workqueue.DefaultControllerRateLimiter(),
				fmt.Sprintf("chi%d", i),
			),
		)
	}
}

// addEventHandlers
func (c *Controller) addEventHandlers(
	chopInformerFactory chopinformers.SharedInformerFactory,
	kubeInformerFactory kubeinformers.SharedInformerFactory,
) {
	chopInformerFactory.Clickhouse().V1().ClickHouseInstallations().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			chi := obj.(*chi.ClickHouseInstallation)
			if !c.chop.Config().IsWatchedNamespace(chi.Namespace) {
				return
			}
			log.V(2).M(chi).Info("chiInformer.AddFunc")
			c.enqueueObject(chi.Namespace, chi.Name, NewReconcileChi(reconcileAdd, nil, chi))
		},
		UpdateFunc: func(old, new interface{}) {
			oldChi := old.(*chi.ClickHouseInstallation)
			newChi := new.(*chi.ClickHouseInstallation)
			if !c.chop.Config().IsWatchedNamespace(newChi.Namespace) {
				return
			}
			log.V(2).M(newChi).Info("chiInformer.UpdateFunc")
			c.enqueueObject(newChi.Namespace, newChi.Name, NewReconcileChi(reconcileUpdate, oldChi, newChi))
		},
		DeleteFunc: func(obj interface{}) {
			chi := obj.(*chi.ClickHouseInstallation)
			if !c.chop.Config().IsWatchedNamespace(chi.Namespace) {
				return
			}
			log.V(2).M(chi).Info("chiInformer.DeleteFunc")
			c.enqueueObject(chi.Namespace, chi.Name, NewReconcileChi(reconcileDelete, chi, nil))
		},
	})

	chopInformerFactory.Clickhouse().V1().ClickHouseInstallationTemplates().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			chit := obj.(*chi.ClickHouseInstallationTemplate)
			if !c.chop.Config().IsWatchedNamespace(chit.Namespace) {
				return
			}
			log.V(2).M(chit).Info("chitInformer.AddFunc")
			c.enqueueObject(chit.Namespace, chit.Name, NewReconcileChit(reconcileAdd, nil, chit))
		},
		UpdateFunc: func(old, new interface{}) {
			oldChit := old.(*chi.ClickHouseInstallationTemplate)
			newChit := new.(*chi.ClickHouseInstallationTemplate)
			if !c.chop.Config().IsWatchedNamespace(newChit.Namespace) {
				return
			}
			log.V(2).M(newChit).Info("chitInformer.UpdateFunc")
			c.enqueueObject(newChit.Namespace, newChit.Name, NewReconcileChit(reconcileUpdate, oldChit, newChit))
		},
		DeleteFunc: func(obj interface{}) {
			chit := obj.(*chi.ClickHouseInstallationTemplate)
			if !c.chop.Config().IsWatchedNamespace(chit.Namespace) {
				return
			}
			log.V(2).M(chit).Info("chitInformer.DeleteFunc")
			c.enqueueObject(chit.Namespace, chit.Name, NewReconcileChit(reconcileDelete, chit, nil))
		},
	})

	chopInformerFactory.Clickhouse().V1().ClickHouseOperatorConfigurations().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			chopConfig := obj.(*chi.ClickHouseOperatorConfiguration)
			if !c.chop.Config().IsWatchedNamespace(chopConfig.Namespace) {
				return
			}
			log.V(2).M(chopConfig).Info("chopInformer.AddFunc")
			c.enqueueObject(chopConfig.Namespace, chopConfig.Name, NewReconcileChopConfig(reconcileAdd, nil, chopConfig))
		},
		UpdateFunc: func(old, new interface{}) {
			newChopConfig := new.(*chi.ClickHouseOperatorConfiguration)
			oldChopConfig := old.(*chi.ClickHouseOperatorConfiguration)
			if !c.chop.Config().IsWatchedNamespace(newChopConfig.Namespace) {
				return
			}
			log.V(2).M(newChopConfig).Info("chopInformer.UpdateFunc")
			c.enqueueObject(newChopConfig.Namespace, newChopConfig.Name, NewReconcileChopConfig(reconcileUpdate, oldChopConfig, newChopConfig))
		},
		DeleteFunc: func(obj interface{}) {
			chopConfig := obj.(*chi.ClickHouseOperatorConfiguration)
			if !c.chop.Config().IsWatchedNamespace(chopConfig.Namespace) {
				return
			}
			log.V(2).M(chopConfig).Info("chopInformer.DeleteFunc")
			c.enqueueObject(chopConfig.Namespace, chopConfig.Name, NewReconcileChopConfig(reconcileDelete, chopConfig, nil))
		},
	})

	kubeInformerFactory.Core().V1().Services().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			service := obj.(*core.Service)
			if !c.isTrackedObject(&service.ObjectMeta) {
				return
			}
			log.V(3).M(service).Info("serviceInformer.AddFunc")
		},
		UpdateFunc: func(old, new interface{}) {
			oldService := old.(*core.Service)
			if !c.isTrackedObject(&oldService.ObjectMeta) {
				return
			}
			log.V(3).M(oldService).Info("serviceInformer.UpdateFunc")
		},
		DeleteFunc: func(obj interface{}) {
			service := obj.(*core.Service)
			if !c.isTrackedObject(&service.ObjectMeta) {
				return
			}
			log.V(3).M(service).Info("serviceInformer.DeleteFunc")
		},
	})

	kubeInformerFactory.Core().V1().Endpoints().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			endpoints := obj.(*core.Endpoints)
			if !c.isTrackedObject(&endpoints.ObjectMeta) {
				return
			}
			log.V(3).M(endpoints).Info("endpointsInformer.AddFunc")
		},
		UpdateFunc: func(old, new interface{}) {
			oldEndpoints := old.(*core.Endpoints)
			newEndpoints := new.(*core.Endpoints)
			if !c.isTrackedObject(&oldEndpoints.ObjectMeta) {
				return
			}

			diff, equal := messagediff.DeepDiff(oldEndpoints, newEndpoints)
			if equal {
				log.V(2).M(oldEndpoints).Info("endpointsInformer.UpdateFunc: no changes found")
				// No need to react
				return
			}

			added := false
			for path := range diff.Added {
				log.V(2).M(oldEndpoints).Info("endpointsInformer.UpdateFunc: added %v", path)
				for _, pathnode := range *path {
					s := pathnode.String()
					if s == ".Addresses" {
						added = true
					}
				}
			}
			for path := range diff.Removed {
				log.V(2).M(oldEndpoints).Info("endpointsInformer.UpdateFunc: removed %v", path)
			}
			for path := range diff.Modified {
				log.V(2).M(oldEndpoints).Info("endpointsInformer.UpdateFunc: modified %v", path)
			}

			if added {
				log.V(1).M(oldEndpoints).Info("endpointsInformer.UpdateFunc: IP ASSIGNED %v", newEndpoints.Subsets)
				c.enqueueObject(newEndpoints.Namespace, newEndpoints.Name, NewDropDns(&newEndpoints.ObjectMeta))
			}
		},
		DeleteFunc: func(obj interface{}) {
			endpoints := obj.(*core.Endpoints)
			if !c.isTrackedObject(&endpoints.ObjectMeta) {
				return
			}
			log.V(2).M(endpoints).Info("endpointsInformer.DeleteFunc")
		},
	})

	kubeInformerFactory.Core().V1().ConfigMaps().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			configMap := obj.(*core.ConfigMap)
			if !c.isTrackedObject(&configMap.ObjectMeta) {
				return
			}
			log.V(3).M(configMap).Info("configMapInformer.AddFunc")
		},
		UpdateFunc: func(old, new interface{}) {
			configMap := old.(*core.ConfigMap)
			if !c.isTrackedObject(&configMap.ObjectMeta) {
				return
			}
			log.V(3).M(configMap).Info("configMapInformer.UpdateFunc")
		},
		DeleteFunc: func(obj interface{}) {
			configMap := obj.(*core.ConfigMap)
			if !c.isTrackedObject(&configMap.ObjectMeta) {
				return
			}
			log.V(3).M(configMap).Info("configMapInformer.DeleteFunc")
		},
	})

	kubeInformerFactory.Apps().V1().StatefulSets().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			statefulSet := obj.(*apps.StatefulSet)
			if !c.isTrackedObject(&statefulSet.ObjectMeta) {
				return
			}
			log.V(3).M(statefulSet).Info("statefulSetInformer.AddFunc")
			//controller.handleObject(obj)
		},
		UpdateFunc: func(old, new interface{}) {
			statefulSet := old.(*apps.StatefulSet)
			if !c.isTrackedObject(&statefulSet.ObjectMeta) {
				return
			}
			log.V(3).M(statefulSet).Info("statefulSetInformer.UpdateFunc")
		},
		DeleteFunc: func(obj interface{}) {
			statefulSet := obj.(*apps.StatefulSet)
			if !c.isTrackedObject(&statefulSet.ObjectMeta) {
				return
			}
			log.V(3).M(statefulSet).Info("statefulSetInformer.DeleteFunc")
			//controller.handleObject(obj)
		},
	})

	kubeInformerFactory.Core().V1().Pods().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*core.Pod)
			if !c.isTrackedObject(&pod.ObjectMeta) {
				return
			}
			log.V(3).M(pod).Info("podInformer.AddFunc")
		},
		UpdateFunc: func(old, new interface{}) {
			pod := old.(*core.Pod)
			if !c.isTrackedObject(&pod.ObjectMeta) {
				return
			}
			log.V(3).M(pod).Info("podInformer.UpdateFunc")
		},
		DeleteFunc: func(obj interface{}) {
			pod := obj.(*core.Pod)
			if !c.isTrackedObject(&pod.ObjectMeta) {
				return
			}
			log.V(3).M(pod).Info("podInformer.DeleteFunc")
		},
	})
}

// isTrackedObject checks whether operator is interested in changes of this object
func (c *Controller) isTrackedObject(objectMeta *meta.ObjectMeta) bool {
	return c.chop.Config().IsWatchedNamespace(objectMeta.Namespace) && chopmodels.IsCHOPGeneratedObject(objectMeta)
}

// Run syncs caches, starts workers
func (c *Controller) Run(ctx context.Context) {
	defer utilruntime.HandleCrash()
	defer func() {
		for i := range c.queues {
			c.queues[i].ShutDown()
		}
	}()

	log.V(1).Info("Starting ClickHouseInstallation controller")
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
	workersNum := len(c.queues)
	log.V(1).A().Info("ClickHouseInstallation controller: starting workers number: %d", workersNum)
	for i := 0; i < workersNum; i++ {
		log.V(1).A().Info("ClickHouseInstallation controller: starting worker %d out of %d", i+1, workersNum)
		worker := c.newWorker(c.queues[i])
		go wait.Until(worker.run, runWorkerPeriod, ctx.Done())
	}
	defer log.V(1).A().Info("ClickHouseInstallation controller: shutting down workers")

	log.V(1).A().Info("ClickHouseInstallation controller: workers started")
	<-ctx.Done()
}

// enqueueObject adds ClickHouseInstallation object to the work queue
func (c *Controller) enqueueObject(namespace, name string, obj interface{}) {
	uid := []byte(fmt.Sprintf("%s/%s", namespace, name))
	index := 0
	switch obj.(type) {
	case *ReconcileChi:
		variants := len(c.queues) - chi.DefaultReconcileSystemThreadsNumber
		index = chi.DefaultReconcileSystemThreadsNumber + util.HashIntoIntTopped(uid, variants)
		break

	case *ReconcileChit:
	case *ReconcileChopConfig:
	case *DropDns:
		variants := chi.DefaultReconcileSystemThreadsNumber
		index = util.HashIntoIntTopped(uid, variants)
		break
	}
	c.queues[index].AddRateLimited(obj)
}

// updateWatch
func (c *Controller) updateWatch(namespace, name string, hostnames []string) {
	go c.updateWatchAsync(namespace, name, hostnames)
}

// updateWatchAsync
func (c *Controller) updateWatchAsync(namespace, name string, hostnames []string) {
	if err := metrics.InformMetricsExporterAboutWatchedCHI(namespace, name, hostnames); err != nil {
		log.V(1).A().Info("FAIL update watch (%s/%s): %q", namespace, name, err)
	} else {
		log.V(2).Info("OK update watch (%s/%s)", namespace, name)
	}
}

// deleteWatch
func (c *Controller) deleteWatch(namespace, name string) {
	go c.deleteWatchAsync(namespace, name)
}

// deleteWatchAsync
func (c *Controller) deleteWatchAsync(namespace, name string) {
	if err := metrics.InformMetricsExporterToDeleteWatchedCHI(namespace, name); err != nil {
		log.V(1).A().Info("FAIL delete watch (%s/%s): %q", namespace, name, err)
	} else {
		log.V(2).Info("OK delete watch (%s/%s)", namespace, name)
	}
}

// addChit sync new CHIT - creates all its resources
func (c *Controller) addChit(chit *chi.ClickHouseInstallationTemplate) error {
	log.V(1).M(chit).F().P()
	c.chop.Config().AddCHITemplate((*chi.ClickHouseInstallation)(chit))
	return nil
}

// updateChit sync CHIT which was already created earlier
func (c *Controller) updateChit(old, new *chi.ClickHouseInstallationTemplate) error {
	if old.ObjectMeta.ResourceVersion == new.ObjectMeta.ResourceVersion {
		log.V(2).M(old).F().Info("ResourceVersion did not change: %s", old.ObjectMeta.ResourceVersion)
		// No need to react
		return nil
	}

	log.V(2).M(new).F().Info("ResourceVersion change: %s to %s", old.ObjectMeta.ResourceVersion, new.ObjectMeta.ResourceVersion)
	c.chop.Config().UpdateCHITemplate((*chi.ClickHouseInstallation)(new))
	return nil
}

// deleteChit deletes CHIT
func (c *Controller) deleteChit(chit *chi.ClickHouseInstallationTemplate) error {
	log.V(2).M(chit).F().P()
	c.chop.Config().DeleteCHITemplate((*chi.ClickHouseInstallation)(chit))
	return nil
}

// addChopConfig
func (c *Controller) addChopConfig(chopConfig *chi.ClickHouseOperatorConfiguration) error {
	if c.chop.ConfigManager.IsConfigListed(chopConfig) {
		log.V(1).M(chopConfig).F().Info("already known config - do nothing")
	} else {
		log.V(1).M(chopConfig).F().Info("new, previously unknown config, need to apply")
		// TODO
		// NEED REFACTORING
		// os.Exit(0)
	}

	return nil
}

// updateChopConfig
func (c *Controller) updateChopConfig(old, new *chi.ClickHouseOperatorConfiguration) error {
	if old.ObjectMeta.ResourceVersion == new.ObjectMeta.ResourceVersion {
		log.V(2).M(old).F().Info("ResourceVersion did not change: %s", old.ObjectMeta.ResourceVersion)
		// No need to react
		return nil
	}

	log.V(2).M(new).F().Info("ResourceVersion change: %s to %s", old.ObjectMeta.ResourceVersion, new.ObjectMeta.ResourceVersion)
	// TODO
	// NEED REFACTORING
	//os.Exit(0)

	return nil
}

// deleteChit deletes CHIT
func (c *Controller) deleteChopConfig(chopConfig *chi.ClickHouseOperatorConfiguration) error {
	log.V(2).M(chopConfig).F().P()
	// TODO
	// NEED REFACTORING
	//os.Exit(0)

	return nil
}

// updateCHIObject updates ClickHouseInstallation object
func (c *Controller) updateCHIObject(chi *chi.ClickHouseInstallation) error {
	new, err := c.chopClient.ClickhouseV1().ClickHouseInstallations(chi.ObjectMeta.Namespace).Update(chi)
	if err != nil {
		// Error update
		log.V(1).M(chi).A().Error("%q", err)
		return err
	}

	if chi.ObjectMeta.ResourceVersion != new.ObjectMeta.ResourceVersion {
		// Updated
		log.V(2).M(chi).F().Info("ResourceVersion change: %s to %s", chi.ObjectMeta.ResourceVersion, new.ObjectMeta.ResourceVersion)
		chi.ObjectMeta.ResourceVersion = new.ObjectMeta.ResourceVersion
		return nil
	}

	// ResourceVersion not changed - no update performed?

	return nil
}

// updateCHIObjectStatus updates ClickHouseInstallation object's Status
func (c *Controller) updateCHIObjectStatus(chi *chi.ClickHouseInstallation, tolerateAbsence bool) error {
	namespace, name := util.NamespaceName(chi.ObjectMeta)
	log.V(2).M(chi).F().Info("Update CHI status")

	cur, err := c.chopClient.ClickhouseV1().ClickHouseInstallations(namespace).Get(name, newGetOptions())
	if err != nil {
		if tolerateAbsence {
			return nil
		}
		log.V(1).M(chi).A().Error("%q", err)
		return err
	}
	if cur == nil {
		if tolerateAbsence {
			return nil
		}
		log.V(1).M(chi).A().Error("NULL returned")
		return fmt.Errorf("ERROR GetCHI (%s/%s): NULL returned", namespace, name)
	}

	// Update status of a real object
	cur.Status = chi.Status
	return c.updateCHIObject(cur)
}

// installFinalizer
func (c *Controller) installFinalizer(chi *chi.ClickHouseInstallation) error {
	log.V(2).M(chi).S().P()
	defer log.V(2).M(chi).E().P()

	cur, err := c.chopClient.ClickhouseV1().ClickHouseInstallations(chi.Namespace).Get(chi.Name, newGetOptions())
	if err != nil {
		return err
	}
	if cur == nil {
		return fmt.Errorf("ERROR GetCHI (%s/%s): NULL returned", chi.Namespace, chi.Name)
	}

	if util.InArray(FinalizerName, cur.ObjectMeta.Finalizers) {
		// Already installed
		return nil
	}

	cur.ObjectMeta.Finalizers = append(cur.ObjectMeta.Finalizers, FinalizerName)
	return c.updateCHIObject(cur)
}

// uninstallFinalizer
func (c *Controller) uninstallFinalizer(chi *chi.ClickHouseInstallation) error {
	log.V(2).M(chi).S().P()
	defer log.V(2).M(chi).E().P()

	cur, err := c.chopClient.ClickhouseV1().ClickHouseInstallations(chi.Namespace).Get(chi.Name, newGetOptions())
	if err != nil {
		return err
	}
	if cur == nil {
		return fmt.Errorf("ERROR GetCHI (%s/%s): NULL returned", chi.Namespace, chi.Name)
	}

	cur.ObjectMeta.Finalizers = util.RemoveFromArray(FinalizerName, cur.ObjectMeta.Finalizers)

	return c.updateCHIObject(cur)
}

// handleObject enqueues CHI which is owner of `obj` into reconcile loop
func (c *Controller) handleObject(obj interface{}) {
	// TODO review
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
	if ownerRef.Kind != chi.ClickHouseInstallationCRDResourceKind {
		return
	}

	log.V(1).Info("Processing object: %s", object.GetName())

	// Get owner - it is expected to be CHI
	chi, err := c.chiLister.ClickHouseInstallations(object.GetNamespace()).Get(ownerRef.Name)

	if err != nil {
		log.V(1).Info("ignoring orphaned object '%s' of ClickHouseInstallation '%s'", object.GetSelfLink(), ownerRef.Name)
		return
	}

	// Add CHI object into reconcile loop
	c.enqueueObject(chi.Namespace, chi.Name, chi)
}

// waitForCacheSync is a logger-wrapper over cache.WaitForCacheSync() and it waits for caches to populate
func waitForCacheSync(name string, stopCh <-chan struct{}, cacheSyncs ...cache.InformerSynced) bool {
	log.V(1).F().Info("Syncing caches for %s controller", name)
	if !cache.WaitForCacheSync(stopCh, cacheSyncs...) {
		utilruntime.HandleError(fmt.Errorf(messageUnableToSync, name))
		return false
	}
	log.V(1).F().Info("Caches are synced for %s controller", name)
	return true
}
