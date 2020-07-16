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
	log "github.com/golang/glog"
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
	eventBroadcaster.StartLogging(log.Infof)
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

func (c *Controller) initQueues() {
	for i := 0; i < c.chop.Config().ReconcileThreadsNumber; i++ {
		c.queues = append(c.queues, workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), fmt.Sprintf("chi%d", i)))
	}
}

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
			log.V(2).Infof("chiInformer.AddFunc - %s/%s added", chi.Namespace, chi.Name)
			c.enqueueObject(chi.Namespace, chi.Name, NewReconcileChi(reconcileAdd, nil, chi))
		},
		UpdateFunc: func(old, new interface{}) {
			oldChi := old.(*chi.ClickHouseInstallation)
			newChi := new.(*chi.ClickHouseInstallation)
			if !c.chop.Config().IsWatchedNamespace(newChi.Namespace) {
				return
			}
			log.V(2).Info("chiInformer.UpdateFunc")
			c.enqueueObject(newChi.Namespace, newChi.Name, NewReconcileChi(reconcileUpdate, oldChi, newChi))
		},
		DeleteFunc: func(obj interface{}) {
			chi := obj.(*chi.ClickHouseInstallation)
			if !c.chop.Config().IsWatchedNamespace(chi.Namespace) {
				return
			}
			log.V(2).Infof("chiInformer.DeleteFunc - CHI %s/%s deleted", chi.Namespace, chi.Name)
			c.enqueueObject(chi.Namespace, chi.Name, NewReconcileChi(reconcileDelete, chi, nil))
		},
	})

	chopInformerFactory.Clickhouse().V1().ClickHouseInstallationTemplates().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			chit := obj.(*chi.ClickHouseInstallationTemplate)
			if !c.chop.Config().IsWatchedNamespace(chit.Namespace) {
				return
			}
			log.V(2).Infof("chitInformer.AddFunc - %s/%s added", chit.Namespace, chit.Name)
			c.enqueueObject(chit.Namespace, chit.Name, NewReconcileChit(reconcileAdd, nil, chit))
		},
		UpdateFunc: func(old, new interface{}) {
			oldChit := old.(*chi.ClickHouseInstallationTemplate)
			newChit := new.(*chi.ClickHouseInstallationTemplate)
			if !c.chop.Config().IsWatchedNamespace(newChit.Namespace) {
				return
			}
			log.V(2).Infof("chitInformer.UpdateFunc - %s/%s", newChit.Namespace, newChit.Name)
			c.enqueueObject(newChit.Namespace, newChit.Name, NewReconcileChit(reconcileUpdate, oldChit, newChit))
		},
		DeleteFunc: func(obj interface{}) {
			chit := obj.(*chi.ClickHouseInstallationTemplate)
			if !c.chop.Config().IsWatchedNamespace(chit.Namespace) {
				return
			}
			log.V(2).Infof("chitInformer.DeleteFunc - %s/%s deleted", chit.Namespace, chit.Name)
			c.enqueueObject(chit.Namespace, chit.Name, NewReconcileChit(reconcileDelete, chit, nil))
		},
	})

	chopInformerFactory.Clickhouse().V1().ClickHouseOperatorConfigurations().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			chopConfig := obj.(*chi.ClickHouseOperatorConfiguration)
			if !c.chop.Config().IsWatchedNamespace(chopConfig.Namespace) {
				return
			}
			log.V(2).Infof("chopInformer.AddFunc - %s/%s added", chopConfig.Namespace, chopConfig.Name)
			c.enqueueObject(chopConfig.Namespace, chopConfig.Name, NewReconcileChopConfig(reconcileAdd, nil, chopConfig))
		},
		UpdateFunc: func(old, new interface{}) {
			newChopConfig := new.(*chi.ClickHouseOperatorConfiguration)
			oldChopConfig := old.(*chi.ClickHouseOperatorConfiguration)
			if !c.chop.Config().IsWatchedNamespace(newChopConfig.Namespace) {
				return
			}
			log.V(2).Infof("chopInformer.UpdateFunc - %s/%s", newChopConfig.Namespace, newChopConfig.Name)
			c.enqueueObject(newChopConfig.Namespace, newChopConfig.Name, NewReconcileChopConfig(reconcileUpdate, oldChopConfig, newChopConfig))
		},
		DeleteFunc: func(obj interface{}) {
			chopConfig := obj.(*chi.ClickHouseOperatorConfiguration)
			if !c.chop.Config().IsWatchedNamespace(chopConfig.Namespace) {
				return
			}
			log.V(2).Infof("chopInformer.DeleteFunc - %s/%s deleted", chopConfig.Namespace, chopConfig.Name)
			c.enqueueObject(chopConfig.Namespace, chopConfig.Name, NewReconcileChopConfig(reconcileDelete, chopConfig, nil))
		},
	})

	kubeInformerFactory.Core().V1().Services().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			service := obj.(*core.Service)
			if !c.isTrackedObject(&service.ObjectMeta) {
				return
			}
			log.V(2).Infof("serviceInformer AddFunc %s/%s", service.Namespace, service.Name)
		},
		UpdateFunc: func(old, new interface{}) {
			oldService := old.(*core.Service)
			if !c.isTrackedObject(&oldService.ObjectMeta) {
				return
			}
		},
		DeleteFunc: func(obj interface{}) {
			service := obj.(*core.Service)
			if !c.isTrackedObject(&service.ObjectMeta) {
				return
			}
			log.V(2).Infof("serviceInformer DeleteFunc %s/%s", service.Namespace, service.Name)
		},
	})

	kubeInformerFactory.Core().V1().Endpoints().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			endpoints := obj.(*core.Endpoints)
			if !c.isTrackedObject(&endpoints.ObjectMeta) {
				return
			}
			log.V(2).Infof("endpointsInformer AddFunc %s/%s", endpoints.Namespace, endpoints.Name)
		},
		UpdateFunc: func(old, new interface{}) {
			oldEndpoints := old.(*core.Endpoints)
			newEndpoints := new.(*core.Endpoints)
			if !c.isTrackedObject(&oldEndpoints.ObjectMeta) {
				return
			}

			diff, equal := messagediff.DeepDiff(oldEndpoints, newEndpoints)
			if equal {
				log.V(2).Infof("onUpdateEndpoints(%s/%s): no changes found", oldEndpoints.Namespace, oldEndpoints.Name)
				// No need to react
				return
			}

			added := false
			for path := range diff.Added {
				log.V(2).Infof("onUpdateEndpoints(%s/%s): added %v", oldEndpoints.Namespace, oldEndpoints.Name, path)
				for _, pathnode := range *path {
					s := pathnode.String()
					if s == ".Addresses" {
						added = true
					}
				}
			}
			for path := range diff.Removed {
				log.V(2).Infof("onUpdateEndpoints(%s/%s): removed %v", oldEndpoints.Namespace, oldEndpoints.Name, path)
			}
			for path := range diff.Modified {
				log.V(2).Infof("onUpdateEndpoints(%s/%s): modified %v", oldEndpoints.Namespace, oldEndpoints.Name, path)
			}

			if added {
				log.V(1).Infof("endpointsInformer UpdateFunc(%s/%s) IP ASSIGNED %v", newEndpoints.Namespace, newEndpoints.Name, newEndpoints.Subsets)
				c.enqueueObject(newEndpoints.Namespace, newEndpoints.Name, NewDropDns(&newEndpoints.ObjectMeta))
			}
		},
		DeleteFunc: func(obj interface{}) {
			endpoints := obj.(*core.Endpoints)
			if !c.isTrackedObject(&endpoints.ObjectMeta) {
				return
			}
			log.V(2).Infof("endpointsInformer DeleteFunc %s/%s", endpoints.Namespace, endpoints.Name)
		},
	})

	kubeInformerFactory.Core().V1().ConfigMaps().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			configMap := obj.(*core.ConfigMap)
			if !c.isTrackedObject(&configMap.ObjectMeta) {
				return
			}
			log.V(2).Infof("configMapInformer AddFunc %s/%s", configMap.Namespace, configMap.Name)
		},
		UpdateFunc: func(old, new interface{}) {
			configMap := old.(*core.ConfigMap)
			if !c.isTrackedObject(&configMap.ObjectMeta) {
				return
			}
			log.V(2).Infof("configMapInformer UpdateFunc %s/%s", configMap.Namespace, configMap.Name)
		},
		DeleteFunc: func(obj interface{}) {
			configMap := obj.(*core.ConfigMap)
			if !c.isTrackedObject(&configMap.ObjectMeta) {
				return
			}
			log.V(2).Infof("configMapInformer DeleteFunc %s/%s", configMap.Namespace, configMap.Name)
		},
	})

	kubeInformerFactory.Apps().V1().StatefulSets().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			statefulSet := obj.(*apps.StatefulSet)
			if !c.isTrackedObject(&statefulSet.ObjectMeta) {
				return
			}
			log.V(2).Infof("statefulSetInformer AddFunc %s/%s", statefulSet.Namespace, statefulSet.Name)
			//controller.handleObject(obj)
		},
		UpdateFunc: func(old, new interface{}) {
			statefulSet := old.(*apps.StatefulSet)
			if !c.isTrackedObject(&statefulSet.ObjectMeta) {
				return
			}
			log.V(2).Infof("statefulSetInformer UpdateFunc %s/%s", statefulSet.Namespace, statefulSet.Name)
		},
		DeleteFunc: func(obj interface{}) {
			statefulSet := obj.(*apps.StatefulSet)
			if !c.isTrackedObject(&statefulSet.ObjectMeta) {
				return
			}
			log.V(2).Infof("statefulSetInformer DeleteFunc %s/%s", statefulSet.Namespace, statefulSet.Name)
			//controller.handleObject(obj)
		},
	})

	kubeInformerFactory.Core().V1().Pods().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*core.Pod)
			if !c.isTrackedObject(&pod.ObjectMeta) {
				return
			}
			log.V(2).Infof("podInformer AddFunc %s/%s", pod.Namespace, pod.Name)
		},
		UpdateFunc: func(old, new interface{}) {
			pod := old.(*core.Pod)
			if !c.isTrackedObject(&pod.ObjectMeta) {
				return
			}
			log.V(2).Infof("podInformer UpdateFunc %s/%s", pod.Namespace, pod.Name)
		},
		DeleteFunc: func(obj interface{}) {
			pod := obj.(*core.Pod)
			if !c.isTrackedObject(&pod.ObjectMeta) {
				return
			}
			log.V(2).Infof("podInformer DeleteFunc %s/%s", pod.Namespace, pod.Name)
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
	log.V(1).Infof("ClickHouseInstallation controller: starting workers number: %d", workersNum)
	for i := 0; i < workersNum; i++ {
		log.V(1).Infof("ClickHouseInstallation controller: starting worker %d out of %d", i+1, workersNum)
		worker := c.newWorker(c.queues[i])
		go wait.Until(worker.run, runWorkerPeriod, ctx.Done())
	}
	defer log.V(1).Info("ClickHouseInstallation controller: shutting down workers")

	log.V(1).Info("ClickHouseInstallation controller: workers started")
	<-ctx.Done()
}

// enqueueObject adds ClickHouseInstallation object to the workqueue
func (c *Controller) enqueueObject(namespace, name string, obj interface{}) {

	c.queues[util.HashIntoIntTopped([]byte(fmt.Sprintf("%s/%s", namespace, name)), len(c.queues))].AddRateLimited(obj)
}

func (c *Controller) updateWatch(namespace, name string, hostnames []string) {
	go c.updateWatchAsync(namespace, name, hostnames)
}

func (c *Controller) updateWatchAsync(namespace, name string, hostnames []string) {
	if err := metrics.InformMetricsExporterAboutWatchedCHI(namespace, name, hostnames); err != nil {
		log.V(1).Infof("FAIL update watch (%s/%s): %q", namespace, name, err)
	} else {
		log.V(2).Infof("OK update watch (%s/%s)", namespace, name)
	}
}

func (c *Controller) deleteWatch(namespace, name string) {
	go c.deleteWatchAsync(namespace, name)
}

func (c *Controller) deleteWatchAsync(namespace, name string) {
	if err := metrics.InformMetricsExporterToDeleteWatchedCHI(namespace, name); err != nil {
		log.V(1).Infof("FAIL delete watch (%s/%s): %q", namespace, name, err)
	} else {
		log.V(2).Infof("OK delete watch (%s/%s)", namespace, name)
	}
}

// addChit sync new CHIT - creates all its resources
func (c *Controller) addChit(chit *chi.ClickHouseInstallationTemplate) error {
	log.V(1).Infof("addChit(%s/%s)", chit.Namespace, chit.Name)
	c.chop.Config().AddCHITemplate((*chi.ClickHouseInstallation)(chit))
	return nil
}

// updateChit sync CHIT which was already created earlier
func (c *Controller) updateChit(old, new *chi.ClickHouseInstallationTemplate) error {
	if old.ObjectMeta.ResourceVersion == new.ObjectMeta.ResourceVersion {
		log.V(2).Infof("updateChit(%s/%s): ResourceVersion did not change: %s", old.Namespace, old.Name, old.ObjectMeta.ResourceVersion)
		// No need to react
		return nil
	}

	log.V(2).Infof("updateChit(%s/%s):", new.Namespace, new.Name)
	c.chop.Config().UpdateCHITemplate((*chi.ClickHouseInstallation)(new))
	return nil
}

// deleteChit deletes CHIT
func (c *Controller) deleteChit(chit *chi.ClickHouseInstallationTemplate) error {
	log.V(2).Infof("deleteChit(%s/%s):", chit.Namespace, chit.Name)
	c.chop.Config().DeleteCHITemplate((*chi.ClickHouseInstallation)(chit))
	return nil
}

// addChopConfig
func (c *Controller) addChopConfig(chopConfig *chi.ClickHouseOperatorConfiguration) error {
	if c.chop.ConfigManager.IsConfigListed(chopConfig) {
		log.V(1).Infof("addChopConfig(%s/%s) already known config - do nothing", chopConfig.Namespace, chopConfig.Name)
	} else {
		log.V(1).Infof("addChopConfig(%s/%s) new, previously unknown config, need to apply", chopConfig.Namespace, chopConfig.Name)
		// TODO
		// NEED REFACTORING
		// os.Exit(0)
	}

	return nil
}

// updateChopConfig
func (c *Controller) updateChopConfig(old, new *chi.ClickHouseOperatorConfiguration) error {
	if old.ObjectMeta.ResourceVersion == new.ObjectMeta.ResourceVersion {
		log.V(2).Infof("updateChopConfig(%s/%s): ResourceVersion did not change: %s", old.Namespace, old.Name, old.ObjectMeta.ResourceVersion)
		// No need to react
		return nil
	}

	log.V(2).Infof("updateChopConfig(%s/%s):", new.Namespace, new.Name)
	// TODO
	// NEED REFACTORING
	//os.Exit(0)

	return nil
}

// deleteChit deletes CHIT
func (c *Controller) deleteChopConfig(chopConfig *chi.ClickHouseOperatorConfiguration) error {
	log.V(2).Infof("deleteChopConfig(%s/%s):", chopConfig.Namespace, chopConfig.Name)
	// TODO
	// NEED REFACTORING
	//os.Exit(0)

	return nil
}

// updateCHIObject updates ClickHouseInstallation object
func (c *Controller) updateCHIObject(chi *chi.ClickHouseInstallation) error {
	namespace, name := NamespaceName(chi.ObjectMeta)
	new, err := c.chopClient.ClickhouseV1().ClickHouseInstallations(namespace).Update(chi)

	if err != nil {
		// Error update
		log.V(1).Infof("ERROR update CHI (%s/%s): %q", namespace, name, err)
		return err
	}

	if chi.ObjectMeta.ResourceVersion != new.ObjectMeta.ResourceVersion {
		// Updated
		log.V(2).Infof("updateCHIObject(%s/%s): ResourceVersion bump %s=>%s",
			namespace, name, chi.ObjectMeta.ResourceVersion, new.ObjectMeta.ResourceVersion,
		)
		chi.ObjectMeta.ResourceVersion = new.ObjectMeta.ResourceVersion
		return nil
	}

	// ResourceVersion not changed - no update performed?

	return nil
}

// updateCHIObjectStatus updates ClickHouseInstallation object's Status
func (c *Controller) updateCHIObjectStatus(chi *chi.ClickHouseInstallation, tolerateAbsence bool) error {
	namespace, name := NamespaceName(chi.ObjectMeta)
	log.V(2).Infof("Update CHI status (%s/%s)", namespace, name)

	cur, err := c.chopClient.ClickhouseV1().ClickHouseInstallations(namespace).Get(name, newGetOptions())
	if err != nil {
		if tolerateAbsence {
			return nil
		}
		log.V(1).Infof("ERROR GetCHI (%s/%s): %q", namespace, name, err)
		return err
	}
	if cur == nil {
		if tolerateAbsence {
			return nil
		}
		log.V(1).Infof("ERROR GetCHI (%s/%s): NULL returned", namespace, name)
		return fmt.Errorf("ERROR GetCHI (%s/%s): NULL returned", namespace, name)
	}

	// Update status of a real object
	cur.Status = chi.Status
	return c.updateCHIObject(cur)
}

func (c *Controller) installFinalizer(chi *chi.ClickHouseInstallation) error {
	namespace, name := NamespaceName(chi.ObjectMeta)
	log.V(2).Infof("Update CHI status (%s/%s)", namespace, name)

	cur, err := c.chopClient.ClickhouseV1().ClickHouseInstallations(namespace).Get(name, newGetOptions())
	if err != nil {
		return err
	}
	if cur == nil {
		return fmt.Errorf("ERROR GetCHI (%s/%s): NULL returned", namespace, name)
	}

	cur.ObjectMeta.Finalizers = append(cur.ObjectMeta.Finalizers, FinalizerName)

	return c.updateCHIObject(cur)
}

func (c *Controller) uninstallFinalizer(chi *chi.ClickHouseInstallation) error {
	namespace, name := NamespaceName(chi.ObjectMeta)
	log.V(2).Infof("Update CHI status (%s/%s)", namespace, name)

	cur, err := c.chopClient.ClickhouseV1().ClickHouseInstallations(namespace).Get(name, newGetOptions())
	if err != nil {
		return err
	}
	if cur == nil {
		return fmt.Errorf("ERROR GetCHI (%s/%s): NULL returned", namespace, name)
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

	log.V(1).Infof("Processing object: %s", object.GetName())

	// Get owner - it is expected to be CHI
	chi, err := c.chiLister.ClickHouseInstallations(object.GetNamespace()).Get(ownerRef.Name)

	if err != nil {
		log.V(1).Infof("ignoring orphaned object '%s' of ClickHouseInstallation '%s'", object.GetSelfLink(), ownerRef.Name)
		return
	}

	// Add CHI object into reconcile loop
	c.enqueueObject(chi.Namespace, chi.Name, chi)
}

// waitForCacheSync is a logger-wrapper over cache.WaitForCacheSync() and it waits for caches to populate
func waitForCacheSync(name string, stopCh <-chan struct{}, cacheSyncs ...cache.InformerSynced) bool {
	log.V(1).Infof("Syncing caches for %s controller", name)
	if !cache.WaitForCacheSync(stopCh, cacheSyncs...) {
		utilruntime.HandleError(fmt.Errorf(messageUnableToSync, name))
		return false
	}
	log.V(1).Infof("Caches are synced for %s controller", name)
	return true
}
