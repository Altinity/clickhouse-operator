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
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/sanity-io/litter"
	"gopkg.in/d4l3k/messagediff.v1"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	apiExtensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeTypes "k8s.io/apimachinery/pkg/types"
	utilRuntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeInformers "k8s.io/client-go/informers"
	kube "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedCore "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"

	"github.com/altinity/queue"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/metrics"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	chopClientSet "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	chopClientSetScheme "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned/scheme"
	chopInformers "github.com/altinity/clickhouse-operator/pkg/client/informers/externalversions"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi/cmd_queue"
	chiKube "github.com/altinity/clickhouse-operator/pkg/controller/chi/kube"
	ctrlLabeler "github.com/altinity/clickhouse-operator/pkg/controller/chi/labeler"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/metrics/clickhouse"
	chiLabeler "github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	"github.com/altinity/clickhouse-operator/pkg/model/common/action_plan"
	"github.com/altinity/clickhouse-operator/pkg/model/common/volume"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Controller defines CRO controller
type Controller struct {
	// kube is a generalized kube client
	kube interfaces.IKube

	//
	// Native clients
	//
	kubeClient kube.Interface
	extClient  apiExtensions.Interface
	chopClient chopClientSet.Interface

	// queues used to organize events queue processed by the operator
	queues []queue.PriorityQueue
	// not used explicitly
	recorder record.EventRecorder

	namer       interfaces.INameManager
	ctrlLabeler *ctrlLabeler.Labeler
	pvcDeleter  *volume.PVCDeleter
}

// NewController creates instance of Controller
func NewController(
	chopClient chopClientSet.Interface,
	extClient apiExtensions.Interface,
	kubeClient kube.Interface,
	chopInformerFactory chopInformers.SharedInformerFactory,
	kubeInformerFactory kubeInformers.SharedInformerFactory,
) *Controller {

	// Initializations
	_ = chopClientSetScheme.AddToScheme(scheme.Scheme)

	// Setup events
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(log.Info)
	eventBroadcaster.StartRecordingToSink(
		&typedCore.EventSinkImpl{
			Interface: kubeClient.CoreV1().Events(""),
		},
	)
	recorder := eventBroadcaster.NewRecorder(
		scheme.Scheme,
		core.EventSource{
			Component: componentName,
		},
	)

	namer := managers.NewNameManager(managers.NameManagerTypeClickHouse)
	kube := chiKube.NewAdapter(kubeClient, chopClient, namer)

	// Create Controller instance
	controller := &Controller{
		kubeClient:  kubeClient,
		extClient:   extClient,
		chopClient:  chopClient,
		recorder:    recorder,
		namer:       namer,
		kube:        kube,
		ctrlLabeler: ctrlLabeler.New(kube),
		pvcDeleter:  volume.NewPVCDeleter(managers.NewNameManager(managers.NameManagerTypeClickHouse)),
	}
	controller.initQueues()
	controller.addEventHandlers(chopInformerFactory, kubeInformerFactory)

	return controller
}

// initQueues
func (c *Controller) initQueues() {
	for i := 0; i < c.getQueuesNum(); i++ {
		c.queues = append(c.queues, c.createQueue())
	}
}

func (c *Controller) getQueuesNum() int {
	return chop.Config().Reconcile.Runtime.ReconcileCHIsThreadsNumber + api.DefaultReconcileSystemThreadsNumber
}

func (c *Controller) createQueue() queue.PriorityQueue {
	return queue.New()
	//workqueue.NewNamedRateLimitingQueue(
	//	workqueue.DefaultControllerRateLimiter(),
	//	fmt.Sprintf("chi%d", i),
	//),
}

func (c *Controller) addEventHandlersCHI(
	chopInformerFactory chopInformers.SharedInformerFactory,
) {
	chopInformerFactory.Clickhouse().V1().ClickHouseInstallations().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			chi := obj.(*api.ClickHouseInstallation)
			if !shouldEnqueue(chi) {
				return
			}
			log.V(3).M(chi).Info("chiInformer.AddFunc")
			c.enqueueObject(cmd_queue.NewReconcileCHI(cmd_queue.ReconcileAdd, nil, chi))
		},
		UpdateFunc: func(old, new interface{}) {
			oldChi := old.(*api.ClickHouseInstallation)
			newChi := new.(*api.ClickHouseInstallation)
			if !shouldEnqueue(newChi) {
				return
			}
			log.V(3).M(newChi).Info("chiInformer.UpdateFunc")
			c.enqueueObject(cmd_queue.NewReconcileCHI(cmd_queue.ReconcileUpdate, oldChi, newChi))
		},
		DeleteFunc: func(obj interface{}) {
			chi := obj.(*api.ClickHouseInstallation)
			if !chop.Config().IsNamespaceWatched(chi.Namespace) {
				return
			}
			log.V(3).M(chi).Info("chiInformer.DeleteFunc")
			c.enqueueObject(cmd_queue.NewReconcileCHI(cmd_queue.ReconcileDelete, chi, nil))
		},
	})
}

func (c *Controller) addEventHandlersCHIT(
	chopInformerFactory chopInformers.SharedInformerFactory,
) {
	chopInformerFactory.Clickhouse().V1().ClickHouseInstallationTemplates().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			chit := obj.(*api.ClickHouseInstallationTemplate)
			if !chop.Config().IsNamespaceWatched(chit.Namespace) {
				return
			}
			log.V(3).M(chit).Info("chitInformer.AddFunc")
			c.enqueueObject(cmd_queue.NewReconcileCHIT(cmd_queue.ReconcileAdd, nil, chit))
		},
		UpdateFunc: func(old, new interface{}) {
			oldChit := old.(*api.ClickHouseInstallationTemplate)
			newChit := new.(*api.ClickHouseInstallationTemplate)
			if !chop.Config().IsNamespaceWatched(newChit.Namespace) {
				return
			}
			log.V(3).M(newChit).Info("chitInformer.UpdateFunc")
			c.enqueueObject(cmd_queue.NewReconcileCHIT(cmd_queue.ReconcileUpdate, oldChit, newChit))
		},
		DeleteFunc: func(obj interface{}) {
			chit := obj.(*api.ClickHouseInstallationTemplate)
			if !chop.Config().IsNamespaceWatched(chit.Namespace) {
				return
			}
			log.V(3).M(chit).Info("chitInformer.DeleteFunc")
			c.enqueueObject(cmd_queue.NewReconcileCHIT(cmd_queue.ReconcileDelete, chit, nil))
		},
	})
}

func (c *Controller) addEventHandlersChopConfig(
	chopInformerFactory chopInformers.SharedInformerFactory,
) {
	chopInformerFactory.Clickhouse().V1().ClickHouseOperatorConfigurations().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			chopConfig := obj.(*api.ClickHouseOperatorConfiguration)
			if !chop.Config().IsNamespaceWatched(chopConfig.Namespace) {
				return
			}
			log.V(3).M(chopConfig).Info("chopInformer.AddFunc")
			c.enqueueObject(cmd_queue.NewReconcileChopConfig(cmd_queue.ReconcileAdd, nil, chopConfig))
		},
		UpdateFunc: func(old, new interface{}) {
			newChopConfig := new.(*api.ClickHouseOperatorConfiguration)
			oldChopConfig := old.(*api.ClickHouseOperatorConfiguration)
			if !chop.Config().IsNamespaceWatched(newChopConfig.Namespace) {
				return
			}
			log.V(3).M(newChopConfig).Info("chopInformer.UpdateFunc")
			c.enqueueObject(cmd_queue.NewReconcileChopConfig(cmd_queue.ReconcileUpdate, oldChopConfig, newChopConfig))
		},
		DeleteFunc: func(obj interface{}) {
			chopConfig := obj.(*api.ClickHouseOperatorConfiguration)
			if !chop.Config().IsNamespaceWatched(chopConfig.Namespace) {
				return
			}
			log.V(3).M(chopConfig).Info("chopInformer.DeleteFunc")
			c.enqueueObject(cmd_queue.NewReconcileChopConfig(cmd_queue.ReconcileDelete, chopConfig, nil))
		},
	})
}

func (c *Controller) addEventHandlersService(
	kubeInformerFactory kubeInformers.SharedInformerFactory,
) {
	kubeInformerFactory.Core().V1().Services().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			service := obj.(*core.Service)
			if !c.isTrackedObject(service.GetObjectMeta()) {
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
}

func normalizeEndpoints(e *core.Endpoints) *core.Endpoints {
	if e == nil {
		e = &core.Endpoints{}
	}
	if len(e.Subsets) == 0 {
		e.Subsets = []core.EndpointSubset{
			{},
		}
	}
	if len(e.Subsets[0].Addresses) == 0 {
		e.Subsets[0].Addresses = []core.EndpointAddress{
			{},
		}
	}
	e.Subsets[0].Addresses[0].TargetRef = nil
	return e
}

func checkIP(path *messagediff.Path, iValue interface{}) bool {
	for _, pathNode := range *path {
		// .String() function adds "." in front of the pathNode
		// So it would be ".IP" for pathNode "IPs"
		s := pathNode.String()
		if s == ".IP" {
			if typed, ok := iValue.(string); ok {
				if typed != "" {
					// Have IP address assigned|modified
					return true
				}
			}
		}
	}
	return false
}

func isUpdatedEndpoints(old, new *core.Endpoints) bool {
	oldSubsets := normalizeEndpoints(old).Subsets
	newSubsets := normalizeEndpoints(new).Subsets

	log.V(1).M(new).F().Info("Check whether is updated Endpoints. %s/%s", new.Namespace, new.Name)

	diff, equal := messagediff.DeepDiff(oldSubsets[0].Addresses, newSubsets[0].Addresses)
	if equal {
		log.V(3).M(old).Info("endpointsInformer.UpdateFunc: no changes found")
		// No need to react
		return false
	}

	assigned := false
	for path, iValue := range diff.Added {
		log.V(3).M(old).Info("endpointsInformer.UpdateFunc: added %v", path)
		if address, ok := iValue.(core.EndpointAddress); ok {
			if address.IP != "" {
				assigned = true
			}
		}
	}
	for path := range diff.Removed {
		log.V(3).M(old).Info("endpointsInformer.UpdateFunc: removed %v", path)
	}
	for path, iValue := range diff.Modified {
		log.V(3).M(old).Info("endpointsInformer.UpdateFunc: modified %v", path)
		assigned = assigned || checkIP(path, iValue)
	}

	if assigned {
		log.V(2).M(old).Info("endpointsInformer.UpdateFunc: IP ASSIGNED: %s", litter.Sdump(new.Subsets))
		return true
	}

	return false
}

func isUpdatedEndpointSlice(old, new *discovery.EndpointSlice) bool {
	log.V(1).M(new).F().Info("Check whether is updated EndpointSlice. %s/%s Transition: '%s'=>'%s'", new.Namespace, new.Name, buildComparableEndpointAddresses(old), buildComparableEndpointAddresses(new))
	return buildComparableEndpointAddresses(old) != buildComparableEndpointAddresses(new)
}

func buildComparableEndpointAddresses(epSlice *discovery.EndpointSlice) string {
	return strings.Join(fetchUniqueReadyAddresses(epSlice), ",")
}

func fetchUniqueReadyAddresses(epSlice *discovery.EndpointSlice) (res []string) {
	if epSlice == nil {
		return nil
	}
	for _, ep := range epSlice.Endpoints {
		if (ep.Conditions.Ready != nil) && (*ep.Conditions.Ready == false) {
			// Skip not-ready address
			continue
		}
		res = append(res, ep.Addresses...)
	}
	sort.Strings(res)

	return util.Unique(res)
}

func (c *Controller) addEventHandlersEndpoints(
	kubeInformerFactory kubeInformers.SharedInformerFactory,
) {
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
			log.V(3).M(newEndpoints).Info("endpointsInformer.UpdateFunc")
			if isUpdatedEndpoints(oldEndpoints, newEndpoints) {
				c.enqueueObject(cmd_queue.NewReconcileEndpoints(cmd_queue.ReconcileUpdate, oldEndpoints, newEndpoints))
			}
		},
		DeleteFunc: func(obj interface{}) {
			endpoints := obj.(*core.Endpoints)
			if !c.isTrackedObject(&endpoints.ObjectMeta) {
				return
			}
			log.V(3).M(endpoints).Info("endpointsInformer.DeleteFunc")
		},
	})
}

func (c *Controller) addEventHandlersEndpointSlice(
	kubeInformerFactory kubeInformers.SharedInformerFactory,
) {
	kubeInformerFactory.Discovery().V1().EndpointSlices().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			endpointSlice := obj.(*discovery.EndpointSlice)
			if !c.isTrackedObject(&endpointSlice.ObjectMeta) {
				return
			}
			log.V(3).M(endpointSlice).Info("endpointSliceInformer.AddFunc")
		},
		UpdateFunc: func(old, new interface{}) {
			oldEndpointSlice := old.(*discovery.EndpointSlice)
			newEndpointSlice := new.(*discovery.EndpointSlice)
			if !c.isTrackedObject(&oldEndpointSlice.ObjectMeta) {
				return
			}
			log.V(3).M(newEndpointSlice).Info("endpointSliceInformer.UpdateFunc")
			if isUpdatedEndpointSlice(oldEndpointSlice, newEndpointSlice) {
				c.enqueueObject(cmd_queue.NewReconcileEndpointSlice(cmd_queue.ReconcileUpdate, oldEndpointSlice, newEndpointSlice))
			}
		},
		DeleteFunc: func(obj interface{}) {
			endpointSlice := obj.(*discovery.EndpointSlice)
			if !c.isTrackedObject(&endpointSlice.ObjectMeta) {
				return
			}
			log.V(3).M(endpointSlice).Info("endpointSliceInformer.DeleteFunc")
		},
	})
}

func (c *Controller) addEventHandlersConfigMap(
	kubeInformerFactory kubeInformers.SharedInformerFactory,
) {
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
}

func (c *Controller) addEventHandlersStatefulSet(
	kubeInformerFactory kubeInformers.SharedInformerFactory,
) {
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
}

func (c *Controller) addEventHandlersPod(
	kubeInformerFactory kubeInformers.SharedInformerFactory,
) {
	kubeInformerFactory.Core().V1().Pods().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*core.Pod)
			if !c.isTrackedObject(&pod.ObjectMeta) {
				return
			}
			log.V(3).M(pod).Info("podInformer.AddFunc")
			c.enqueueObject(cmd_queue.NewReconcilePod(cmd_queue.ReconcileAdd, nil, pod))
		},
		UpdateFunc: func(old, new interface{}) {
			oldPod := old.(*core.Pod)
			newPod := new.(*core.Pod)
			if !c.isTrackedObject(&newPod.ObjectMeta) {
				return
			}
			log.V(3).M(newPod).Info("podInformer.UpdateFunc")
			c.enqueueObject(cmd_queue.NewReconcilePod(cmd_queue.ReconcileUpdate, oldPod, newPod))
		},
		DeleteFunc: func(obj interface{}) {
			pod := obj.(*core.Pod)
			if !c.isTrackedObject(&pod.ObjectMeta) {
				return
			}
			log.V(3).M(pod).Info("podInformer.DeleteFunc")
			c.enqueueObject(cmd_queue.NewReconcilePod(cmd_queue.ReconcileDelete, pod, nil))
		},
	})
}

// addEventHandlers
func (c *Controller) addEventHandlers(
	chopInformerFactory chopInformers.SharedInformerFactory,
	kubeInformerFactory kubeInformers.SharedInformerFactory,
) {
	c.addEventHandlersCHI(chopInformerFactory)
	c.addEventHandlersCHIT(chopInformerFactory)
	c.addEventHandlersChopConfig(chopInformerFactory)
	c.addEventHandlersService(kubeInformerFactory)
	//c.addEventHandlersEndpoints(kubeInformerFactory)
	c.addEventHandlersEndpointSlice(kubeInformerFactory)
	c.addEventHandlersConfigMap(kubeInformerFactory)
	c.addEventHandlersStatefulSet(kubeInformerFactory)
	c.addEventHandlersPod(kubeInformerFactory)
}

// isTrackedObject checks whether operator is interested in changes of this object
func (c *Controller) isTrackedObject(meta meta.Object) bool {
	return chop.Config().IsNamespaceWatched(meta.GetNamespace()) && chiLabeler.New(nil).IsCHOPGeneratedObject(meta)
}

// Run syncs caches, starts workers
func (c *Controller) Run(ctx context.Context) {
	defer utilRuntime.HandleCrash()
	defer func() {
		for i := range c.queues {
			//c.queues[i].ShutDown()
			c.queues[i].Close()
		}
	}()

	log.V(1).Info("Starting ClickHouseInstallation controller")

	// Label controller runtime objects with proper labels
	max := 10
	for cnt := 0; cnt < max; cnt++ {
		switch err := c.ctrlLabeler.LabelMyObjectsTree(ctx); err {
		case nil:
			cnt = max
		case ctrlLabeler.ErrOperatorPodNotSpecified:
			log.V(1).F().Error("Since operator pod is not specified, will not perform labeling")
			cnt = max
		default:
			log.V(1).F().Error("ERROR label objects, will retry. Err: %v", err)
			util.WaitContextDoneOrTimeout(ctx, 5*time.Second)
		}
	}

	//
	// Start threads
	//
	workersNum := len(c.queues)
	log.V(1).F().Info("ClickHouseInstallation controller: starting workers number: %d", workersNum)
	for i := 0; i < workersNum; i++ {
		log.V(1).F().Info("ClickHouseInstallation controller: starting worker %d out of %d", i+1, workersNum)
		sys := false
		if i < api.DefaultReconcileSystemThreadsNumber {
			sys = true
		}
		worker := c.newWorker(c.queues[i], sys)
		go wait.Until(worker.run, runWorkerPeriod, ctx.Done())
	}
	defer log.V(1).F().Info("ClickHouseInstallation controller: shutting down workers")

	log.V(1).F().Info("ClickHouseInstallation controller: workers started")
	<-ctx.Done()
}

func prepareCHIAdd(command *cmd_queue.ReconcileCHI) bool {
	newjs, _ := json.Marshal(command.New)
	newchi := api.ClickHouseInstallation{
		TypeMeta: meta.TypeMeta{
			APIVersion: api.SchemeGroupVersion.String(),
			Kind:       api.ClickHouseInstallationCRDResourceKind,
		},
	}
	_ = json.Unmarshal(newjs, &newchi)
	command.New = &newchi
	logCommand(command)
	return true
}

func prepareCHIUpdate(command *cmd_queue.ReconcileCHI) bool {
	actionPlan := action_plan.NewActionPlan(command.Old, command.New)
	if !actionPlan.HasActionsToDo() {
		return false
	}
	oldjson, _ := json.MarshalIndent(command.Old, "", "  ")
	newjson, _ := json.MarshalIndent(command.New, "", "  ")
	log.V(2).Info("AP enqueue---------------------------------------------:\n%s\n", actionPlan)
	log.V(3).Info("old enqueue--------------------------------------------:\n%s\n", string(oldjson))
	log.V(3).Info("new enqueue--------------------------------------------:\n%s\n", string(newjson))

	oldjs, _ := json.Marshal(command.Old)
	newjs, _ := json.Marshal(command.New)
	oldchi := api.ClickHouseInstallation{}
	newchi := api.ClickHouseInstallation{
		TypeMeta: meta.TypeMeta{
			APIVersion: api.SchemeGroupVersion.String(),
			Kind:       api.ClickHouseInstallationCRDResourceKind,
		},
	}
	_ = json.Unmarshal(oldjs, &oldchi)
	_ = json.Unmarshal(newjs, &newchi)
	command.Old = &oldchi
	command.New = &newchi
	logCommand(command)
	return true
}

func logCommand(command *cmd_queue.ReconcileCHI) {
	namespace := "uns"
	name := "un"
	switch {
	case command.New != nil:
		namespace = command.New.Namespace
		name = command.New.Name
	case command.Old != nil:
		namespace = command.Old.Namespace
		name = command.Old.Name
	}
	log.V(1).Info("ENQUEUE new ReconcileCHI cmd=%s for %s/%s", command.Cmd, namespace, name)
}

// enqueueObject adds ClickHouseInstallation object to the work queue
func (c *Controller) enqueueObject(obj queue.PriorityQueueItem) {
	handle := []byte(obj.Handle().(string))
	index := 0
	enqueue := false
	switch command := obj.(type) {
	case *cmd_queue.ReconcileCHI:
		variants := len(c.queues) - api.DefaultReconcileSystemThreadsNumber
		index = api.DefaultReconcileSystemThreadsNumber + util.HashIntoIntTopped(handle, variants)
		switch command.Cmd {
		case cmd_queue.ReconcileAdd:
			enqueue = prepareCHIAdd(command)
		case cmd_queue.ReconcileUpdate:
			enqueue = prepareCHIUpdate(command)
		}
	case
		*cmd_queue.ReconcileCHIT,
		*cmd_queue.ReconcileChopConfig,
		*cmd_queue.ReconcileEndpoints,
		*cmd_queue.ReconcileEndpointSlice,
		*cmd_queue.ReconcilePod:
		variants := api.DefaultReconcileSystemThreadsNumber
		index = util.HashIntoIntTopped(handle, variants)
		enqueue = true
	}
	if enqueue {
		//c.queues[index].AddRateLimited(obj)
		c.queues[index].Insert(obj)
	}
}

// updateWatch
func (c *Controller) updateWatch(chi *api.ClickHouseInstallation) {
	watched := metrics.NewWatchedCR(chi)
	go c.updateWatchAsync(watched)
}

// updateWatchAsync
func (c *Controller) updateWatchAsync(chi *metrics.WatchedCR) {
	if err := clickhouse.InformMetricsExporterAboutWatchedCHI(chi); err != nil {
		log.V(1).F().Info("FAIL update watch (%s/%s): %q", chi.Namespace, chi.Name, err)
	} else {
		log.V(1).Info("OK update watch (%s/%s): %s", chi.Namespace, chi.Name, chi)
	}
}

// deleteWatch
func (c *Controller) deleteWatch(chi *api.ClickHouseInstallation) {
	watched := metrics.NewWatchedCR(chi)
	go c.deleteWatchAsync(watched)
}

// deleteWatchAsync
func (c *Controller) deleteWatchAsync(chi *metrics.WatchedCR) {
	if err := clickhouse.InformMetricsExporterToDeleteWatchedCHI(chi); err != nil {
		log.V(1).F().Info("FAIL delete watch (%s/%s): %q", chi.Namespace, chi.Name, err)
	} else {
		log.V(1).Info("OK delete watch (%s/%s)", chi.Namespace, chi.Name)
	}
}

// addChopConfig
func (c *Controller) addChopConfig(chopConfig *api.ClickHouseOperatorConfiguration) error {
	if chop.Get().ConfigManager.IsConfigListed(chopConfig) {
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
func (c *Controller) updateChopConfig(old, new *api.ClickHouseOperatorConfiguration) error {
	if old.GetObjectMeta().GetResourceVersion() == new.GetObjectMeta().GetResourceVersion() {
		log.V(2).M(old).F().Info("ResourceVersion did not change: %s", old.GetObjectMeta().GetResourceVersion())
		// No need to react
		return nil
	}

	log.V(2).M(new).F().Info("ResourceVersion change: %s to %s", old.GetObjectMeta().GetResourceVersion(), new.GetObjectMeta().GetResourceVersion())
	// TODO
	// NEED REFACTORING
	//os.Exit(0)

	return nil
}

// deleteChit deletes CHIT
func (c *Controller) deleteChopConfig(chopConfig *api.ClickHouseOperatorConfiguration) error {
	log.V(2).M(chopConfig).F().P()
	// TODO
	// NEED REFACTORING
	//os.Exit(0)

	return nil
}

type patchFinalizers struct {
	Op    string   `json:"op"`
	Path  string   `json:"path"`
	Value []string `json:"value"`
}

// patchCHIFinalizers patch ClickHouseInstallation finalizers
func (c *Controller) patchCHIFinalizers(ctx context.Context, chi *api.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted. CR patch fin: %s ", chi.GetName())
		return nil
	}

	// TODO fix this with verbosity update
	// Start Debug object
	//js, err := json.MarshalIndent(chi, "", "  ")
	//if err != nil {
	//	log.V(1).M(chi).F().Error("%q", err)
	//}
	//log.V(3).M(chi).F().Info("\n%s\n", js)
	// End Debug object

	payload, _ := json.Marshal([]patchFinalizers{{
		Op:    "replace",
		Path:  "/metadata/finalizers",
		Value: chi.GetFinalizers(),
	}})

	_new, err := c.chopClient.ClickhouseV1().ClickHouseInstallations(chi.Namespace).Patch(ctx, chi.Name, kubeTypes.JSONPatchType, payload, controller.NewPatchOptions())
	if err != nil {
		// Error update
		log.V(1).M(chi).F().Error("%q", err)
		return err
	}

	if chi.GetResourceVersion() != _new.GetResourceVersion() {
		// Updated
		log.V(2).M(chi).F().Info("ResourceVersion change: %s to %s", chi.GetResourceVersion(), _new.GetResourceVersion())
		chi.SetResourceVersion(_new.GetResourceVersion())
		return nil
	}

	// ResourceVersion not changed - no update performed?

	return nil
}

func (c *Controller) poll(ctx context.Context, chi *api.ClickHouseInstallation, f func(c *api.ClickHouseInstallation, e error) bool) {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted. Polling CR: %s ", chi.GetName())
		return
	}

	namespace, name := util.NamespaceName(chi)

	for {
		cur, err := c.kube.CR().Get(ctx, namespace, name)
		if f(cur.(*api.ClickHouseInstallation), err) {
			// Continue polling
			if util.IsContextDone(ctx) {
				log.V(1).Info("Reconcile is aborted. Polling CR: %s ", chi.GetName())
				return
			}
			time.Sleep(15 * time.Second)
		} else {
			// Stop polling
			return
		}
	}
}

// installFinalizer
func (c *Controller) installFinalizer(ctx context.Context, chi *api.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted. CR install fn: %s ", chi.GetName())
		return nil
	}

	log.V(2).M(chi).S().P()
	defer log.V(2).M(chi).E().P()

	cur, err := c.chopClient.ClickhouseV1().ClickHouseInstallations(chi.Namespace).Get(ctx, chi.Name, controller.NewGetOptions())
	if err != nil {
		return err
	}
	if cur == nil {
		return fmt.Errorf("ERROR GetCR (%s/%s): NULL returned", chi.Namespace, chi.Name)
	}

	if util.InArray(FinalizerName, cur.GetFinalizers()) {
		// Already installed
		return nil
	}
	log.V(3).M(chi).F().Info("no finalizer found, need to install one")

	cur.SetFinalizers(append(cur.GetFinalizers(), FinalizerName))
	return c.patchCHIFinalizers(ctx, cur)
}

// uninstallFinalizer
func (c *Controller) uninstallFinalizer(ctx context.Context, chi *api.ClickHouseInstallation) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted. CR uninstall fin: %s ", chi.GetName())
		return nil
	}

	log.V(2).M(chi).S().P()
	defer log.V(2).M(chi).E().P()

	cur, err := c.chopClient.ClickhouseV1().ClickHouseInstallations(chi.Namespace).Get(ctx, chi.Name, controller.NewGetOptions())
	if err != nil {
		return err
	}
	if cur == nil {
		return fmt.Errorf("ERROR GetCR (%s/%s): NULL returned", chi.Namespace, chi.Name)
	}

	cur.SetFinalizers(util.RemoveFromArray(FinalizerName, cur.GetFinalizers()))

	return c.patchCHIFinalizers(ctx, cur)
}

// handleObject enqueues CHI which is owner of `obj` into reconcile loop
func (c *Controller) handleObject(obj interface{}) {
	// TODO review
	object, ok := obj.(meta.Object)
	if !ok {
		ts, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilRuntime.HandleError(fmt.Errorf(messageUnableToDecode))
			return
		}
		object, ok = ts.Obj.(meta.Object)
		if !ok {
			utilRuntime.HandleError(fmt.Errorf(messageUnableToDecode))
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
	if ownerRef.Kind != api.ClickHouseInstallationCRDResourceKind {
		return
	}

	log.V(1).Info("Processing object: %s", object.GetName())

	// Get owner - it is expected to be CHI
	// TODO chi, err := c.chi.ClickHouseInstallations(object.GetNamespace()).Get(ownerRef.Name)

	// TODO
	//if err != nil {
	//	log.V(1).Infof("ignoring orphaned object '%s' of ClickHouseInstallation '%s'", object.GetSelfLink(), ownerRef.Name)
	//	return
	//}

	// Add CHI object into reconcile loop
	// TODO c.enqueueObject(chi.Namespace, chi.Name, chi)
}

func shouldEnqueue(chi *api.ClickHouseInstallation) bool {
	if !chop.Config().IsNamespaceWatched(chi.Namespace) {
		return false
	}

	// if CR is suspended, should skip reconciliation
	if chi.Spec.Suspend.Value() {
		log.V(5).M(chi).Info("chiInformer: skip enqueue, CHI suspended")
		return false
	}

	return true
}
