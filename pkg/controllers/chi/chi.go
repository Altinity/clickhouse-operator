package chi

import (
	"context"
	"fmt"
	"time"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	clientset "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	clientscheme "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned/scheme"
	informers "github.com/altinity/clickhouse-operator/pkg/client/informers/externalversions/clickhouse.altinity.com/v1"
	listers "github.com/altinity/clickhouse-operator/pkg/client/listers/clickhouse.altinity.com/v1"
	parser "github.com/altinity/clickhouse-operator/pkg/parser"

	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	wait "k8s.io/apimachinery/pkg/util/wait"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	kubernetes "k8s.io/client-go/kubernetes"
	scheme "k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	cache "k8s.io/client-go/tools/cache"
	record "k8s.io/client-go/tools/record"
	workqueue "k8s.io/client-go/util/workqueue"

	"github.com/golang/glog"
)

// Controller defines CRO controller
type Controller struct {
	kubeClient              kubernetes.Interface
	chiClient               clientset.Interface
	chiLister               listers.ClickHouseInstallationLister
	chiListerSynced         cache.InformerSynced
	statefulSetLister       appslisters.StatefulSetLister
	statefulSetListerSynced cache.InformerSynced
	configMapLister         corelisters.ConfigMapLister
	configMapListerSynced   cache.InformerSynced
	serviceLister           corelisters.ServiceLister
	serviceListerSynced     cache.InformerSynced
	queue                   workqueue.RateLimitingInterface
	recorder                record.EventRecorder
}

const (
	successSynced         = "Synced"
	messageResourceSynced = "ClickHouseInstallation synced successfully"
	errResourceExists     = "ErrResourceExists"
	messageResourceExists = "Resource %q already exists and is not managed by ClickHouseInstallation"
)

// CreateController creates instance of Controller
func CreateController(
	chiClient clientset.Interface, kubeClient kubernetes.Interface,
	chiInformer informers.ClickHouseInstallationInformer, ssInformer appsinformers.StatefulSetInformer,
	cmInformer coreinformers.ConfigMapInformer, serviceInformer coreinformers.ServiceInformer) *Controller {
	clientscheme.AddToScheme(scheme.Scheme)
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "clickhouse-operator"})
	controller := &Controller{
		kubeClient:              kubeClient,
		chiClient:               chiClient,
		chiLister:               chiInformer.Lister(),
		chiListerSynced:         chiInformer.Informer().HasSynced,
		statefulSetLister:       ssInformer.Lister(),
		statefulSetListerSynced: ssInformer.Informer().HasSynced,
		configMapLister:         cmInformer.Lister(),
		configMapListerSynced:   cmInformer.Informer().HasSynced,
		serviceLister:           serviceInformer.Lister(),
		serviceListerSynced:     serviceInformer.Informer().HasSynced,
		queue:                   workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "chi"),
		recorder:                recorder,
	}
	chiInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueChi,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueChi(new)
		},
	})
	ssInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.handleObject,
		UpdateFunc: func(old, new interface{}) {
			newStatefulSet := new.(*apps.StatefulSet)
			oldStatefulSet := old.(*apps.StatefulSet)
			if newStatefulSet.ResourceVersion == oldStatefulSet.ResourceVersion {
				return
			}
			controller.handleObject(new)
		},
		DeleteFunc: controller.handleObject,
	})
	return controller
}

// Run syncs caches, starts workers
func (c *Controller) Run(ctx context.Context, threadiness int) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()
	glog.V(1).Info("Starting ClickHouseInstallation controller")
	if !waitForCacheSync("ClickHouseInstallation", ctx.Done(),
		c.chiListerSynced,
		c.statefulSetListerSynced,
		c.configMapListerSynced,
		c.serviceListerSynced) {
		return
	}
	glog.V(1).Info("ClickHouseInstallation controller: starting workers")
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, ctx.Done())
	}
	glog.V(1).Info("ClickHouseInstallation controller: workers started")
	defer glog.V(1).Info("ClickHouseInstallation controller: shutting down workers")
	<-ctx.Done()
}

func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

func (c *Controller) processNextWorkItem() bool {
	obj, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	err := func(obj interface{}) error {
		defer c.queue.Done(obj)
		key, ok := obj.(string)
		if !ok {
			c.queue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("unexpected object in the queue - %#v", obj))
			return nil
		}
		if err := c.syncHandler(key); err != nil {
			return fmt.Errorf("unable to sync an object '%s': %s", key, err.Error())
		}
		c.queue.Forget(obj)
		return nil
	}(obj)
	if err != nil {
		utilruntime.HandleError(err)
		return true
	}
	return true
}

func (c *Controller) syncHandler(key string) error {
	ns, n, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("incorrect resource key: %s", key))
		return nil
	}
	chi, err := c.chiLister.ClickHouseInstallations(ns).Get(n)
	if err != nil {
		if apierrors.IsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("ClickHouseInstallation object '%s' no longer exists in the work queue", key))
			return nil
		}
		return err
	}
	if chi.Status.ObjectPrefixes == nil || len(chi.Status.ObjectPrefixes) == 0 {
		chiObjects, prefixes := parser.CreateObjects(chi)
		if err := c.createControlledResources(chi, chiObjects); err != nil {
			glog.V(2).Infof("ClickHouseInstallation (%q) unable to create controlled resources: %q", chi.Name, err)
			return err
		}
		if err := c.updateChiStatus(chi, prefixes); err != nil {
			glog.V(2).Infof("ClickHouseInstallation (%q) unable to update status of CHI resource: %q", chi.Name, err)
			return err
		}
		glog.V(2).Infof("ClickHouseInstallation (%q) controlled resources are synced (created/updated): %v", chi.Name, prefixes)
	}
	return nil
}

func (c *Controller) createControlledResources(chi *chiv1.ClickHouseInstallation, oMap parser.ObjectsMap) error {
	for _, objList := range oMap {
		switch v := objList.(type) {
		case parser.ConfigMapList:
			for _, obj := range v {
				if err := c.createConfigMap(chi, obj); err != nil {
					return err
				}
			}
		case parser.ServiceList:
			for _, obj := range v {
				if err := c.createService(chi, obj); err != nil {
					return err
				}
			}
		case parser.StatefulSetList:
			for _, obj := range v {
				if err := c.createStatefulSet(chi, obj); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *Controller) createConfigMap(chi *chiv1.ClickHouseInstallation, newConfigMap *corev1.ConfigMap) error {
	res, err := c.configMapLister.ConfigMaps(chi.Namespace).Get(newConfigMap.Name)
	if res != nil {
		return nil
	}
	if apierrors.IsNotFound(err) {
		_, err = c.kubeClient.CoreV1().ConfigMaps(chi.Namespace).Create(newConfigMap)
	}
	if err != nil {
		return err
	}
	return nil
}

func (c *Controller) createService(chi *chiv1.ClickHouseInstallation, newService *corev1.Service) error {
	res, err := c.serviceLister.Services(chi.Namespace).Get(newService.Name)
	if res != nil {
		return nil
	}
	if apierrors.IsNotFound(err) {
		_, err = c.kubeClient.CoreV1().Services(chi.Namespace).Create(newService)
	}
	if err != nil {
		return err
	}
	return nil
}

func (c *Controller) createStatefulSet(chi *chiv1.ClickHouseInstallation, newStatefulSet *apps.StatefulSet) error {
	res, err := c.statefulSetLister.StatefulSets(chi.Namespace).Get(newStatefulSet.Name)
	if res != nil {
		return nil
	}
	if apierrors.IsNotFound(err) {
		_, err = c.kubeClient.AppsV1().StatefulSets(chi.Namespace).Create(newStatefulSet)
	}
	if err != nil {
		return err
	}
	return nil
}

func (c *Controller) updateChiStatus(chi *chiv1.ClickHouseInstallation, objectPrefixes []string) error {
	chiCopy := chi.DeepCopy()
	chiCopy.Status = chiv1.ChiStatus{
		ObjectPrefixes: objectPrefixes,
	}
	_, err := c.chiClient.ClickhouseV1().ClickHouseInstallations(chi.Namespace).Update(chiCopy)
	return err
}

func (c *Controller) enqueueChi(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.queue.AddRateLimited(key)
}

func (c *Controller) handleObject(obj interface{}) {
	object, ok := obj.(metav1.Object)
	if !ok {
		ts, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("unable to decode object (invalid type)"))
			return
		}
		object, ok = ts.Obj.(metav1.Object)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("unable to decode object (invalid type)"))
			return
		}
	}
	glog.V(2).Infof("Processing object: %s", object.GetName())
	if ownerRef := metav1.GetControllerOf(object); ownerRef != nil {
		if ownerRef.Kind != chiv1.ClickHouseInstallationCRDResourceKind {
			return
		}
		chi, err := c.chiLister.ClickHouseInstallations(object.GetNamespace()).Get(ownerRef.Name)
		if err != nil {
			glog.V(2).Infof("ignoring orphaned object '%s' of ClickHouseInstallation '%s'", object.GetSelfLink(), ownerRef.Name)
			return
		}
		c.enqueueChi(chi)
		return
	}
}

func waitForCacheSync(n string, ch <-chan struct{}, syncs ...cache.InformerSynced) bool {
	glog.V(1).Infof("Syncing caches for %s controller", n)
	if !cache.WaitForCacheSync(ch, syncs...) {
		utilruntime.HandleError(fmt.Errorf("Unable to sync caches for %s controller", n))
		return false
	}
	glog.V(1).Infof("Caches are synced for %s controller", n)
	return true
}
