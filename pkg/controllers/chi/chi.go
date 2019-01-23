package chi

import (
	"context"

	apisv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	clientset "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	informers "github.com/altinity/clickhouse-operator/pkg/client/informers/externalversions/clickhouse.altinity.com/v1"
	listers "github.com/altinity/clickhouse-operator/pkg/client/listers/clickhouse.altinity.com/v1"

	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	kubernetes "k8s.io/client-go/kubernetes"
	scheme "k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	appslisters "k8s.io/client-go/listers/apps/v1"
	cache "k8s.io/client-go/tools/cache"
	record "k8s.io/client-go/tools/record"
	workqueue "k8s.io/client-go/util/workqueue"

	"github.com/golang/glog"
)

// Controller defines CRO controller
type Controller struct {
	kubeClient              kubernetes.Interface
	chiClient               clientset.Interface
	shutdown                bool
	queue                   workqueue.RateLimitingInterface
	recorder                record.EventRecorder
	chiLister               listers.ClickHouseInstallationLister
	chiListerSynced         cache.InformerSynced
	chiUpdater              chiUpdaterInterface
	statefulSetLister       appslisters.StatefulSetLister
	statefulSetListerSynced cache.InformerSynced
	statefulSetControl      statefulSetControlInterface
}

type chiUpdaterInterface interface {
	UpdateClusterLabels(chi *apisv1.ClickHouseInstallation, ls labels.Set) error
}

type statefulSetControlInterface interface {
	CreateStatefulSet(ss *apps.StatefulSet) error
	Patch(old *apps.StatefulSet, new *apps.StatefulSet) error
}

// CreateController creates instance of Controller
func CreateController(
	chiClient clientset.Interface,
	kubeClient kubernetes.Interface,
	chiInformer informers.ClickHouseInstallationInformer,
	ssInformer appsinformers.StatefulSetInformer,
	podInformer coreinformers.PodInformer,
	serviceInformer coreinformers.ServiceInformer) *Controller {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "clickhouse-operator"})
	controller := &Controller{
		kubeClient: kubeClient,
		chiClient:  chiClient,
		queue:      workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "chi"),
		recorder:   recorder,
	}
	return controller
}

// Run syncs caches, starts workers
func (c *Controller) Run(ctx context.Context, threadiness int) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()
}
