package app

import (
	"os"

	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.com/v1alpha1"
	"github.com/altinity/clickhouse-operator/pkg/controller/chk"
	appsv1 "k8s.io/api/apps/v1"
	apimachineryruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	scheme = apimachineryruntime.NewScheme()
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
}

func KeeperRun() {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	var log = ctrl.Log.WithName("keeper-runner")
	log.Info("KeeperRun() called")

	manager, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
	})
	if err != nil {
		log.Error(err, "could not create manager")
		os.Exit(1)
	}

	err = ctrl.
		NewControllerManagedBy(manager).   // Create the Controller
		For(&v1alpha1.ClickHouseKeeper{}). // ReplicaSet is the Application API
		Owns(&appsv1.StatefulSet{}).       // ReplicaSet owns Pods created by it
		Complete(&chk.ChkReconciler{
			Client: manager.GetClient(),
			Scheme: manager.GetScheme(),
		})
	if err != nil {
		log.Error(err, "could not create controller")
		os.Exit(1)
	}

	if err := manager.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "could not start manager")
		os.Exit(1)
	}
}
