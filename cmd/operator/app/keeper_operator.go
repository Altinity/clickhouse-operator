package app

import (
	"os"

	appsV1 "k8s.io/api/apps/v1"
	apiMachineryRuntime "k8s.io/apimachinery/pkg/runtime"
	utilRuntime "k8s.io/apimachinery/pkg/util/runtime"
	clientGoScheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.com/v1alpha1"
	"github.com/altinity/clickhouse-operator/pkg/controller/chk"
)

var (
	scheme = apiMachineryRuntime.NewScheme()
)

func init() {
	utilRuntime.Must(clientGoScheme.AddToScheme(scheme))
	utilRuntime.Must(v1alpha1.AddToScheme(scheme))
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
		Owns(&appsV1.StatefulSet{}).       // ReplicaSet owns Pods created by it
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
