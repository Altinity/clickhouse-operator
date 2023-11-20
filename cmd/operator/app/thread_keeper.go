package app

import (
	"context"
	"os"

	"github.com/go-logr/logr"

	apps "k8s.io/api/apps/v1"
	apiMachineryRuntime "k8s.io/apimachinery/pkg/runtime"
	utilRuntime "k8s.io/apimachinery/pkg/util/runtime"
	clientGoScheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	controller "github.com/altinity/clickhouse-operator/pkg/controller/chk"
)

var scheme = apiMachineryRuntime.NewScheme()

func init() {
	utilRuntime.Must(clientGoScheme.AddToScheme(scheme))
	utilRuntime.Must(api.AddToScheme(scheme))
}

var (
	manager ctrl.Manager
	logger  logr.Logger
)

func initKeeper(ctx context.Context) {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	logger = ctrl.Log.WithName("keeper-runner")
	var err error

	manager, err = ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
	})
	if err != nil {
		logger.Error(err, "could not create manager")
		os.Exit(1)
	}

	err = ctrl.
		NewControllerManagedBy(manager).          // Create the Controller
		For(&api.ClickHouseKeeperInstallation{}). // ReplicaSet is the Application API
		Owns(&apps.StatefulSet{}).                // ReplicaSet owns Pods created by it
		Complete(&controller.ChkReconciler{
			Client: manager.GetClient(),
			Scheme: manager.GetScheme(),
		})
	if err != nil {
		logger.Error(err, "could not create controller")
		os.Exit(1)
	}
}

func runKeeper(ctx context.Context) {
	if err := manager.Start(ctx); err != nil {
		logger.Error(err, "could not start manager")
		os.Exit(1)
	}
}
