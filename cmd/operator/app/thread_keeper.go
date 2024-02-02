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
	ctrlRuntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	//	ctrl "sigs.k8s.io/controller-runtime/pkg/controller"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	controller "github.com/altinity/clickhouse-operator/pkg/controller/chk"
)

var scheme = apiMachineryRuntime.NewScheme()

func init() {
	utilRuntime.Must(clientGoScheme.AddToScheme(scheme))
	utilRuntime.Must(api.AddToScheme(scheme))
}

var (
	manager ctrlRuntime.Manager
	logger  logr.Logger
)

func initKeeper(ctx context.Context) {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	logger = ctrl.Log.WithName("keeper-runner")

	var err error

	manager, err = ctrlRuntime.NewManager(ctrlRuntime.GetConfigOrDie(), ctrlRuntime.Options{
		Scheme: scheme,
		Cache: cache.Options{
			Namespaces: []string{chop.Config().GetInformerNamespace()},
		},
	})
	if err != nil {
		os.Exit(1)
	}

	err = ctrlRuntime.
		NewControllerManagedBy(manager).
		For(&api.ClickHouseKeeperInstallation{}).
		Owns(&apps.StatefulSet{}).
		Complete(
			&controller.ChkReconciler{
				Client: manager.GetClient(),
				Scheme: manager.GetScheme(),
			},
		)
	if err != nil {
		os.Exit(1)
	}
}

func runKeeper(ctx context.Context) {
	if err := manager.Start(ctx); err != nil {
		os.Exit(1)
	}
}
