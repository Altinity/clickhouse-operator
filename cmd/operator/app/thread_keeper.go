package app

import (
	"context"

	"github.com/go-logr/logr"

	apps "k8s.io/api/apps/v1"
	apiMachineryRuntime "k8s.io/apimachinery/pkg/runtime"
	clientGoScheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlRuntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	//	ctrl "sigs.k8s.io/controller-runtime/pkg/controller"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	controller "github.com/altinity/clickhouse-operator/pkg/controller/chk"
)

var (
	scheme  *apiMachineryRuntime.Scheme
	manager ctrlRuntime.Manager
	logger  logr.Logger
)

func initKeeper(ctx context.Context) error {
	var err error

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	logger = ctrl.Log.WithName("keeper-runner")

	scheme = apiMachineryRuntime.NewScheme()
	if err = clientGoScheme.AddToScheme(scheme); err != nil {
		logger.Error(err, "init keeper - unable to clientGoScheme.AddToScheme")
		return err
	}
	if err = api.AddToScheme(scheme); err != nil {
		logger.Error(err, "init keeper - unable to api.AddToScheme")
		return err
	}

	defaultNamespaces := make(map[string]cache.Config)
	for _, ns := range chop.Config().GetCacheNamespaces() {
		defaultNamespaces[ns] = cache.Config{}
	}
	manager, err = ctrlRuntime.NewManager(ctrlRuntime.GetConfigOrDie(), ctrlRuntime.Options{
		Scheme: scheme,
		Cache: cache.Options{
			// GetCacheNamespaces returns exact namespace names when all configured watch namespaces are
			// valid DNS labels, enabling per-namespace cache scoping. Falls back to NamespaceAll when
			// any watch namespace is a regexp pattern (the controller's Reconcile guard handles filtering
			// in that case).
			DefaultNamespaces: defaultNamespaces,
		},
		// Disable controller-runtime's built-in metrics listener. Default is ":8080" which
		// would bind a third HTTP endpoint on the operator pod (not in any Service, not in
		// any pod annotation, not in any containerPort) — an orphan reachable only by direct
		// PodIP routing. The operator's own /metrics endpoint already lives at :9999 via
		// pkg/metrics/operator, and the CHK reconcile counters worth exposing are surfaced
		// through that path, not through controller-runtime's manager-default exposition.
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		logger.Error(err, "init keeper - unable to ctrlRuntime.NewManager")
		return err
	}

	// Build the apiextensions client for CRD deletion checks during CHK cleanup.
	// Uses the same kubeConfigFile/masterURL package vars as the CHI thread.
	_, extClient, _, _ := chop.GetClientset(kubeConfigFile, masterURL)

	err = ctrlRuntime.
		NewControllerManagedBy(manager).
		For(
			&api.ClickHouseKeeperInstallation{},
			builder.WithPredicates(keeperPredicate()),
		).
		Owns(&apps.StatefulSet{}).
		Complete(
			&controller.Controller{
				Client:    manager.GetClient(),
				APIReader: manager.GetAPIReader(),
				Scheme:    manager.GetScheme(),
				ExtClient: extClient,
			},
		)
	if err != nil {
		logger.Error(err, "init keeper - unable to ctrlRuntime.NewControllerManagedBy")
		return err
	}

	// Initialization successful
	return nil
}

func runKeeper(ctx context.Context) error {
	if err := manager.Start(ctx); err != nil {
		logger.Error(err, "run keeper - unable to manager.Start")
		return err
	}
	// Run successful
	return nil
}

func keeperPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			new, ok := e.Object.(*api.ClickHouseKeeperInstallation)
			if !ok {
				return false
			}

			if !controller.ShouldEnqueue(new) {
				return false
			}

			return true
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			new, ok := e.ObjectNew.(*api.ClickHouseKeeperInstallation)
			if !ok {
				return false
			}

			if !controller.ShouldEnqueue(new) {
				return false
			}

			return true
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return true
		},
	}
}
