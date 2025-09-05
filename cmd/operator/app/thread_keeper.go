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

	manager, err = ctrlRuntime.NewManager(ctrlRuntime.GetConfigOrDie(), ctrlRuntime.Options{
		Scheme: scheme,
		Cache: cache.Options{
			Namespaces: []string{chop.Config().GetInformerNamespace()},
		},
	})
	if err != nil {
		logger.Error(err, "init keeper - unable to ctrlRuntime.NewManager")
		return err
	}

	err = ctrlRuntime.
		NewControllerManagedBy(manager).
		For(&api.ClickHouseKeeperInstallation{}, builder.WithPredicates(keeperPredicate())).
		Owns(&apps.StatefulSet{}).
		Complete(
			&controller.Controller{
				Client: manager.GetClient(),
				Scheme: manager.GetScheme(),
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
			obj, ok := e.Object.(*api.ClickHouseKeeperInstallation)
			if !ok {
				return false
			}

			// Check if namespace should be watched (includes deny list check)
			if !chop.Config().IsWatchedNamespace(obj.Namespace) {
				logger.V(2).Info("chkInformer: skip event, namespace is not watched or is in deny list", "namespace", obj.Namespace)
				return false
			}

			if obj.Spec.Suspend.Value() {
				return false
			}
			return true
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			obj, ok := e.ObjectNew.(*api.ClickHouseKeeperInstallation)
			if !ok {
				return false
			}

			// Check if namespace should be watched (includes deny list check)
			if !chop.Config().IsWatchedNamespace(obj.Namespace) {
				logger.V(2).Info("chkInformer: skip event, namespace is not watched or is in deny list", "namespace", obj.Namespace)
				return false
			}

			if obj.Spec.Suspend.Value() {
				return false
			}
			return true
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return true
		},
	}
}
