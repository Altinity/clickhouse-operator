package app

import (
	"context"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-backup.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	controller "github.com/altinity/clickhouse-operator/pkg/controller/chb"
	apiMachineryRuntime "k8s.io/apimachinery/pkg/runtime"
	clientGoScheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlRuntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

func initBackup(ctx context.Context) error {
	var err error

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	logger = ctrl.Log.WithName("backup-runner")

	scheme = apiMachineryRuntime.NewScheme()
	if err = clientGoScheme.AddToScheme(scheme); err != nil {
		logger.Error(err, "init backup - unable to clientGoScheme.AddToScheme")
		return err
	}
	if err = api.AddToScheme(scheme); err != nil {
		logger.Error(err, "init backup - unable to api.AddToScheme")
		return err
	}

	manager, err = ctrlRuntime.NewManager(ctrlRuntime.GetConfigOrDie(), ctrlRuntime.Options{
		Scheme: scheme,
		Cache: cache.Options{
			Namespaces: []string{chop.Config().GetInformerNamespace()},
		},
		MetricsBindAddress:     "0",
		HealthProbeBindAddress: "0",
	})
	if err != nil {
		logger.Error(err, "init backup - unable to ctrlRuntime.NewManager")
		return err
	}

	err = ctrlRuntime.
		NewControllerManagedBy(manager).
		For(&api.ClickHouseBackup{}, builder.WithPredicates(backupPredicate())).
		Complete(
			&controller.Controller{
				Client: manager.GetClient(),
				Scheme: manager.GetScheme(),
			},
		)
	if err != nil {
		logger.Error(err, "init backup - unable to ctrlRuntime.NewControllerManagedBy")
		return err
	}

	// Initialization successful
	return nil
}

func runBackup(ctx context.Context) error {
	if err := manager.Start(ctx); err != nil {
		logger.Error(err, "run backup - unable to manager.Start")
		return err
	}
	// Run successful
	return nil
}

func backupPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			chb, ok := e.Object.(*api.ClickHouseBackup)
			if !ok {
				return false
			}
			if chb.Status.State == "Completed" {
				return false
			}

			return true
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return false
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return false
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return true
		},
	}
}
