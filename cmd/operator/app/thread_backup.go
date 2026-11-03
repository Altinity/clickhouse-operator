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

package app

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"

	batch "k8s.io/api/batch/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	apiMachineryRuntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/discovery"
	clientGoScheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlRuntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	ctrlManager "sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	controller "github.com/altinity/clickhouse-operator/pkg/controller/chbackup"
	chiKube "github.com/altinity/clickhouse-operator/pkg/controller/chi/kube"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chbackup"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// backupComponent names the backup controllers in start-up logs.
const backupComponent = "backup"

// backupCRDsPollInterval paces the check for the backup CRDs while they are not served.
const backupCRDsPollInterval = time.Minute

// recoveryControllerName tells the recovery controller apart from the CHI controller in logs;
// both reconcile installations.
const recoveryControllerName = "clickhouseinstallation-recovery"

// backupKinds are the kinds the backup controllers reconcile.
var backupKinds = []string{
	api.ClickHouseBackupCRDResourceKind,
	api.ClickHouseBackupScheduleCRDResourceKind,
	api.ClickHouseRestoreCRDResourceKind,
}

// The backup controllers run in a manager of their own rather than in Keeper's. Helm installs the
// crds/ directory on first install only and never upgrades it, so an operator upgraded in place
// runs where the backup CRDs may not exist; a manager watching a missing kind fails to start, and
// sharing one would take Keeper reconciliation down with backup.
var (
	backupScheme  *apiMachineryRuntime.Scheme
	backupManager ctrlRuntime.Manager
	backupLogger  logr.Logger
)

// waitForBackupCRDs waits until the API server serves every backup kind, and reports whether it
// does: false means ctx is done. A missing CRD is warned about once, then polled for quietly.
func waitForBackupCRDs(ctx context.Context, discovery discovery.DiscoveryInterface, interval time.Duration) bool {
	warned := false
	for {
		missing, err := missingBackupKinds(discovery)
		switch {
		case (err == nil) && (len(missing) == 0):
			log.Info("backup CRDs are served")
			return true
		case !warned:
			log.Warning("backup CRDs are not served, backup reconciliation waits for them. missing: %v err: %v", missing, err)
			warned = true
		default:
			log.V(1).Info("backup CRDs are still not served. missing: %v err: %v", missing, err)
		}
		if util.WaitContextDoneOrTimeout(ctx, interval) {
			return false
		}
	}
}

// missingBackupKinds lists the backup kinds the API server does not serve.
func missingBackupKinds(discovery discovery.DiscoveryInterface) ([]string, error) {
	resources, err := discovery.ServerResourcesForGroupVersion(api.SchemeGroupVersion.String())
	if err != nil {
		if apiErrors.IsNotFound(err) {
			return backupKinds, nil
		}
		return nil, err
	}
	served := make(map[string]bool)
	for _, resource := range resources.APIResources {
		served[resource.Kind] = true
	}
	var missing []string
	for _, kind := range backupKinds {
		if !served[kind] {
			missing = append(missing, kind)
		}
	}
	return missing, nil
}

// newBackupCacheOptions builds the backup manager cache config. Jobs and CronJobs are narrowed to
// the ones the backup controllers build; without that, the cache would hold every Job and CronJob
// in the watched namespaces. The custom resources and installations stay unfiltered: they are
// user-authored and carry no operator label. Installations are trimmed instead, to the metadata
// and status the watches need.
//
// log is a parameter rather than the package-level logger because a logr zero value discards Info
// silently, so reading that var would let this line vanish if the sole call site ever moved.
func newBackupCacheOptions(log logr.Logger, cacheNamespaces []string) cache.Options {
	selector := chbackup.JobSelector()
	byObject := map[client.Object]cache.ByObject{
		&batch.Job{}:                  {Label: selector},
		&batch.CronJob{}:              {Label: selector},
		&api.ClickHouseInstallation{}: {Transform: controller.TrimInstallation},
	}

	// A wrong selector fails silently - the cache just stays empty - so the effective one goes in
	// the boot log.
	log.Info("init backup - cache label selector", "selector", selector.String(), "types", len(byObject))

	return cache.Options{
		DefaultNamespaces: newCacheNamespaces(cacheNamespaces),
		ByObject:          byObject,
	}
}

// newBackupInitializer resolves the clients that must be built exactly once, then returns the
// step that is safe to retry: it waits for the backup CRDs to be served and builds the manager.
//
// chop.GetClientset terminates the process when it cannot build a client, so it runs here, once,
// outside the retry; initClickHouse has made the identical call by this point.
func newBackupInitializer() func(context.Context) error {
	kubeClient, _, chopClient, _ := chop.GetClientset(kubeConfigFile, masterURL, chopConfigFile)
	cr := chiKube.NewCR(chopClient, kubeClient)
	return func(ctx context.Context) error {
		if !waitForBackupCRDs(ctx, kubeClient.Discovery(), backupCRDsPollInterval) {
			return ctx.Err()
		}
		return initBackup(ctx, cr)
	}
}

func initBackup(ctx context.Context, cr interfaces.IKubeCR) error {
	var err error

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	backupLogger = ctrl.Log.WithName("backup-runner")

	// Scheme registration is compiled-in and touches no API server: a failure is a programming
	// fault that fails identically on every retry.
	backupScheme = apiMachineryRuntime.NewScheme()
	if err = clientGoScheme.AddToScheme(backupScheme); err != nil {
		backupLogger.Error(err, "init backup - unable to clientGoScheme.AddToScheme")
		return fmt.Errorf("%w: %w", errInitTerminal, err)
	}
	if err = api.AddToScheme(backupScheme); err != nil {
		backupLogger.Error(err, "init backup - unable to api.AddToScheme")
		return fmt.Errorf("%w: %w", errInitTerminal, err)
	}

	// GetConfig, not GetConfigOrDie: the latter exits the process, which is exactly what this
	// retry exists to avoid.
	restConfig, err := ctrlRuntime.GetConfig()
	if err != nil {
		backupLogger.Error(err, "init backup - unable to ctrlRuntime.GetConfig")
		return err
	}

	backupManager, err = ctrlRuntime.NewManager(restConfig, ctrlRuntime.Options{
		Scheme: backupScheme,
		Cache:  newBackupCacheOptions(backupLogger, chop.Config().GetCacheNamespaces()),
		// Disable controller-runtime's built-in metrics listener: operator metrics, backup
		// metrics included, are served by pkg/metrics/operator on the operator's own endpoint.
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		// Not terminal: building the manager depends on the API server.
		backupLogger.Error(err, "init backup - unable to ctrlRuntime.NewManager")
		return err
	}

	if err = setupBackupControllers(backupManager, cr); err != nil {
		// Building controllers resolves the scheme and records watches without API calls, so a
		// failure is a programming fault.
		backupLogger.Error(err, "init backup - unable to set up controllers")
		return fmt.Errorf("%w: %w", errInitTerminal, err)
	}

	// Logged by the manager itself once it runs its controllers, rather than after Start returns,
	// which happens only at shutdown.
	if err = backupManager.Add(ctrlManager.RunnableFunc(func(context.Context) error {
		log.Info("Starting backup OK")
		return nil
	})); err != nil {
		return fmt.Errorf("%w: %w", errInitTerminal, err)
	}

	// Initialization successful
	return nil
}

// setupBackupControllers registers the backup controllers with the manager. Every controller
// drops events from unwatched namespaces before they are queued.
func setupBackupControllers(manager ctrlRuntime.Manager, cr interfaces.IKubeCR) error {
	c, apiReader, scheme := manager.GetClient(), manager.GetAPIReader(), manager.GetScheme()

	if err := ctrlRuntime.
		NewControllerManagedBy(manager).
		For(&api.ClickHouseBackup{}).
		Owns(&batch.Job{}).
		WithEventFilter(controller.WatchedNamespacePredicate()).
		Complete(controller.NewBackupController(c, apiReader, scheme, cr)); err != nil {
		return err
	}

	schedule := controller.NewScheduleController(c, apiReader, scheme, cr)
	if err := ctrlRuntime.
		NewControllerManagedBy(manager).
		For(&api.ClickHouseBackupSchedule{}).
		Owns(&batch.CronJob{}).
		Watches(
			&api.ClickHouseInstallation{},
			handler.EnqueueRequestsFromMapFunc(schedule.SchedulesForCHI),
			builder.WithPredicates(controller.CHITopologyPredicate()),
		).
		Watches(&api.ClickHouseRestore{}, handler.EnqueueRequestsFromMapFunc(schedule.SchedulesForRestore)).
		Watches(&batch.Job{}, handler.EnqueueRequestsFromMapFunc(schedule.SchedulesForJob)).
		WithEventFilter(controller.WatchedNamespacePredicate()).
		Complete(schedule); err != nil {
		return err
	}

	if err := ctrlRuntime.
		NewControllerManagedBy(manager).
		For(&api.ClickHouseRestore{}).
		Owns(&batch.Job{}).
		WithEventFilter(controller.WatchedNamespacePredicate()).
		Complete(controller.NewRestoreController(c, apiReader, scheme, cr)); err != nil {
		return err
	}

	// Owns the recovery restores, one per cluster: a change of their status records the recovery once
	// every one of them has completed.
	return ctrlRuntime.
		NewControllerManagedBy(manager).
		Named(recoveryControllerName).
		For(&api.ClickHouseInstallation{}, builder.WithPredicates(controller.RecoveryPredicate())).
		Owns(&api.ClickHouseRestore{}).
		WithEventFilter(controller.WatchedNamespacePredicate()).
		Complete(controller.NewRecoveryController(c, apiReader, scheme, cr))
}

func runBackup(ctx context.Context) error {
	if err := backupManager.Start(ctx); err != nil {
		backupLogger.Error(err, "run backup - unable to backupManager.Start")
		return err
	}
	// Run successful
	return nil
}
