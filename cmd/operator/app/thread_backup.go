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

	"github.com/go-logr/logr"

	batchv1 "k8s.io/api/batch/v1"
	apiMachineryRuntime "k8s.io/apimachinery/pkg/runtime"
	clientGoScheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlRuntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	backup "github.com/altinity/clickhouse-operator/pkg/controller/chbackup"
)

var (
	backupScheme  *apiMachineryRuntime.Scheme
	backupManager ctrlRuntime.Manager
	backupLogger  logr.Logger
)

func initBackup(ctx context.Context) error {
	var err error

	backupLogger = ctrl.Log.WithName("backup-runner")

	backupScheme = apiMachineryRuntime.NewScheme()
	if err = clientGoScheme.AddToScheme(backupScheme); err != nil {
		backupLogger.Error(err, "init backup - unable to clientGoScheme.AddToScheme")
		return err
	}
	// Registers ClickHouseInstallation along with ClickHouseBackup/Schedule/Restore kinds.
	if err = api.AddToScheme(backupScheme); err != nil {
		backupLogger.Error(err, "init backup - unable to api.AddToScheme")
		return err
	}

	defaultNamespaces := make(map[string]cache.Config)
	for _, ns := range chop.Config().GetCacheNamespaces() {
		defaultNamespaces[ns] = cache.Config{}
	}
	backupManager, err = ctrlRuntime.NewManager(ctrlRuntime.GetConfigOrDie(), ctrlRuntime.Options{
		Scheme: backupScheme,
		Cache: cache.Options{
			DefaultNamespaces: defaultNamespaces,
		},
		// Disable the metrics listener: the keeper manager owns it on this pod.
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		backupLogger.Error(err, "init backup - unable to ctrlRuntime.NewManager")
		return err
	}

	recorder := backupManager.GetEventRecorderFor("clickhouse-backup")

	if err = ctrlRuntime.
		NewControllerManagedBy(backupManager).
		For(&api.ClickHouseBackup{}).
		Owns(&batchv1.Job{}).
		Complete(&backup.BackupController{
			Client:   backupManager.GetClient(),
			Scheme:   backupManager.GetScheme(),
			Recorder: recorder,
		}); err != nil {
		backupLogger.Error(err, "init backup - unable to build ClickHouseBackup controller")
		return err
	}

	if err = ctrlRuntime.
		NewControllerManagedBy(backupManager).
		For(&api.ClickHouseBackupSchedule{}).
		Owns(&batchv1.CronJob{}).
		Complete(&backup.ScheduleController{
			Client: backupManager.GetClient(),
			Scheme: backupManager.GetScheme(),
		}); err != nil {
		backupLogger.Error(err, "init backup - unable to build ClickHouseBackupSchedule controller")
		return err
	}

	if err = ctrlRuntime.
		NewControllerManagedBy(backupManager).
		For(&api.ClickHouseRestore{}).
		Owns(&batchv1.Job{}).
		Complete(&backup.RestoreController{
			Client:   backupManager.GetClient(),
			Scheme:   backupManager.GetScheme(),
			Recorder: recorder,
		}); err != nil {
		backupLogger.Error(err, "init backup - unable to build ClickHouseRestore controller")
		return err
	}

	// Bootstrap-from-backup: watch CHIs and auto-restore when annotated.
	if err = ctrlRuntime.
		NewControllerManagedBy(backupManager).
		For(&api.ClickHouseInstallation{}).
		Complete(&backup.BootstrapController{
			Client:   backupManager.GetClient(),
			Scheme:   backupManager.GetScheme(),
			Recorder: recorder,
		}); err != nil {
		backupLogger.Error(err, "init backup - unable to build bootstrap controller")
		return err
	}

	// Initialization successful
	return nil
}

func runBackup(ctx context.Context) error {
	if err := backupManager.Start(ctx); err != nil {
		backupLogger.Error(err, "run backup - unable to backupManager.Start")
		return err
	}
	// Run successful
	return nil
}
