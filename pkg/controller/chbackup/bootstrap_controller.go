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

package chbackup

import (
	"context"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

const (
	// AnnotationRecoverFromBackup, set on a fresh ClickHouseInstallation, makes the operator
	// auto-restore the named remote backup once the cluster is up (CloudNativePG-style bootstrap).
	AnnotationRecoverFromBackup = "clickhouse.altinity.com/recover-from-backup"
	// AnnotationRecoveredFrom is the guard the operator stamps after triggering the recovery,
	// so the bootstrap restore fires exactly once.
	AnnotationRecoveredFrom = "clickhouse.altinity.com/recovered-from"
	// AnnotationRecoverCredentialsSecret optionally names the Secret (CLICKHOUSE_USER/PASSWORD)
	// the bootstrap restore should use to authenticate to ClickHouse.
	AnnotationRecoverCredentialsSecret = "clickhouse.altinity.com/recover-credentials-secret"
)

// BootstrapController watches ClickHouseInstallations and, when one carries the
// recover-from-backup annotation and has finished its first reconcile, creates a one-time
// ClickHouseRestore. It only ever reads the CHI and stamps a guard annotation; it never
// mutates the CHI spec or its children, so it does not conflict with the main CHI controller.
type BootstrapController struct {
	client.Client
	Scheme   *apiruntime.Scheme
	Recorder record.EventRecorder
}

// Reconcile implements the bootstrap-from-backup behavior.
func (c *BootstrapController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	chi := &api.ClickHouseInstallation{}
	if err := c.Get(ctx, req.NamespacedName, chi); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	backupName := chi.GetAnnotations()[AnnotationRecoverFromBackup]
	if backupName == "" {
		return ctrl.Result{}, nil // not a bootstrap CHI
	}
	if chi.GetAnnotations()[AnnotationRecoveredFrom] != "" {
		return ctrl.Result{}, nil // already recovered once
	}
	if !chiCompleted(chi) {
		// Wait until the cluster (and its sidecars) are up before restoring.
		return ctrl.Result{RequeueAfter: requeueInterval}, nil
	}

	restoreName := chi.Name + "-bootstrap"
	restore := &api.ClickHouseRestore{
		ObjectMeta: meta.ObjectMeta{Name: restoreName, Namespace: chi.Namespace},
		Spec: api.ClickHouseRestoreSpec{
			ClickHouseInstallation:          chi.Name,
			BackupName:                      backupName,
			ClickHouseCredentialsSecretName: chi.GetAnnotations()[AnnotationRecoverCredentialsSecret],
		},
	}
	if err := ctrl.SetControllerReference(chi, restore, c.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := c.Create(ctx, restore); err != nil && !apierrors.IsAlreadyExists(err) {
		return ctrl.Result{}, err
	}
	logger.Info("created bootstrap restore", "restore", restoreName, "backup", backupName)

	// Stamp the guard annotation so this fires exactly once.
	base := chi.DeepCopy()
	if chi.Annotations == nil {
		chi.Annotations = map[string]string{}
	}
	chi.Annotations[AnnotationRecoveredFrom] = backupName
	if err := c.Patch(ctx, chi, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, err
	}
	if c.Recorder != nil {
		c.Recorder.Eventf(chi, core.EventTypeNormal, "BootstrapRestore",
			"created ClickHouseRestore %s from backup %s", restoreName, backupName)
	}
	return ctrl.Result{}, nil
}
