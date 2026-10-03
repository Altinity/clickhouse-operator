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

	batchv1 "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/chbackup/metrics"
	"github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// RestoreController reconciles a ClickHouseRestore object into a restore Job.
type RestoreController struct {
	client.Client
	Scheme   *apiruntime.Scheme
	Recorder record.EventRecorder
}

// Reconcile drives a one-off ClickHouseRestore. It runs preflight validation, creates the
// restore Job (which itself enforces the overwrite guard and topology check before touching
// data), and tracks completion in the CR status.
func (c *RestoreController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	chr := &api.ClickHouseRestore{}
	if err := c.Get(ctx, req.NamespacedName, chr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if chr.Status.Phase == api.BackupPhaseCompleted || chr.Status.Phase == api.BackupPhaseFailed {
		return ctrl.Result{}, nil
	}

	// Preflight: restore requires the target CHI to exist and be Completed.
	chi, err := getCHI(ctx, c.Client, chr.Namespace, chr.Spec.ClickHouseInstallation)
	if err != nil {
		if apierrors.IsNotFound(err) {
			chr.Status.Phase = api.BackupPhasePending
			setCondition(&chr.Status.Conditions, api.ConditionValidated, metav1.ConditionFalse,
				"CHINotFound", "referenced ClickHouseInstallation not found", chr.Generation)
			return c.updateStatus(ctx, chr, ctrl.Result{RequeueAfter: requeueInterval})
		}
		return ctrl.Result{}, err
	}
	if !chiCompleted(chi) {
		chr.Status.Phase = api.BackupPhasePending
		setCondition(&chr.Status.Conditions, api.ConditionValidated, metav1.ConditionFalse,
			"CHINotReady", "referenced ClickHouseInstallation is not in Completed state", chr.Generation)
		return c.updateStatus(ctx, chr, ctrl.Result{RequeueAfter: requeueInterval})
	}
	setCondition(&chr.Status.Conditions, api.ConditionValidated, metav1.ConditionTrue, "Validated", "target CHI is ready", chr.Generation)
	if hasBackupSidecar(chi) {
		setCondition(&chr.Status.Conditions, "SidecarPresent", metav1.ConditionTrue, "SidecarFound", "clickhouse-backup sidecar detected", chr.Generation)
	} else {
		setCondition(&chr.Status.Conditions, "SidecarPresent", metav1.ConditionFalse, "SidecarMissing",
			"no clickhouse-backup sidecar detected in CHI pod templates; the restore job will fail without it", chr.Generation)
	}

	// Ensure the restore Job exists.
	job := &batchv1.Job{}
	err = c.Get(ctx, types.NamespacedName{Namespace: chr.Namespace, Name: chbackup.RestoreJobName(chr)}, job)
	switch {
	case apierrors.IsNotFound(err):
		job = chbackup.BuildRestoreJob(chr, chi)
		if err := ctrl.SetControllerReference(chr, job, c.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := c.Create(ctx, job); err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("created restore job", "job", job.Name)
		chr.Status.Phase = api.BackupPhaseRunning
		chr.Status.JobName = job.Name
		if chr.Status.StartTime == nil {
			now := metav1.Now()
			chr.Status.StartTime = &now
		}
		setCondition(&chr.Status.Conditions, api.ConditionJobCreated, metav1.ConditionTrue, "JobCreated", "restore job created", chr.Generation)
		metrics.RestoreStarted(ctx, chr.Namespace, chr.Spec.ClickHouseInstallation)
		c.event(chr, core.EventTypeNormal, "RestoreStarted", "restore job %s created", job.Name)
		return c.updateStatus(ctx, chr, ctrl.Result{RequeueAfter: requeueInterval})
	case err != nil:
		return ctrl.Result{}, err
	}

	ns, chiName := chr.Namespace, chr.Spec.ClickHouseInstallation
	switch {
	case jobComplete(job):
		if chr.Status.CompletionTime == nil {
			now := metav1.Now()
			chr.Status.CompletionTime = &now
			chr.Status.DurationSeconds = durationSeconds(chr.Status.StartTime, now)
			metrics.RestoreCompleted(ctx, ns, chiName)
			c.event(chr, core.EventTypeNormal, "RestoreCompleted", "restore of %s completed in %ds", chr.Spec.BackupName, chr.Status.DurationSeconds)
		}
		chr.Status.Phase = api.BackupPhaseCompleted
		setCondition(&chr.Status.Conditions, api.ConditionReady, metav1.ConditionTrue, "RestoreCompleted", "restore completed successfully", chr.Generation)
		return c.updateStatus(ctx, chr, ctrl.Result{})
	case jobFailed(job):
		if chr.Status.Phase != api.BackupPhaseFailed {
			metrics.RestoreFailed(ctx, ns, chiName)
			c.event(chr, core.EventTypeWarning, "RestoreFailed", "restore job failed; inspect job logs")
		}
		chr.Status.Phase = api.BackupPhaseFailed
		setCondition(&chr.Status.Conditions, api.ConditionReady, metav1.ConditionFalse, "JobFailed", "restore job failed; inspect job logs", chr.Generation)
		return c.updateStatus(ctx, chr, ctrl.Result{})
	default:
		chr.Status.Phase = api.BackupPhaseRunning
		return c.updateStatus(ctx, chr, ctrl.Result{RequeueAfter: requeueInterval})
	}
}

func (c *RestoreController) event(chr *api.ClickHouseRestore, eventType, reason, msgFmt string, args ...interface{}) {
	if c.Recorder != nil {
		c.Recorder.Eventf(chr, eventType, reason, msgFmt, args...)
	}
}

func (c *RestoreController) updateStatus(ctx context.Context, chr *api.ClickHouseRestore, result ctrl.Result) (ctrl.Result, error) {
	if err := c.Status().Update(ctx, chr); err != nil {
		return ctrl.Result{}, err
	}
	return result, nil
}
