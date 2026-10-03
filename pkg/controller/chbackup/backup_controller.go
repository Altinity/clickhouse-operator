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

// BackupController reconciles a ClickHouseBackup object into a backup Job.
type BackupController struct {
	client.Client
	Scheme   *apiruntime.Scheme
	Recorder record.EventRecorder
}

// Reconcile drives a one-off ClickHouseBackup: validate the target CHI, create the driving
// Job, track its completion, optionally verify it, and reflect everything in status, Events
// and Prometheus metrics.
func (c *BackupController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	chb := &api.ClickHouseBackup{}
	if err := c.Get(ctx, req.NamespacedName, chb); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Terminal phases are not re-processed. With verification enabled the phase only becomes
	// Completed once verification resolves, so this guard still lets us track the verify Job.
	if chb.Status.Phase == api.BackupPhaseCompleted || chb.Status.Phase == api.BackupPhaseFailed {
		return ctrl.Result{}, nil
	}

	ns, chiName := chb.Namespace, chb.Spec.ClickHouseInstallation

	// Preflight: the referenced CHI must exist and be Completed.
	chi, err := getCHI(ctx, c.Client, ns, chiName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			chb.Status.Phase = api.BackupPhasePending
			setCondition(&chb.Status.Conditions, api.ConditionValidated, metav1.ConditionFalse,
				"CHINotFound", "referenced ClickHouseInstallation not found", chb.Generation)
			return c.updateStatus(ctx, chb, ctrl.Result{RequeueAfter: requeueInterval})
		}
		return ctrl.Result{}, err
	}
	if !chiCompleted(chi) {
		chb.Status.Phase = api.BackupPhasePending
		setCondition(&chb.Status.Conditions, api.ConditionValidated, metav1.ConditionFalse,
			"CHINotReady", "referenced ClickHouseInstallation is not in Completed state", chb.Generation)
		return c.updateStatus(ctx, chb, ctrl.Result{RequeueAfter: requeueInterval})
	}
	setCondition(&chb.Status.Conditions, api.ConditionValidated, metav1.ConditionTrue, "Validated", "target CHI is ready", chb.Generation)
	c.surfaceSidecar(chb, chi)

	// Ensure the backup Job exists.
	job := &batchv1.Job{}
	err = c.Get(ctx, types.NamespacedName{Namespace: ns, Name: chbackup.BackupJobName(chb)}, job)
	switch {
	case apierrors.IsNotFound(err):
		job = chbackup.BuildBackupJob(chb, chi)
		if err := ctrl.SetControllerReference(chb, job, c.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := c.Create(ctx, job); err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("created backup job", "job", job.Name)
		chb.Status.Phase = api.BackupPhaseRunning
		chb.Status.JobName = job.Name
		chb.Status.BackupName = chbackup.ResolveBackupName(chb)
		if chb.Status.StartTime == nil {
			now := metav1.Now()
			chb.Status.StartTime = &now
		}
		setCondition(&chb.Status.Conditions, api.ConditionJobCreated, metav1.ConditionTrue, "JobCreated", "backup job created", chb.Generation)
		metrics.BackupStarted(ctx, ns, chiName)
		c.event(chb, core.EventTypeNormal, "BackupStarted", "backup job %s created", job.Name)
		return c.updateStatus(ctx, chb, ctrl.Result{RequeueAfter: requeueInterval})
	case err != nil:
		return ctrl.Result{}, err
	}

	// Job exists - track its completion.
	switch {
	case jobComplete(job):
		// First-time completion bookkeeping (runs once).
		if chb.Status.CompletionTime == nil {
			now := metav1.Now()
			chb.Status.CompletionTime = &now
			chb.Status.DurationSeconds = durationSeconds(chb.Status.StartTime, now)
			setCondition(&chb.Status.Conditions, api.ConditionReady, metav1.ConditionTrue, "BackupCompleted", "backup completed successfully", chb.Generation)
			metrics.BackupCompleted(ctx, ns, chiName, float64(chb.Status.DurationSeconds))
			c.event(chb, core.EventTypeNormal, "BackupCompleted", "backup %s completed in %ds", chb.Status.BackupName, chb.Status.DurationSeconds)
		}
		if chb.Spec.Verify {
			return c.trackVerify(ctx, chb, chi)
		}
		chb.Status.Phase = api.BackupPhaseCompleted
		return c.updateStatus(ctx, chb, ctrl.Result{})
	case jobFailed(job):
		if chb.Status.Phase != api.BackupPhaseFailed {
			metrics.BackupFailed(ctx, ns, chiName)
			c.event(chb, core.EventTypeWarning, "BackupFailed", "backup job failed; inspect job logs")
		}
		chb.Status.Phase = api.BackupPhaseFailed
		setCondition(&chb.Status.Conditions, api.ConditionReady, metav1.ConditionFalse, "JobFailed", "backup job failed; inspect job logs", chb.Generation)
		return c.updateStatus(ctx, chb, ctrl.Result{})
	default:
		chb.Status.Phase = api.BackupPhaseRunning
		return c.updateStatus(ctx, chb, ctrl.Result{RequeueAfter: requeueInterval})
	}
}

// trackVerify creates and tracks the verification Job (spec.verify). The backup data is already
// uploaded; verification is advisory, so a failed verification still leaves the backup Completed
// but with Verified=False.
func (c *BackupController) trackVerify(ctx context.Context, chb *api.ClickHouseBackup, chi *api.ClickHouseInstallation) (ctrl.Result, error) {
	ns, chiName := chb.Namespace, chb.Spec.ClickHouseInstallation

	vjob := &batchv1.Job{}
	err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: chbackup.VerifyJobName(chb)}, vjob)
	switch {
	case apierrors.IsNotFound(err):
		vjob = chbackup.BuildVerifyJob(chb, chi)
		if err := ctrl.SetControllerReference(chb, vjob, c.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := c.Create(ctx, vjob); err != nil {
			return ctrl.Result{}, err
		}
		setCondition(&chb.Status.Conditions, api.ConditionVerified, metav1.ConditionUnknown, "Verifying", "verifying backup integrity", chb.Generation)
		chb.Status.Phase = api.BackupPhaseRunning
		c.event(chb, core.EventTypeNormal, "VerifyStarted", "verification job %s created", vjob.Name)
		return c.updateStatus(ctx, chb, ctrl.Result{RequeueAfter: requeueInterval})
	case err != nil:
		return ctrl.Result{}, err
	}

	switch {
	case jobComplete(vjob):
		setCondition(&chb.Status.Conditions, api.ConditionVerified, metav1.ConditionTrue, "Verified", "backup verified restorable", chb.Generation)
		c.event(chb, core.EventTypeNormal, "Verified", "backup %s verified", chb.Status.BackupName)
		chb.Status.Phase = api.BackupPhaseCompleted
		return c.updateStatus(ctx, chb, ctrl.Result{})
	case jobFailed(vjob):
		setCondition(&chb.Status.Conditions, api.ConditionVerified, metav1.ConditionFalse, "VerificationFailed", "backup verification failed; inspect job logs", chb.Generation)
		metrics.VerificationFailed(ctx, ns, chiName)
		c.event(chb, core.EventTypeWarning, "VerificationFailed", "backup %s verification failed", chb.Status.BackupName)
		chb.Status.Phase = api.BackupPhaseCompleted // backup data exists; verification is advisory
		return c.updateStatus(ctx, chb, ctrl.Result{})
	default:
		chb.Status.Phase = api.BackupPhaseRunning
		return c.updateStatus(ctx, chb, ctrl.Result{RequeueAfter: requeueInterval})
	}
}

func (c *BackupController) surfaceSidecar(chb *api.ClickHouseBackup, chi *api.ClickHouseInstallation) {
	if hasBackupSidecar(chi) {
		setCondition(&chb.Status.Conditions, "SidecarPresent", metav1.ConditionTrue, "SidecarFound", "clickhouse-backup sidecar detected", chb.Generation)
	} else {
		setCondition(&chb.Status.Conditions, "SidecarPresent", metav1.ConditionFalse, "SidecarMissing",
			"no clickhouse-backup sidecar detected in CHI pod templates; the backup job will fail without it", chb.Generation)
	}
}

func (c *BackupController) event(chb *api.ClickHouseBackup, eventType, reason, msgFmt string, args ...interface{}) {
	if c.Recorder != nil {
		c.Recorder.Eventf(chb, eventType, reason, msgFmt, args...)
	}
}

func (c *BackupController) updateStatus(ctx context.Context, chb *api.ClickHouseBackup, result ctrl.Result) (ctrl.Result, error) {
	if err := c.Status().Update(ctx, chb); err != nil {
		return ctrl.Result{}, err
	}
	return result, nil
}
