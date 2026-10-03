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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// ScheduleController reconciles a ClickHouseBackupSchedule object into a managed CronJob.
type ScheduleController struct {
	client.Client
	Scheme *apiruntime.Scheme
}

// Reconcile ensures a CronJob exists and matches the ClickHouseBackupSchedule spec.
func (c *ScheduleController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	chbs := &api.ClickHouseBackupSchedule{}
	if err := c.Get(ctx, req.NamespacedName, chbs); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	chi, err := getCHI(ctx, c.Client, chbs.Namespace, chbs.Spec.ClickHouseInstallation)
	if err != nil {
		if apierrors.IsNotFound(err) {
			setCondition(&chbs.Status.Conditions, api.ConditionValidated, metav1.ConditionFalse,
				"CHINotFound", "referenced ClickHouseInstallation not found", chbs.Generation)
			if e := c.Status().Update(ctx, chbs); e != nil {
				return ctrl.Result{}, e
			}
			return ctrl.Result{RequeueAfter: requeueInterval}, nil
		}
		return ctrl.Result{}, err
	}
	setCondition(&chbs.Status.Conditions, api.ConditionValidated, metav1.ConditionTrue, "Validated", "target CHI found", chbs.Generation)

	desired := chbackup.BuildBackupCronJob(chbs, chi)
	if err := ctrl.SetControllerReference(chbs, desired, c.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	existing := &batchv1.CronJob{}
	err = c.Get(ctx, types.NamespacedName{Namespace: desired.Namespace, Name: desired.Name}, existing)
	switch {
	case apierrors.IsNotFound(err):
		if err := c.Create(ctx, desired); err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("created backup cronjob", "cronjob", desired.Name)
	case err != nil:
		return ctrl.Result{}, err
	default:
		existing.Spec = desired.Spec
		existing.Labels = desired.Labels
		if err := c.Update(ctx, existing); err != nil {
			return ctrl.Result{}, err
		}
		chbs.Status.LastScheduleTime = existing.Status.LastScheduleTime
	}

	chbs.Status.CronJobName = desired.Name
	setCondition(&chbs.Status.Conditions, api.ConditionJobCreated, metav1.ConditionTrue, "CronJobReady", "backup cronjob reconciled", chbs.Generation)
	setCondition(&chbs.Status.Conditions, api.ConditionReady, metav1.ConditionTrue, "Scheduled", "backup schedule is active", chbs.Generation)
	if err := c.Status().Update(ctx, chbs); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}
