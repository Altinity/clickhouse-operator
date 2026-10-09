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

// Package chbackup contains the controller-runtime controllers that reconcile the
// ClickHouseBackup, ClickHouseBackupSchedule and ClickHouseRestore custom resources
// into Kubernetes Jobs/CronJobs driving clickhouse-backup.
package chbackup

import (
	"context"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// requeueInterval is how often a controller re-checks an in-flight Job's completion.
const requeueInterval = 15 * time.Second

// getCHI fetches the referenced ClickHouseInstallation in the given namespace.
func getCHI(ctx context.Context, c client.Client, namespace, name string) (*api.ClickHouseInstallation, error) {
	chi := &api.ClickHouseInstallation{}
	err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, chi)
	return chi, err
}

// chiCompleted reports whether the CHI has finished reconciling successfully.
func chiCompleted(chi *api.ClickHouseInstallation) bool {
	return chi != nil && chi.Status != nil && chi.Status.GetStatus() == api.StatusCompleted
}

// hasBackupSidecar reports, best-effort, whether the CHI pod templates include a
// clickhouse-backup sidecar container. Used to surface a (non-blocking) warning condition.
func hasBackupSidecar(chi *api.ClickHouseInstallation) bool {
	if chi == nil || chi.Spec.Templates == nil {
		return false
	}
	for _, pt := range chi.Spec.Templates.PodTemplates {
		for _, container := range pt.Spec.Containers {
			if strings.Contains(container.Image, "clickhouse-backup") || strings.Contains(container.Name, "backup") {
				return true
			}
		}
	}
	return false
}

// setCondition upserts a status condition.
func setCondition(conditions *[]metav1.Condition, condType string, status metav1.ConditionStatus, reason, message string, generation int64) {
	apimeta.SetStatusCondition(conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	})
}

// jobConditionTrue reports whether a Job carries the given condition with status True.
func jobConditionTrue(job *batchv1.Job, condType batchv1.JobConditionType) bool {
	for _, cond := range job.Status.Conditions {
		if cond.Type == condType && cond.Status == core.ConditionTrue {
			return true
		}
	}
	return false
}

func jobComplete(job *batchv1.Job) bool { return jobConditionTrue(job, batchv1.JobComplete) }
func jobFailed(job *batchv1.Job) bool   { return jobConditionTrue(job, batchv1.JobFailed) }

// durationSeconds returns end-start in whole seconds (>=0), or 0 if start is unset.
func durationSeconds(start *metav1.Time, end metav1.Time) int64 {
	if start == nil {
		return 0
	}
	d := int64(end.Time.Sub(start.Time).Seconds())
	if d < 0 {
		return 0
	}
	return d
}
