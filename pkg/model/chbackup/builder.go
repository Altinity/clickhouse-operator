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
	"fmt"
	"strconv"

	batchv1 "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

const (
	// DefaultClientImage is the default image used by backup/restore trigger jobs.
	// It must provide the clickhouse-client binary used to submit commands to the
	// clickhouse-backup sidecar via the system.backup_actions integration table.
	// The clickhouse-server image is used (it bundles clickhouse-client) because the
	// standalone clickhouse/clickhouse-client image is deprecated and not multi-arch.
	// Overridable per CR via spec.image.
	DefaultClientImage = "clickhouse/clickhouse-server:24.8"

	// containerName is the name of the trigger container in the job pod.
	containerName = "clickhouse-backup-trigger"

	// LabelApp / LabelCRKind / LabelCRName tag generated jobs for observability.
	LabelApp    = "clickhouse.altinity.com/app"
	LabelCRKind = "clickhouse.altinity.com/cr-kind"
	LabelCRName = "clickhouse.altinity.com/cr-name"
	LabelChi    = "clickhouse.altinity.com/chi"
)

func int32Ptr(i int32) *int32 { return &i }

func labels(crKind, crName, chiName string) map[string]string {
	return map[string]string{
		LabelApp:    "clickhouse-backup",
		LabelCRKind: crKind,
		LabelCRName: crName,
		LabelChi:    chiName,
	}
}

func imageOrDefault(image string) string {
	if image != "" {
		return image
	}
	return DefaultClientImage
}

// podSpec builds the pod spec shared by backup and restore jobs.
func podSpec(image, credentialsSecretName, script string) core.PodSpec {
	c := core.Container{
		Name:    containerName,
		Image:   imageOrDefault(image),
		Command: []string{"bash", "-ec", script},
	}
	if credentialsSecretName != "" {
		c.EnvFrom = []core.EnvFromSource{
			{
				SecretRef: &core.SecretEnvSource{
					LocalObjectReference: core.LocalObjectReference{Name: credentialsSecretName},
				},
			},
		}
	}
	return core.PodSpec{
		RestartPolicy: core.RestartPolicyNever,
		Containers:    []core.Container{c},
	}
}

// ResolveBackupName returns the remote backup name for a one-off ClickHouseBackup.
// It is deterministic across reconciles (derived from the CR creation timestamp).
func ResolveBackupName(chb *api.ClickHouseBackup) string {
	if chb.Spec.BackupName != "" {
		return chb.Spec.BackupName
	}
	return fmt.Sprintf("%s-%d", chb.Name, chb.CreationTimestamp.Unix())
}

// BackupJobName returns the name of the Job created for a one-off backup.
func BackupJobName(chb *api.ClickHouseBackup) string { return chb.Name + "-backup" }

// CronJobName returns the name of the CronJob created for a backup schedule.
func CronJobName(chbs *api.ClickHouseBackupSchedule) string { return chbs.Name + "-backup" }

// RestoreJobName returns the name of the Job created for a restore.
func RestoreJobName(chr *api.ClickHouseRestore) string { return chr.Name + "-restore" }

// VerifyJobName returns the name of the Job created to verify a one-off backup.
func VerifyJobName(chb *api.ClickHouseBackup) string { return chb.Name + "-verify" }

// BuildBackupJob builds the one-off backup Job for a ClickHouseBackup CR.
func BuildBackupJob(chb *api.ClickHouseBackup, chi *api.ClickHouseInstallation) *batchv1.Job {
	top := Topology(chi)
	services := BackupServices(top, chb.Spec.GetReplicaSelection())
	script := BackupScript(services, strconv.Quote(ResolveBackupName(chb)), BackupOpts{
		SchemaOnly:     chb.Spec.SchemaOnly,
		Tables:         chb.Spec.Tables,
		Partitions:     chb.Spec.Partitions,
		DiffFromRemote: chb.Spec.DiffFromRemote,
		KeepLastRemote: chb.Spec.KeepLastRemote,
	})
	lbls := labels(api.ClickHouseBackupCRDResourceKind, chb.Name, chb.Spec.ClickHouseInstallation)

	return &batchv1.Job{
		ObjectMeta: meta.ObjectMeta{
			Name:      BackupJobName(chb),
			Namespace: chb.Namespace,
			Labels:    lbls,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: int32Ptr(0),
			Template: core.PodTemplateSpec{
				ObjectMeta: meta.ObjectMeta{Labels: lbls},
				Spec:       podSpec(chb.Spec.Image, chb.Spec.ClickHouseCredentialsSecretName, script),
			},
		},
	}
}

// BuildVerifyJob builds a Job that verifies a one-off backup is restorable (downloads it and
// checks integrity, without touching cluster data). Created by the controller when spec.verify.
func BuildVerifyJob(chb *api.ClickHouseBackup, chi *api.ClickHouseInstallation) *batchv1.Job {
	top := Topology(chi)
	services := FirstPerShardServices(top)
	script := VerifyScript(services, ResolveBackupName(chb))
	lbls := labels(api.ClickHouseBackupCRDResourceKind, chb.Name, chb.Spec.ClickHouseInstallation)

	return &batchv1.Job{
		ObjectMeta: meta.ObjectMeta{
			Name:      VerifyJobName(chb),
			Namespace: chb.Namespace,
			Labels:    lbls,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: int32Ptr(0),
			Template: core.PodTemplateSpec{
				ObjectMeta: meta.ObjectMeta{Labels: lbls},
				Spec:       podSpec(chb.Spec.Image, chb.Spec.ClickHouseCredentialsSecretName, script),
			},
		},
	}
}

// BuildBackupCronJob builds the recurring backup CronJob for a ClickHouseBackupSchedule CR.
func BuildBackupCronJob(chbs *api.ClickHouseBackupSchedule, chi *api.ClickHouseInstallation) *batchv1.CronJob {
	tmpl := chbs.Spec.BackupTemplate
	top := Topology(chi)
	services := BackupServices(top, tmpl.GetReplicaSelection())

	prefix := tmpl.BackupNamePrefix
	if prefix == "" {
		prefix = chbs.Name
	}
	// Each scheduled run computes a unique, timestamped backup name at runtime.
	backupNameExpr := "\"" + prefix + "-$(date -u +%Y%m%d-%H%M%S)\""
	script := BackupScript(services, backupNameExpr, BackupOpts{
		SchemaOnly:     tmpl.SchemaOnly,
		Tables:         tmpl.Tables,
		Partitions:     tmpl.Partitions,
		KeepLastRemote: tmpl.KeepLastRemote,
	})
	lbls := labels(api.ClickHouseBackupScheduleCRDResourceKind, chbs.Name, chbs.Spec.ClickHouseInstallation)

	concurrency := batchv1.ForbidConcurrent
	if chbs.Spec.ConcurrencyPolicy != "" {
		concurrency = batchv1.ConcurrencyPolicy(chbs.Spec.ConcurrencyPolicy)
	}
	successLimit := int32Ptr(3)
	if chbs.Spec.SuccessfulJobsHistoryLimit != nil {
		successLimit = chbs.Spec.SuccessfulJobsHistoryLimit
	}
	failedLimit := int32Ptr(1)
	if chbs.Spec.FailedJobsHistoryLimit != nil {
		failedLimit = chbs.Spec.FailedJobsHistoryLimit
	}
	suspend := false
	if chbs.Spec.Suspend != nil {
		suspend = *chbs.Spec.Suspend
	}

	return &batchv1.CronJob{
		ObjectMeta: meta.ObjectMeta{
			Name:      CronJobName(chbs),
			Namespace: chbs.Namespace,
			Labels:    lbls,
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                   chbs.Spec.Schedule,
			Suspend:                    &suspend,
			ConcurrencyPolicy:          concurrency,
			StartingDeadlineSeconds:    chbs.Spec.StartingDeadlineSeconds,
			SuccessfulJobsHistoryLimit: successLimit,
			FailedJobsHistoryLimit:     failedLimit,
			JobTemplate: batchv1.JobTemplateSpec{
				ObjectMeta: meta.ObjectMeta{Labels: lbls},
				Spec: batchv1.JobSpec{
					BackoffLimit: int32Ptr(0),
					Template: core.PodTemplateSpec{
						ObjectMeta: meta.ObjectMeta{Labels: lbls},
						Spec:       podSpec(tmpl.Image, tmpl.ClickHouseCredentialsSecretName, script),
					},
				},
			},
		},
	}
}

// BuildRestoreJob builds the one-off restore Job for a ClickHouseRestore CR.
func BuildRestoreJob(chr *api.ClickHouseRestore, chi *api.ClickHouseInstallation) *batchv1.Job {
	top := Topology(chi)
	// Schema and data are both restored on the first replica of each shard. For
	// Replicated* tables the sidecar must set restore_schema_on_cluster, so the schema
	// CREATE is issued ON CLUSTER from that one node and reaches every replica with an
	// identical ZooKeeper/Keeper path; native replication then clones the data to the
	// other replicas. Restoring schema independently on every replica is intentionally
	// avoided: clickhouse-backup rewrites the replica path per node, leaving replicas on
	// divergent paths that never sync.
	services := FirstPerShardServices(top)
	script := RestoreScript(services, services, chr.Spec.BackupName, chr.Spec.SchemaOnly, chr.Spec.Overwrite, chr.Spec.IsTopologyValidationEnabled())
	lbls := labels(api.ClickHouseRestoreCRDResourceKind, chr.Name, chr.Spec.ClickHouseInstallation)

	return &batchv1.Job{
		ObjectMeta: meta.ObjectMeta{
			Name:      RestoreJobName(chr),
			Namespace: chr.Namespace,
			Labels:    lbls,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: int32Ptr(0),
			Template: core.PodTemplateSpec{
				ObjectMeta: meta.ObjectMeta{Labels: lbls},
				Spec:       podSpec(chr.Spec.Image, chr.Spec.ClickHouseCredentialsSecretName, script),
			},
		},
	}
}
