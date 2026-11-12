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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// Every Job pod runs the script of the ConfigMap, mounted read-only, as the orchestrator ServiceAccount, in the image
// of the clickhouse-backup container pulled anew; its template carries the hash of the script.
func TestJobsRunTheOrchestratorScriptOfTheConfigMap(t *testing.T) {
	orchestrator := testOrchestrator()
	for name, template := range map[string]core.PodTemplateSpec{
		"backup":  BuildBackupJob(testBackup("b1"), testInstallation(), testTopology(), orchestrator).Spec.Template,
		"verify":  BuildVerifyJob(testBackup("b1"), testInstallation(), testTopology(), orchestrator).Spec.Template,
		"restore": BuildRestoreJob(testRestore("r1"), testInstallation(), testTopology(), orchestrator).Spec.Template,
		"cronjob": BuildBackupCronJob(testSchedule("s1"), testInstallation(), testTopology(), orchestrator, false).Spec.JobTemplate.Spec.Template,
	} {
		t.Run(name, func(t *testing.T) {
			spec := template.Spec
			c := container(t, spec)
			require.Equal(t, "backup-orchestrator", c.Name)
			require.Equal(t, testImage, c.Image)
			require.Equal(t, core.PullAlways, c.ImagePullPolicy)
			require.Equal(t, []string{"bash", "/etc/clickhouse-backup-orchestrator/backup-orchestrator.sh"}, c.Command)
			require.Equal(t, []core.VolumeMount{{Name: orchestratorVolumeName, MountPath: OrchestratorMountPath, ReadOnly: true}}, c.VolumeMounts)
			require.Len(t, spec.Volumes, 1)
			require.Equal(t, orchestratorVolumeName, spec.Volumes[0].Name)
			require.NotNil(t, spec.Volumes[0].ConfigMap)
			require.Equal(t, orchestrator.ConfigMap, spec.Volumes[0].ConfigMap.Name)
			require.Equal(t, []core.KeyToPath{{Key: api.BackupOrchestratorScriptKey, Path: OrchestratorScriptFile}}, spec.Volumes[0].ConfigMap.Items)
			require.Equal(t, orchestrator.ServiceAccountName, spec.ServiceAccountName)
			require.Equal(t, orchestrator.ScriptSHA256, template.Annotations[AnnotationOrchestratorScriptSHA256])
		})
	}
}

// A change of the script changes the CronJob, whose next run takes it.
func TestScriptChangeRebuildsTheCronJob(t *testing.T) {
	version := func(orchestrator Orchestrator) string {
		cronJob := BuildBackupCronJob(testSchedule("s1"), testInstallation(), testTopology(), orchestrator, false)
		return cronJob.Annotations[AnnotationBackupObjectVersion]
	}
	changed := testOrchestrator()
	changed.ScriptSHA256 = ScriptSHA256("echo changed")
	require.NotEqual(t, version(testOrchestrator()), version(changed))
	require.Equal(t, version(testOrchestrator()), version(testOrchestrator()))
}

// The script posts its Events on the custom resource the Job works for, from the namespace and name of its pod.
func TestJobsNameTheEventObject(t *testing.T) {
	chb, chr, chbs := testBackup("b1"), testRestore("r1"), testSchedule("s1")
	chb.UID, chr.UID, chbs.UID = "uid-b", "uid-r", "uid-s"
	for name, tc := range map[string]struct {
		spec       core.PodSpec
		kind, name string
		uid        string
	}{
		"backup":  {BuildBackupJob(chb, testInstallation(), testTopology(), testOrchestrator()).Spec.Template.Spec, api.ClickHouseBackupCRDResourceKind, "b1", "uid-b"},
		"verify":  {BuildVerifyJob(chb, testInstallation(), testTopology(), testOrchestrator()).Spec.Template.Spec, api.ClickHouseBackupCRDResourceKind, "b1", "uid-b"},
		"restore": {BuildRestoreJob(chr, testInstallation(), testTopology(), testOrchestrator()).Spec.Template.Spec, api.ClickHouseRestoreCRDResourceKind, "r1", "uid-r"},
		"cronjob": {BuildBackupCronJob(chbs, testInstallation(), testTopology(), testOrchestrator(), false).Spec.JobTemplate.Spec.Template.Spec,
			api.ClickHouseBackupScheduleCRDResourceKind, "s1", "uid-s"},
	} {
		t.Run(name, func(t *testing.T) {
			c := container(t, tc.spec)
			values := env(c)
			require.Equal(t, api.SchemeGroupVersion.String(), values[envEventObjectAPIVersion])
			require.Equal(t, tc.kind, values[envEventObjectKind])
			require.Equal(t, tc.name, values[envEventObjectName])
			require.Equal(t, tc.uid, values[envEventObjectUID])
			require.Equal(t, "metadata.namespace", envVarNamed(t, c, envPodNamespace).ValueFrom.FieldRef.FieldPath)
			require.Equal(t, "metadata.name", envVarNamed(t, c, envPodName).ValueFrom.FieldRef.FieldPath)
		})
	}
}

// With the pod transport every host list names the backup Pod of every shard, one per shard.
func TestPodTransportNamesTheShardPods(t *testing.T) {
	orchestrator := testOrchestrator()
	orchestrator.Transport = api.BackupModePod
	orchestrator.ShardPodHosts = []string{"pod-0.svc", "pod-1.svc"}
	chb := testBackup("b1")
	chb.Spec.ReplicaSelection = api.ReplicaSelectionAllReplicas
	chbs := testSchedule("s1")
	chbs.Spec.ReplicaSelection = api.ReplicaSelectionAllReplicas

	backup := env(container(t, BuildBackupJob(chb, testInstallation(), testTopology(), orchestrator).Spec.Template.Spec))
	require.Equal(t, backupTransportPod, backup[envBackupTransport])
	require.Equal(t, "pod-0.svc pod-1.svc", backup[envBackupHosts])
	verify := env(container(t, BuildVerifyJob(chb, testInstallation(), testTopology(), orchestrator).Spec.Template.Spec))
	require.Equal(t, "pod-0.svc pod-1.svc", verify[envVerifyHosts])
	restore := env(container(t, BuildRestoreJob(testRestore("r1"), testInstallation(), testTopology(), orchestrator).Spec.Template.Spec))
	for _, name := range []string{envSchemaHosts, envDataHosts, envGuardHosts} {
		require.Equal(t, "pod-0.svc pod-1.svc", restore[name], name)
	}
	scheduled := env(container(t, BuildBackupCronJob(chbs, testInstallation(), testTopology(), orchestrator, false).Spec.JobTemplate.Spec.Template.Spec))
	require.Equal(t, "pod-0.svc pod-1.svc", scheduled[envBackupHosts])
	require.Equal(t, "pod-0.svc pod-1.svc", scheduled[envRetentionHosts])
	require.Equal(t, []string{"pod-0.svc", "pod-1.svc"}, RetentionHosts(chbs, testTopology(), orchestrator))

	sidecar := env(container(t, BuildBackupJob(chb, testInstallation(), testTopology(), testOrchestrator()).Spec.Template.Spec))
	require.Equal(t, backupTransportSidecar, sidecar[envBackupTransport])
	require.Equal(t, "chi-demo-main-0-0 chi-demo-main-0-1 chi-demo-main-1-0 chi-demo-main-1-1", sidecar[envBackupHosts])
}

// A scheduled run skips retention while a restore of the cluster is not finished.
func TestCronJobTellsARestoreInProgress(t *testing.T) {
	orchestrator := testOrchestrator()
	values := env(container(t, BuildBackupCronJob(testSchedule("s1"), testInstallation(), testTopology(), orchestrator, false).Spec.JobTemplate.Spec.Template.Spec))
	require.Equal(t, "false", values[envRestoreInProgress])
	orchestrator.RestoreInProgress = true
	values = env(container(t, BuildBackupCronJob(testSchedule("s1"), testInstallation(), testTopology(), orchestrator, true).Spec.JobTemplate.Spec.Template.Spec))
	require.Equal(t, "true", values[envRestoreInProgress])
	require.NotContains(t, env(container(t, BuildBackupJob(testBackup("b1"), testInstallation(), testTopology(), orchestrator).Spec.Template.Spec)),
		envRestoreInProgress, "a one-off backup prunes nothing")
}

// The operator keeps the script of a cluster in a ConfigMap of the installation, garbage-collected with it.
func TestOrchestratorConfigMap(t *testing.T) {
	chi := testInstallation()
	chi.UID = "chi-uid"
	configMap := BuildOrchestratorConfigMap(chi, "main", "echo script")
	require.Equal(t, "demo-main-backup-orchestrator", configMap.Name)
	require.Equal(t, chi.Namespace, configMap.Namespace)
	require.Equal(t, map[string]string{api.BackupOrchestratorScriptKey: "echo script"}, configMap.Data)
	require.Equal(t, ScriptSHA256("echo script"), configMap.Annotations[AnnotationOrchestratorScriptSHA256])
	require.Len(t, configMap.OwnerReferences, 1)
	require.Equal(t, chi.UID, configMap.OwnerReferences[0].UID)
	require.True(t, *configMap.OwnerReferences[0].Controller)

	long := OrchestratorConfigMapName(strings.Repeat("c", 200), strings.Repeat("k", 100))
	require.LessOrEqual(t, len(long), validation.DNS1123SubdomainMaxLength)
	require.True(t, strings.HasSuffix(long, "-backup-orchestrator"))
	require.Len(t, ScriptSHA256("x"), 64)
}

func TestClusterBackupOf(t *testing.T) {
	chi := newCHI("demo")
	chi.Spec.Configuration = &api.Configuration{Clusters: []*api.Cluster{{Name: "main", Backup: &api.ClusterBackup{Mode: types.NewString(api.BackupModePod)}}}}
	require.Equal(t, api.BackupModePod, ClusterBackupOf(chi, "main").GetMode())
	require.Nil(t, ClusterBackupOf(chi, "other"))
	require.Nil(t, ClusterBackupOf(nil, "main"))
	require.Equal(t, api.BackupModeSidecar, ClusterBackupOf(chi, "other").GetMode())
}

// The slot of a scheduled run is the minute its Job name tells, in the time zone of the schedule.
func TestScheduledSlot(t *testing.T) {
	chbs := testSchedule("s1")
	chbs.Spec.TimeZone = "Europe/Berlin"
	cronJob := BuildBackupCronJob(chbs, testInstallation(), testTopology(), testOrchestrator(), false)
	due := time.Date(2026, 3, 1, 3, 0, 0, 0, time.UTC)
	run := &batch.Job{Spec: *cronJob.Spec.JobTemplate.Spec.DeepCopy()}
	run.Name = cronJob.Name + "-" + strconv.FormatInt(due.Unix()/60, 10)
	run.Spec.Template.Labels = map[string]string{LegacyJobNameLabel: run.Name}

	slot, ok := ScheduledSlot(run)
	require.True(t, ok)
	require.True(t, slot.Equal(due))
	require.Equal(t, "Europe/Berlin", slot.Location().String())
	require.True(t, strings.HasSuffix(ScheduledBackupName(run), "-20260301040000"))

	run.Spec.Template.Labels = nil
	_, ok = ScheduledSlot(run)
	require.False(t, ok, "a pod without the Job name label names its backup after the time it runs")
	_, ok = ScheduledSlot(BuildBackupJob(testBackup("b1"), testInstallation(), testTopology(), testOrchestrator()))
	require.False(t, ok, "a one-off backup is no scheduled run")
}
