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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// namingTopology is testTopology with the naming template of the Altinity Cloud Manager on its cluster
func namingTopology(template string) *Topology {
	topology := testTopology()
	topology.clusters[0].naming = template
	topology.enabled[0].naming = template
	return topology
}

// Every Job of a cluster with a naming template tells the script the template, the number of the shard of every host
// it may reach - every replica over the sidecars, the backup Pod of every shard with the pod transport - and the values
// of {chi} and {namespace}; a cluster without one tells nothing of it, as before.
func TestJobsTellTheNamingTemplate(t *testing.T) {
	topology := namingTopology(api.BackupNamingACM)
	chb := testBackup("b1")
	chb.Status.BackupName = "shard1-20231114T221320"
	for name, env := range map[string][]core.EnvVar{
		"backup":   BuildBackupJob(chb, testInstallation(), topology, testOrchestrator()).Spec.Template.Spec.Containers[0].Env,
		"verify":   BuildVerifyJob(chb, testInstallation(), topology, testOrchestrator()).Spec.Template.Spec.Containers[0].Env,
		"restore":  BuildRestoreJob(testRestore("r1"), testInstallation(), topology, testOrchestrator()).Spec.Template.Spec.Containers[0].Env,
		"schedule": BuildBackupCronJob(testSchedule("s1"), testInstallation(), topology, testOrchestrator(), false).Spec.JobTemplate.Spec.Template.Spec.Containers[0].Env,
	} {
		values := envValues(env)
		require.Equal(t, api.BackupNamingACM, values[envBackupNaming], name)
		require.Equal(t, "chi-demo-main-0-0=1 chi-demo-main-0-1=1 chi-demo-main-1-0=2 chi-demo-main-1-1=2", values[envShardNumbers], name)
		require.Equal(t, "demo", values[envNamingInstallation], name)
		require.Equal(t, testNamespace, values[envNamingNamespace], name)
	}
	pods := testOrchestrator()
	pods.Transport = api.BackupModePod
	values := envValues(BuildBackupJob(chb, testInstallation(), topology, pods).Spec.Template.Spec.Containers[0].Env)
	require.Equal(t, ShardPodHost(testNamespace, "demo", "main", "0")+"=1 "+ShardPodHost(testNamespace, "demo", "main", "1")+"=2",
		values[envShardNumbers])

	values = envValues(BuildBackupJob(chb, testInstallation(), testTopology(), testOrchestrator()).Spec.Template.Spec.Containers[0].Env)
	for _, name := range []string{envBackupNaming, envShardNumbers, envNamingInstallation, envNamingNamespace} {
		require.NotContains(t, values, name)
	}
}

// envValues returns the plain values of env by name
func envValues(env []core.EnvVar) map[string]string {
	values := map[string]string{}
	for _, v := range env {
		if v.ValueFrom == nil {
			values[v.Name] = v.Value
		}
	}
	return values
}

// A backup of a cluster with a naming template is named as the template names the backup of the first shard: after
// the time it is due, with a T before the hour, as it is made at once, or the time its backupName gives, and the
// backup name prefix of the schedule it runs, or its own name, for {prefix}. A scheduled run is named after its slot.
func TestBackupNamesOfANamingTemplate(t *testing.T) {
	naming := namingTopology("{prefix}-shard{shard}-{YYYYMMDDHHIISS}").BackupNaming()
	chb := testBackup("b1")
	name, err := ResolveTemplateBackupName(chb, naming)
	require.NoError(t, err)
	require.Equal(t, "b1-shard1-20231114T221320", name)

	chb.Spec.BackupName = "20260101020304"
	name, err = ResolveTemplateBackupName(chb, naming)
	require.NoError(t, err)
	require.Equal(t, "b1-shard1-20260101020304", name)
	chb.Spec.BackupName = "nightly"
	_, err = ResolveTemplateBackupName(chb, naming)
	require.ErrorContains(t, err, `backupName "nightly" is no time a backup is named after`)

	run := testBackup("now")
	run.Spec.ScheduleRef = &api.BackupScheduleRef{Name: "s1"}
	run.Status.Schedule = &api.BackupScheduleRecord{BackupNamePrefix: "nightly"}
	name, err = ResolveTemplateBackupName(run, naming)
	require.NoError(t, err)
	require.Equal(t, "nightly-shard1-20231114T221320", name)

	acm := namingTopology(api.BackupNamingACM)
	chbs := testSchedule("s1")
	job := scheduledRun(chbs, time.Date(2026, time.November, 13, 2, 0, 0, 0, time.UTC))
	require.Equal(t, BackupNamePrefix(chbs)+"-20261113020000", ScheduledBackupName(job), "the default naming")
	cronJob := BuildBackupCronJob(chbs, testInstallation(), acm, testOrchestrator(), false)
	job.Spec = cronJob.Spec.JobTemplate.Spec
	job.Spec.Template.Labels = map[string]string{LegacyJobNameLabel: job.Name}
	require.Equal(t, "shard1-20261113020000", ScheduledBackupName(job))
	require.Equal(t, "shard1-20261113020000", JobBackupName(job))
}
