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

	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

const (
	foreignEnv      = "--env=S3_PATH=backups/other/{cluster} --env=S3_OBJECT_DISK_PATH=objects/other/{cluster}"
	foreignDownload = "download --hardlink-exists-files " + foreignEnv + " b1"
)

// foreignRestoreSpec returns the spec of a restore of b1 on h0 and h1, one shard, from the storage paths of the
// installation "other", whose paths hold {installation}.
func foreignRestoreSpec() *model.RunSpec {
	spec := restoreSpec("h0", "h1")
	spec.SourceInstallation = "other"
	spec.PathCheck = &model.PathCheck{
		Installation: "demo", Namespace: "ns", Operation: api.BackupRunOperationRestore, ValidateTopology: true,
		Clusters: []model.PathCluster{{
			Name: "main", Shards: []string{"0"},
			Hosts: []model.PathHost{
				{Name: "h0", Macros: map[string]string{model.MacroReplica: "h0"}},
				{Name: "h1", Macros: map[string]string{model.MacroReplica: "h1"}},
			},
			Settings: map[string]*string{
				"REMOTE_STORAGE": ptr.To("s3"), "S3_BUCKET": ptr.To("bucket"),
				"S3_PATH": ptr.To("backups/{installation}/{cluster}"), "S3_OBJECT_DISK_PATH": ptr.To("objects/{installation}/{cluster}"),
			},
		}},
	}
	return spec
}

// earlierDownloads are the actions of an earlier run of the restore that downloaded the backup on the hosts given and
// failed.
func earlierDownloads(command string, hosts ...string) []api.BackupRunAction {
	var actions []api.BackupRunAction
	for _, host := range hosts {
		actions = append(actions, api.BackupRunAction{Step: stepDownload, Host: host, Command: command, TimeFrom: "2026-11-12 00:00:01",
			Status: "error"})
	}
	return actions
}

// A rerun of a restore from another installation with validateTopology resumes, by default, the partial download its
// earlier run left: its preflight checks that copy in place of downloading the schema over it, and deletes it not. A
// host its earlier run downloaded nothing on gets the copy of the schema as ever, deleted before the download.
func TestRestoreRerunFromAnotherInstallationResumesItsDownload(t *testing.T) {
	sc := newFakeSidecars("h0", "h1")
	sc.hosts["h0"].local = []backupRow{{Name: "b1", Location: "local", Desc: "broken (can't stat metadata.json)"}}
	spec := foreignRestoreSpec()
	spec.PreviousActions = earlierDownloads(foreignDownload, "h0")
	outcome, status, events := runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []string{
		"h1: download --schema --rbac " + foreignEnv + " b1",
		"h1: delete local b1",
		"h0: " + foreignDownload, "h1: " + foreignDownload,
		"h0: restore --schema=1 --ignore-dependencies " + foreignEnv + " --env=RESTORE_SCHEMA_ON_CLUSTER={cluster} b1",
		"h1: restore --schema=1 --ignore-dependencies " + foreignEnv + " b1",
		"h1: restore --data=1 --ignore-dependencies --replicated-copy-to-detached " + foreignEnv + " b1",
		"h0: restore --data=1 --ignore-dependencies " + foreignEnv + " b1",
		"h0: delete local b1", "h1: delete local b1",
	}, sc.commands())
	require.Equal(t, []string{"h0"}, status.Plan.Resumed)
	require.Empty(t, status.Plan.Replaced)
	require.NotContains(t, reasonsOf(events), "OrchestratorRerunReplaced")
	require.Contains(t, eventsOf(events, "OrchestratorHostStepSucceeded"), "h0: download b1 succeeded, resuming the local copy the host held")
	require.Contains(t, eventsOf(events, "OrchestratorHostStepSucceeded"), "h1: download b1 succeeded")

	// A copy no run of the restore downloaded is refused as ever
	foreign := newFakeSidecars("h0", "h1")
	foreign.hosts["h1"].local = []backupRow{{Name: "b1", Location: "local", Desc: "broken (can't stat metadata.json)"}}
	outcome, _, _ = runOf(t, foreign, spec, nil)
	require.True(t, outcome.failed)
	require.Contains(t, outcome.message, "preflight failed: h1 holds a local copy of b1")
	require.Empty(t, commandsHolding(foreign, "delete"))
}

// Without resumeDownload a rerun of a restore from another installation with validateTopology deletes the local copy its
// earlier run downloaded before its preflight, and downloads the backup again from scratch.
func TestRestoreRerunFromAnotherInstallationStartsOver(t *testing.T) {
	sc := newFakeSidecars("h0", "h1")
	sc.hosts["h0"].local = []backupRow{{Name: "b1", Location: "local", Desc: "broken (can't stat metadata.json)"}}
	spec := foreignRestoreSpec()
	spec.ResumeDownload = false
	spec.PreviousActions = earlierDownloads(foreignDownload, "h0")
	outcome, status, events := runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []string{
		"h0: delete local b1",
		"h0: download --schema --rbac " + foreignEnv + " b1", "h1: download --schema --rbac " + foreignEnv + " b1",
		"h0: delete local b1", "h1: delete local b1",
		"h0: " + foreignDownload, "h1: " + foreignDownload,
	}, sc.commands()[:7])
	require.Equal(t, []string{"h0"}, status.Plan.Replaced)
	require.Empty(t, status.Plan.Resumed)
	require.Equal(t, []string{"h0: the local copy of b1 an earlier run of the restore downloaded deleted, the download starts over"},
		eventsOf(events, "OrchestratorRerunReplaced"))
	require.Contains(t, eventsOf(events, "OrchestratorHostStepSucceeded"),
		"h0: download b1 succeeded, downloaded again in place of the local copy an earlier run of the restore downloaded")
}

// Without resumeDownload a rerun of a restore of the own backups deletes the local copies its earlier run downloaded,
// complete or not, and downloads the backup again; by default it resumes the incomplete one and keeps the complete one.
func TestRestoreRerunStartsOverWithoutResumeDownload(t *testing.T) {
	setup := func() *fakeSidecars {
		sc := newFakeSidecars("h0", "h1")
		for _, host := range []string{"h0", "h1"} {
			sc.hosts[host].remote = []backupRow{{Name: "b1", DataSize: 1, MetadataSize: 1}}
		}
		sc.hosts["h0"].local = []backupRow{{Name: "b1", Location: "local", Desc: "broken (can't stat metadata.json)"}}
		sc.hosts["h1"].local = []backupRow{{Name: "b1", Location: "local", DataSize: 100, MetadataSize: 10}}
		return sc
	}
	spec := restoreSpec("h0", "h1")
	spec.ResumeDownload = false
	spec.PreviousActions = earlierDownloads("download --hardlink-exists-files b1", "h0", "h1")
	sc := setup()
	outcome, status, events := runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []string{
		"h0: delete local b1", "h1: delete local b1",
		"h0: download --hardlink-exists-files b1", "h1: download --hardlink-exists-files b1",
	}, sc.commands()[:4])
	require.Equal(t, []string{"h0", "h1"}, status.Plan.Replaced)
	require.Len(t, eventsOf(events, "OrchestratorRerunReplaced"), 2)

	spec.ResumeDownload = true
	sc = setup()
	outcome, status, events = runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []string{"h0: download --hardlink-exists-files b1"}, commandsHolding(sc, "download"),
		"the incomplete copy is resumed, the complete one kept")
	require.Equal(t, []string{"h0"}, status.Plan.Resumed)
	require.Empty(t, eventsOf(events, "OrchestratorRerunReplaced"))
}

// The first run of a restore, which no earlier run precedes, is the same with resumeDownload or without.
func TestRestoreFirstRunIgnoresResumeDownload(t *testing.T) {
	var commands [][]string
	for _, resume := range []bool{true, false} {
		for _, foreign := range []bool{false, true} {
			sc := newFakeSidecars("h0", "h1")
			spec := foreignRestoreSpec()
			if !foreign {
				spec = restoreSpec("h0", "h1")
				for _, host := range []string{"h0", "h1"} {
					sc.hosts[host].remote = []backupRow{{Name: "b1", DataSize: 1, MetadataSize: 1}}
				}
			}
			spec.ResumeDownload = resume
			outcome, status, events := runOf(t, sc, spec, nil)
			require.False(t, outcome.failed, outcome.message)
			require.Empty(t, status.Plan.Replaced)
			require.Empty(t, eventsOf(events, "OrchestratorRerunReplaced"))
			commands = append(commands, sc.commands())
		}
	}
	require.Equal(t, commands[0], commands[2])
	require.Equal(t, commands[1], commands[3])
}
