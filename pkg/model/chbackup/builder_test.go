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
	"strings"
	"testing"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

func testCHI(name, cluster string, shards, replicas int) *api.ClickHouseInstallation {
	return &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{Name: name, Namespace: "ns"},
		Spec: api.ChiSpec{
			Configuration: &api.Configuration{
				Clusters: []*api.Cluster{
					{
						Name:   cluster,
						Layout: &api.ChiClusterLayout{ShardsCount: shards, ReplicasCount: replicas},
					},
				},
			},
		},
	}
}

func TestTopologyServiceNames(t *testing.T) {
	chi := testCHI("demo", "default", 2, 2)
	top := Topology(chi)

	all := AllServices(top)
	want := []string{
		"chi-demo-default-0-0", "chi-demo-default-0-1",
		"chi-demo-default-1-0", "chi-demo-default-1-1",
	}
	if strings.Join(all, ",") != strings.Join(want, ",") {
		t.Fatalf("AllServices = %v, want %v", all, want)
	}

	first := FirstPerShardServices(top)
	wantFirst := []string{"chi-demo-default-0-0", "chi-demo-default-1-0"}
	if strings.Join(first, ",") != strings.Join(wantFirst, ",") {
		t.Fatalf("FirstPerShardServices = %v, want %v", first, wantFirst)
	}

	shardsN, replicasN := Counts(top)
	if shardsN != 2 || replicasN != 2 {
		t.Fatalf("Counts = (%d,%d), want (2,2)", shardsN, replicasN)
	}
}

func TestLayoutDefaultsToSingleHost(t *testing.T) {
	// shardsCount/replicasCount omitted -> default 1x1.
	chi := &api.ClickHouseInstallation{
		ObjectMeta: meta.ObjectMeta{Name: "demo"},
		Spec: api.ChiSpec{Configuration: &api.Configuration{
			Clusters: []*api.Cluster{{Name: "default"}},
		}},
	}
	all := AllServices(Topology(chi))
	if len(all) != 1 || all[0] != "chi-demo-default-0-0" {
		t.Fatalf("AllServices = %v, want [chi-demo-default-0-0]", all)
	}
}

func TestBackupServicesSelection(t *testing.T) {
	top := Topology(testCHI("demo", "default", 2, 2))
	if got := BackupServices(top, api.ReplicaSelectionAllReplicas); len(got) != 4 {
		t.Fatalf("AllReplicas selection = %d services, want 4", len(got))
	}
	if got := BackupServices(top, api.ReplicaSelectionFirstPerShard); len(got) != 2 {
		t.Fatalf("FirstPerShard selection = %d services, want 2", len(got))
	}
	// Empty selection must default to FirstPerShard.
	if got := BackupServices(top, ""); len(got) != 2 {
		t.Fatalf("default selection = %d services, want 2", len(got))
	}
}

func TestBackupScript(t *testing.T) {
	s := BackupScript([]string{"svc-a", "svc-b"}, `"my-backup"`, BackupOpts{})
	for _, want := range []string{"system.backup_actions", "create_remote", "svc-a svc-b", `BACKUP_NAME="my-backup"`} {
		if !strings.Contains(s, want) {
			t.Errorf("backup script missing %q", want)
		}
	}
	if strings.Contains(s, "--schema") {
		t.Errorf("non-schema-only backup should not pass --schema")
	}

	schemaOnly := BackupScript([]string{"svc-a"}, `"x"`, BackupOpts{SchemaOnly: true})
	if !strings.Contains(schemaOnly, "create_remote --schema") {
		t.Errorf("schemaOnly backup must pass --schema")
	}
}

func TestRestoreScriptSafety(t *testing.T) {
	schema := []string{"chi-demo-default-0-0", "chi-demo-default-0-1"}
	data := []string{"chi-demo-default-0-0"}

	// overwrite=false, validateTopology=true: guards present, no --rm.
	s := RestoreScript(schema, data, "bk", false, false, true)
	for _, want := range []string{
		`BACKUP_NAME="bk"`,
		"restore_remote --schema ${BACKUP_NAME}",
		"restore_remote --data ${BACKUP_NAME}",
		"overwrite guard",
		"validating target topology",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("restore script missing %q", want)
		}
	}
	if strings.Contains(s, "--rm") {
		t.Errorf("non-overwrite restore must not use --rm")
	}

	// overwrite=true: --rm present, guard absent.
	s2 := RestoreScript(schema, data, "bk", false, true, true)
	if !strings.Contains(s2, "restore_remote --schema --rm ${BACKUP_NAME}") {
		t.Errorf("overwrite restore must use --rm")
	}
	if strings.Contains(s2, "overwrite guard") {
		t.Errorf("overwrite restore must skip the non-empty guard")
	}

	// schemaOnly restore: no data phase.
	s3 := RestoreScript(schema, data, "bk", true, false, false)
	if strings.Contains(s3, "restore_remote --data") {
		t.Errorf("schemaOnly restore must not restore data")
	}
}

func TestBuildBackupJob(t *testing.T) {
	chi := testCHI("demo", "default", 2, 2)
	chb := &api.ClickHouseBackup{
		ObjectMeta: meta.ObjectMeta{Name: "b1", Namespace: "ns"},
		Spec:       api.ClickHouseBackupSpec{ClickHouseInstallation: "demo"},
	}
	job := BuildBackupJob(chb, chi)

	if job.Name != "b1-backup" || job.Namespace != "ns" {
		t.Fatalf("unexpected job meta: %s/%s", job.Namespace, job.Name)
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Errorf("backup job BackoffLimit must be 0")
	}
	if job.Spec.Template.Spec.RestartPolicy != core.RestartPolicyNever {
		t.Errorf("backup job RestartPolicy must be Never")
	}
	if got := job.Spec.Template.Spec.Containers[0].Image; got != DefaultClientImage {
		t.Errorf("default image = %q, want %q", got, DefaultClientImage)
	}
	script := strings.Join(job.Spec.Template.Spec.Containers[0].Command, " ")
	if !strings.Contains(script, "chi-demo-default-0-0") || strings.Contains(script, "chi-demo-default-0-1") {
		t.Errorf("FirstPerShard backup should target shard-first hosts only")
	}
}

func TestBuildBackupCronJob(t *testing.T) {
	chi := testCHI("demo", "default", 1, 1)
	suspend := true
	chbs := &api.ClickHouseBackupSchedule{
		ObjectMeta: meta.ObjectMeta{Name: "s1", Namespace: "ns"},
		Spec: api.ClickHouseBackupScheduleSpec{
			ClickHouseInstallation: "demo",
			Schedule:               "0 2 * * *",
			Suspend:                &suspend,
		},
	}
	cj := BuildBackupCronJob(chbs, chi)
	if cj.Spec.Schedule != "0 2 * * *" {
		t.Errorf("schedule = %q", cj.Spec.Schedule)
	}
	if cj.Spec.Suspend == nil || !*cj.Spec.Suspend {
		t.Errorf("suspend must propagate to CronJob")
	}
	if cj.Spec.ConcurrencyPolicy != "Forbid" {
		t.Errorf("default ConcurrencyPolicy must be Forbid, got %q", cj.Spec.ConcurrencyPolicy)
	}
	// Scheduled runs compute a unique timestamped name.
	script := strings.Join(cj.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Command, " ")
	if !strings.Contains(script, "date -u +%Y%m%d-%H%M%S") {
		t.Errorf("scheduled backup must compute a timestamped name")
	}
}

func TestBuildRestoreJob(t *testing.T) {
	chi := testCHI("demo", "default", 2, 2)
	chr := &api.ClickHouseRestore{
		ObjectMeta: meta.ObjectMeta{Name: "r1", Namespace: "ns"},
		Spec:       api.ClickHouseRestoreSpec{ClickHouseInstallation: "demo", BackupName: "bk"},
	}
	job := BuildRestoreJob(chr, chi)
	if job.Name != "r1-restore" {
		t.Fatalf("restore job name = %q", job.Name)
	}
	script := strings.Join(job.Spec.Template.Spec.Containers[0].Command, " ")
	// Schema and data are restored on the first replica of each shard only; the schema
	// CREATE reaches the other replicas ON CLUSTER (via the sidecar's restore_schema_on_cluster).
	for _, h := range []string{"chi-demo-default-0-0", "chi-demo-default-1-0"} {
		if !strings.Contains(script, h) {
			t.Errorf("restore script must target shard-first host %q", h)
		}
	}
	for _, h := range []string{"chi-demo-default-0-1", "chi-demo-default-1-1"} {
		if strings.Contains(script, h) {
			t.Errorf("restore script must NOT target non-first replica %q", h)
		}
	}
}
