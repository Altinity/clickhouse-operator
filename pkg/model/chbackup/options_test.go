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

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

func i32(v int32) *int32 { return &v }

func TestBackupScriptOptions(t *testing.T) {
	s := BackupScript([]string{"svc"}, `"bk"`, BackupOpts{
		Tables:         "db.*",
		Partitions:     []string{"202601", "202602"},
		DiffFromRemote: "base-backup",
		KeepLastRemote: i32(2),
	})
	for _, want := range []string{
		"create_remote --diff-from-remote=base-backup --tables=db.* --partitions=202601,202602 ",
		"system.backup_list",
		"delete remote",
		"OFFSET 2",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("backup script missing %q", want)
		}
	}
}

func TestBackupScriptNoRetentionWhenUnset(t *testing.T) {
	s := BackupScript([]string{"svc"}, `"bk"`, BackupOpts{})
	if strings.Contains(s, "delete remote") {
		t.Errorf("no retention pruning expected when KeepLastRemote is unset")
	}
}

func TestBuildVerifyJob(t *testing.T) {
	chi := testCHI("demo", "default", 1, 1)
	chb := &api.ClickHouseBackup{
		ObjectMeta: meta.ObjectMeta{Name: "b1", Namespace: "ns"},
		Spec:       api.ClickHouseBackupSpec{ClickHouseInstallation: "demo", BackupName: "bk", Verify: true},
	}
	job := BuildVerifyJob(chb, chi)
	if job.Name != "b1-verify" {
		t.Fatalf("verify job name = %q, want b1-verify", job.Name)
	}
	script := strings.Join(job.Spec.Template.Spec.Containers[0].Command, " ")
	for _, want := range []string{"download ${BACKUP_NAME}", "delete local", "system.backup_list"} {
		if !strings.Contains(script, want) {
			t.Errorf("verify script missing %q", want)
		}
	}
}

func TestBuildBackupJobPropagatesOptions(t *testing.T) {
	chi := testCHI("demo", "default", 1, 1)
	chb := &api.ClickHouseBackup{
		ObjectMeta: meta.ObjectMeta{Name: "b1", Namespace: "ns"},
		Spec: api.ClickHouseBackupSpec{
			ClickHouseInstallation: "demo",
			BackupName:             "bk",
			Tables:                 "db.t",
			KeepLastRemote:         i32(3),
		},
	}
	script := strings.Join(BuildBackupJob(chb, chi).Spec.Template.Spec.Containers[0].Command, " ")
	if !strings.Contains(script, "--tables=db.t") {
		t.Errorf("backup job script must carry --tables")
	}
	if !strings.Contains(script, "OFFSET 3") {
		t.Errorf("backup job script must carry retention (OFFSET 3)")
	}
}
