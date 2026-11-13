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
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// The entries of a schedule carry their config overrides to the script, which takes those it may and refuses any
// other, as the operator does.
func TestJobScriptParsesConfigOverrides(t *testing.T) {
	entries := []api.BackupScheduleEntry{
		{Period: api.BackupPeriodDaily, Time: "03:00", Keep: 7},
		{Period: api.BackupPeriodMonthly, Time: "03:00", Day: int32Ref(1), Keep: 12,
			ConfigOverrides: []string{"S3_STORAGE_CLASS=GLACIER_IR", "--env=S3_ACL=private"}},
	}
	env := EntriesEnv(entries)
	require.Equal(t, "period=daily time=03:00 keep=7;period=monthly time=03:00 day=1 keep=12 env=S3_STORAGE_CLASS=GLACIER_IR env=S3_ACL=private", env)
	out := snippetOutput(t, map[string]string{envSchedules: env}, `parse_schedules; printf '[%s]\n' "${SCHED_ENV[@]}"`,
		"for entry in parse_schedules(env('SCHEDULES')): print('[%s]' % entry['env'])")
	require.Equal(t, "[]\n[ --env=S3_STORAGE_CLASS=GLACIER_IR --env=S3_ACL=private]\n", out)
	for _, invalid := range []string{"S3_PATH=elsewhere", "API_LISTEN=0.0.0.0:1", "S3_SECRET_KEY=x", "API_FOO=1", "s3_class=x",
		"S3_STORAGE_CLASS=a$b", "REMOTE_STORAGE=gcs", "CLICKHOUSE_HOST=x", "S3_OBJECT_DISK_PATH=x"} {
		code, out := snippet(t, map[string]string{envSchedules: "period=daily time=03:00 keep=1 env=" + invalid}, "parse_schedules",
			"parse_schedules(env('SCHEDULES'))")
		require.Equal(t, 1, code, "%q: %s", invalid, out)
		require.Contains(t, out, "ERROR: SCHEDULES entry 0: the config override", invalid)
		require.Error(t, api.ValidateConfigOverrides("configOverrides", []string{invalid}), invalid)
	}
}

// The run of a slot of an entry with config overrides makes its backup on every host at once through the actions
// endpoint, as only its create_remote command takes them, with the flags of the options and the base of an
// incremental backup; a slot of an entry without overrides makes it through the typed request, as before.
func TestJobScriptBacksUpWithConfigOverrides(t *testing.T) {
	const schedules = "period=daily time=03:00 keep=7 incremental=true;period=monthly time=03:00 day=1 keep=12 env=S3_STORAGE_CLASS=GLACIER_IR"
	hosts := map[string]*stubHost{"h0": {remote: []stubBackup{remote("daily-20261130030000", "")}}, "h1": {remote: []stubBackup{remote("daily-20261130030000", "")}}}
	s := newSidecarStub(t, hosts)
	env := scheduledEnv("h0 h1", schedules, "2026-12-01T03:00:00Z")
	env[envTables] = "db.*"
	code, out := s.run(env)
	require.Zero(t, code, out)
	require.Empty(t, s.matching(http.MethodPost, "/backup/create_remote"))
	actions := s.matching(http.MethodPost, "/backup/actions")
	require.Len(t, actions, 2)
	for _, action := range actions {
		require.Equal(t, "create_remote --tables=db.* --env=S3_STORAGE_CLASS=GLACIER_IR daily-20261201030000", action.command, action.host)
	}
	require.Contains(t, out, "made with the config overrides --env=S3_STORAGE_CLASS=GLACIER_IR")
	for _, host := range []string{"h0", "h1"} {
		require.Contains(t, s.remoteNames(host), "daily-20261201030000")
	}

	// The next daily slot has none: the typed request, incremental from the monthly backup
	code, out = s.run(scheduledEnv("h0 h1", schedules, "2026-12-02T03:00:00Z"))
	require.Zero(t, code, out)
	creates := s.matching(http.MethodPost, "/backup/create_remote")
	require.Len(t, creates, 2)
	require.Equal(t, "daily-20261201030000", creates[0].query.Get("diff-from-remote"))
	require.Len(t, s.matching(http.MethodPost, "/backup/actions"), 2)

	// A daily slot of an entry with overrides, incremental: the base is a flag of the command
	s = newSidecarStub(t, map[string]*stubHost{"h0": {remote: []stubBackup{remote("daily-20261130030000", "")}}})
	code, out = s.run(scheduledEnv("h0", "period=daily time=03:00 keep=7 incremental=true env=S3_STORAGE_CLASS=STANDARD_IA", "2026-12-01T03:00:00Z"))
	require.Zero(t, code, out)
	actions = s.matching(http.MethodPost, "/backup/actions")
	require.Len(t, actions, 1)
	require.Equal(t, "create_remote --diff-from-remote=daily-20261130030000 --env=S3_STORAGE_CLASS=STANDARD_IA daily-20261201030000", actions[0].command)
}

// A backup made once takes the config overrides of its spec, and a host that fails leaves the others to finish.
func TestJobScriptBacksUpOnceWithConfigOverrides(t *testing.T) {
	hosts := map[string]*stubHost{"h0": {}, "h1": {failOp: map[string]string{"create_remote": "access denied"}}, "h2": {}}
	s := newSidecarStub(t, hosts)
	code, out := s.run(map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0 h1 h2",
		envConfigOverrides: "S3_STORAGE_CLASS=STANDARD_IA S3_ACL=private", envSchemaOnly: "true"})
	require.Equal(t, 1, code, out)
	require.Contains(t, out, "h1: create_remote bk-1 ended with status 'error': access denied")
	var commands []string
	for _, action := range s.matching(http.MethodPost, "/backup/actions") {
		commands = append(commands, action.host+" "+action.command)
	}
	slices.Sort(commands)
	require.Equal(t, []string{
		"h0 create_remote --schema --env=S3_STORAGE_CLASS=STANDARD_IA --env=S3_ACL=private bk-1",
		"h1 create_remote --schema --env=S3_STORAGE_CLASS=STANDARD_IA --env=S3_ACL=private bk-1",
		"h2 create_remote --schema --env=S3_STORAGE_CLASS=STANDARD_IA --env=S3_ACL=private bk-1",
	}, commands, "every host was asked before the failure was known")

	// A forbidden override is refused before anything changes
	s = newSidecarStub(t, map[string]*stubHost{"h0": {}})
	code, out = s.run(map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0",
		envConfigOverrides: "S3_BUCKET=other", envUnchangedExitCode: "3"})
	require.Equal(t, 3, code, out)
	require.Contains(t, out, "ERROR: CONFIG_OVERRIDES: the config override of S3_BUCKET is refused")
	require.Zero(t, s.countCalls())
}

// A sidecar that restarts during the upload of a backup with config overrides resumes the upload by itself, without
// them: the Job follows the resumed upload, as for a typed request, and warns that the overrides were lost.
func TestJobScriptFollowsAnUploadResumedWithoutTheConfigOverrides(t *testing.T) {
	hosts := map[string]*stubHost{"h0": {loseActions: true, resume: "success"}}
	s := newSidecarStub(t, hosts)
	code, out := s.run(map[string]string{envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "h0",
		envConfigOverrides: "S3_STORAGE_CLASS=STANDARD_IA"})
	require.Zero(t, code, out)
	require.Contains(t, out, "h0: the resumed upload of bk-1 succeeded")
	require.Contains(t, out, "which it resumed without the config overrides --env=S3_STORAGE_CLASS=STANDARD_IA")
	require.True(t, strings.Contains(out, "backup bk-1 completed on 1 host(s)"), out)
}

// A config override never sets where the backups are, which every setting of the storage path check tells.
func TestConfigOverridesNeverSetThePaths(t *testing.T) {
	for _, key := range PathCheckKeys {
		require.Contains(t, api.BackupConfigOverrideForbidden, key)
	}
}
