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
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// testShardNumbers numbers the shards of the hosts of the tests of a naming template: a0 and a1 are the replicas of
// shard 1, b0 and b1 those of shard 2
const testShardNumbers = "a0=1 a1=1 b0=2 b1=2"

// scriptNamingEnv returns the environment of a Job of a cluster with the naming template, on top of env
func scriptNamingEnv(template string, env map[string]string) map[string]string {
	return withEnv(env, map[string]string{
		envBackupNaming: template, envShardNumbers: testShardNumbers, envCluster: "main",
		envNamingInstallation: "prod", envNamingNamespace: "db",
	})
}

// The script reads names of a naming template as the operator writes them: the shard, the prefix and the time, with a
// T before the hour for a backup made at once, of the values of the cluster.
func TestJobScriptParsesTheNamingTemplate(t *testing.T) {
	for _, tc := range []struct {
		template, name, prefix string
		// want is "<shard> <prefix> <key>", empty for a name the template does not make
		want string
	}{
		{template: api.BackupNamingACM, name: "shard1-20261113020537", want: "1  20261113020537"},
		{template: api.BackupNamingACM, name: "shard12-20261113T020537", want: "12  20261113T020537"},
		{template: api.BackupNamingACM, name: "shard0-20261113020537"},
		{template: api.BackupNamingACM, name: "shard1-20261313020537"},
		{template: api.BackupNamingACM, name: "shard1-2026111302053"},
		{template: api.BackupNamingACM, name: "xshard1-20261113020537"},
		{template: "{prefix}-s{shard}-{YYYYMMDDHHIISS}", name: "nightly.x-s2-20261113020000", want: "2 nightly.x 20261113020000"},
		{template: "{prefix}-s{shard}-{YYYYMMDDHHIISS}", name: "nightly.x-s2-20261113020000", prefix: "nightly.x", want: "2 nightly.x 20261113020000"},
		{template: "{prefix}-s{shard}-{YYYYMMDDHHIISS}", name: "nightlyax-s2-20261113020000", prefix: "nightly.x"},
		{template: "{cluster}-{chi}-{namespace}-{shard}-{DD}.{MM}.{YYYY}-{HH}{II}{SS}", name: "main-prod-db-3-13.11.2026-020000", want: "3  20261113020000"},
		{template: "{cluster}-{chi}-{namespace}-{shard}-{DD}.{MM}.{YYYY}-{HH}{II}{SS}", name: "main-prod-db-3-13.11.2026-T020000", want: "3  20261113T020000"},
		{template: "{cluster}-{chi}-{namespace}-{shard}-{DD}.{MM}.{YYYY}-{HH}{II}{SS}", name: "other-prod-db-3-13.11.2026-020000"},
	} {
		out := sourcedOutput(t, scriptNamingEnv(tc.template, map[string]string{"NAME": tc.name, "PREFIX": tc.prefix}), `
			naming_init
			if naming_parse "${NAME}" "${PREFIX}"; then echo "${N_SHARD} ${N_PREFIX} ${N_KEY}"; else echo; fi`)
		require.Equal(t, tc.want+"\n", out, "%+v", tc)
		if tc.want == "" {
			continue
		}
		// The template makes the name back, as the operator does
		fields := strings.SplitN(tc.want, " ", 3)
		out = sourcedOutput(t, scriptNamingEnv(tc.template, map[string]string{"SHARD": fields[0], "PREFIX": fields[1], "KEY": fields[2]}), `
			naming_init
			naming_render "${SHARD}" "${KEY}" "${PREFIX}"; echo "${REPLY}"`)
		require.Equal(t, tc.name+"\n", out, "%+v", tc)
		name, err := api.RenderBackupName(tc.template, api.BackupNameValues{
			Shard: atoi(t, fields[0]), Cluster: "main", Chi: "prod", Namespace: "db", Prefix: fields[1], Tag: fields[2],
		})
		require.NoError(t, err)
		require.Equal(t, tc.name, name, "the operator names the backup as the script does")
	}
	for _, invalid := range []string{"shard{shard}", "{YYYYMMDDHHIISS}", "shard{shard}-{YYYYMMDDHHII}", "s{shard}-{YYYYMMDDHHIISS}{SS}",
		"s{replica}-{shard}-{YYYYMMDDHHIISS}", "s {shard}-{YYYYMMDDHHIISS}"} {
		code, out := sourced(t, scriptNamingEnv(invalid, nil), "naming_init")
		require.Equal(t, 1, code, "%q: %s", invalid, out)
		require.Contains(t, out, "ERROR: BACKUP_NAMING", invalid)
	}
}

// atoi parses a number of a test
func atoi(t *testing.T, value string) int {
	t.Helper()
	var n int
	for _, c := range value {
		require.True(t, (c >= '0') && (c <= '9'), value)
		n = n*10 + int(c-'0')
	}
	return n
}

// With a naming template a backup belongs to the slot of a schedule entry up to 30 minutes after the slot, as the
// Altinity Cloud Manager names a scheduled backup after the time its run started, its seconds included.
func TestJobScriptSlotTolerance(t *testing.T) {
	for _, tc := range []struct {
		entry string
		tag   string
		// match tells whether the tag is of a slot with the tolerance, exact whether it is without, to the minute
		match, exact bool
	}{
		{"period=daily time=02:00 keep=1", "20261113020000", true, true},
		{"period=daily time=02:00 keep=1", "20261113020037", true, true},
		{"period=daily time=02:00 keep=1", "20261113022959", true, false},
		{"period=daily time=02:00 keep=1", "20261113023000", false, false},
		{"period=daily time=02:00 keep=1", "20261113015959", false, false},
		{"period=daily time=23:50 keep=1", "20261114001000", true, false},
		{"period=daily time=23:50 keep=1", "20261114002000", false, false},
		{"period=hour time=00:45 keep=1", "20261113031000", true, false},
		{"period=hour time=00:45 keep=1", "20261113032000", false, false},
		// 2026-11-15 is a Sunday: its slot at 23:45 takes a backup made on Monday at 00:05
		{"period=weekly day=0 time=23:45 keep=1", "20261116000500", true, false},
		{"period=weekly day=1 time=23:45 keep=1", "20261116000500", false, false},
		{"period=monthly day=31 time=23:45 keep=1", "20261201000500", true, false},
		{"period=monthly day=1 time=23:45 keep=1", "20261201000500", false, false},
	} {
		out := sourcedOutput(t, scriptNamingEnv(api.BackupNamingACM, map[string]string{envSchedules: tc.entry, "TAG": tc.tag}),
			`naming_init; parse_schedules; if slot_matches 0 "${TAG}"; then echo yes; else echo no; fi`)
		require.Equal(t, map[bool]string{true: "yes\n", false: "no\n"}[tc.match], out, "%s at %s", tc.entry, tc.tag)
		// Without a naming template a slot is that minute
		out = sourcedOutput(t, map[string]string{envSchedules: tc.entry, "TAG": tc.tag},
			`naming_init; parse_schedules; if slot_matches 0 "${TAG}"; then echo yes; else echo no; fi`)
		require.Equal(t, map[bool]string{true: "yes\n", false: "no\n"}[tc.exact], out, "%s at %s, without a naming template", tc.entry, tc.tag)
	}
}

// acmBackups returns the backups the Altinity Cloud Manager made of every shard of a cluster at one storage path, one
// tag for all shards, named after the time the run started
func acmBackups(tags ...string) []stubBackup {
	var backups []stubBackup
	for _, tag := range tags {
		for _, shard := range []string{"1", "2"} {
			backups = append(backups, remote("shard"+shard+"-"+tag, ""))
		}
	}
	return backups
}

// A scheduled backup of a cluster with the naming template of the Altinity Cloud Manager makes the backup of every shard
// under its own name, at the storage path the shards share, incremental from the newest backup every shard holds, made by
// the Altinity Cloud Manager too; retention then counts the backups of the slots by their time, with its tolerance, and
// deletes on every host the name of its own shard.
func TestJobScriptBacksUpByTheNamingTemplate(t *testing.T) {
	shared := &stubHost{remote: append(acmBackups("20261110020537", "20261111020812", "20261112021503"),
		// A backup the Altinity Cloud Manager left of the first shard alone
		remote("shard1-20261109020004", ""),
		// A backup of another cluster with another naming, at the same path
		remote("other-20261101000000", ""))}
	hosts := map[string]*stubHost{"a0": shared, "b0": shared}
	s := newSidecarStub(t, hosts)
	env := scriptNamingEnv(api.BackupNamingACM, scheduledEnv("a0 b0", "period=daily time=02:00 keep=2 incremental=true", "2026-11-13T02:00:00Z"))
	code, out := s.run(env)
	require.Zero(t, code, out)
	creates := s.matching(http.MethodPost, "/backup/create_remote")
	require.Len(t, creates, 2)
	got := map[string]url.Values{}
	for _, create := range creates {
		got[create.host] = create.query
	}
	require.Equal(t, "shard1-20261113020000", got["a0"].Get("name"))
	require.Equal(t, "shard1-20261112021503", got["a0"].Get("diff-from-remote"))
	require.Equal(t, "shard2-20261113020000", got["b0"].Get("name"))
	require.Equal(t, "shard2-20261112021503", got["b0"].Get("diff-from-remote"))
	names := s.remoteNames("a0")
	slices.Sort(names)
	require.Equal(t, []string{"other-20261101000000", "shard1-20261112021503", "shard1-20261113020000",
		"shard2-20261112021503", "shard2-20261113020000"}, names,
		"retention keeps 2 daily backups, deletes the older ones of every shard and the incomplete one, and leaves other names alone:\n%s", out)
	for _, deletion := range s.matching(http.MethodPost, "/backup/delete/remote/") {
		shard := map[string]string{"a0": "shard1-", "b0": "shard2-"}[deletion.host]
		require.True(t, strings.HasPrefix(strings.TrimPrefix(deletion.path, "/backup/delete/remote/"), shard),
			"%s deletes the backups of its own shard alone: %s", deletion.host, deletion.path)
	}

	// A run of the same slot again finds the backup there, made by either shard
	code, out = s.run(withEnv(env, map[string]string{envUnchangedExitCode: "3"}))
	require.Equal(t, 3, code, out)
	require.Contains(t, out, "ERROR: backup shard1-20261113020000 already exists in the storage of a0")
}

// A backup run now, or once, of a cluster with a naming template is named by it with a T before the hour, which the
// operator gives it as the name of the backup of the first shard; the Job names the backup of every other shard alike.
func TestJobScriptBacksUpOnceByTheNamingTemplate(t *testing.T) {
	s := newSidecarStub(t, map[string]*stubHost{"a0": {}, "b0": {}})
	code, out := s.run(scriptNamingEnv("{prefix}-shard{shard}-{YYYYMMDDHHIISS}", map[string]string{
		envOperation: operationBackup, envBackupName: "once-shard1-20261113T101500", envBackupNamePrefix: "once", envBackupHosts: "a0 b0",
	}))
	require.Zero(t, code, out)
	require.Equal(t, []string{"once-shard1-20261113T101500"}, s.remoteNames("a0"))
	require.Equal(t, []string{"once-shard2-20261113T101500"}, s.remoteNames("b0"))

	// A name the template does not make is refused before anything changes
	s = newSidecarStub(t, map[string]*stubHost{"a0": {}, "b0": {}})
	code, out = s.run(scriptNamingEnv(api.BackupNamingACM, map[string]string{
		envOperation: operationBackup, envBackupName: "bk-1", envBackupHosts: "a0 b0", envUnchangedExitCode: "3",
	}))
	require.Equal(t, 3, code, out)
	require.Contains(t, out, "ERROR: BACKUP_NAME bk-1 is no name the naming template 'shard{shard}-{YYYYMMDDHHIISS}' makes on cluster main")
}

// A restore of a cluster with a naming template restores on every host the backup of its shard: named by its time
// alone, as the Altinity Cloud Manager tells a backup, or by the name of the backup of any shard.
func TestJobScriptRestoresByTheNamingTemplate(t *testing.T) {
	for _, name := range []string{"20261112021503", "shard2-20261112021503"} {
		hosts := replicaRestoreHosts()
		for _, host := range []string{"a0", "a1"} {
			hosts[host].remote = acmBackups("20261112021503")[:1]
		}
		for _, host := range []string{"b0", "b1"} {
			hosts[host].remote = acmBackups("20261112021503")[1:]
		}
		s := newSidecarStub(t, hosts)
		code, out := s.run(scriptNamingEnv(api.BackupNamingACM, replicaRestoreEnv(map[string]string{envBackupName: name})))
		require.Zero(t, code, out)
		require.Equal(t, []string{
			"a0 download hardlink_exists_files=", "a1 download schema=", "b0 download hardlink_exists_files=", "b1 download schema=",
			"a0 rm= schema=", "a1 rm= schema=", "b0 rm= schema=", "b1 rm= schema=", "a0 data=", "b0 data=",
		}, s.restoreSteps())
		for _, c := range s.matching(http.MethodPost, "/backup/") {
			shard := map[string]string{"a0": "shard1-", "a1": "shard1-", "b0": "shard2-", "b1": "shard2-"}[c.host]
			require.Contains(t, c.path, shard+"20261112021503", "%s %s", c.host, c.path)
		}
	}

	// A time alone does not tell the prefix of a template that holds one
	s := newSidecarStub(t, replicaRestoreHosts())
	code, out := s.run(scriptNamingEnv("{prefix}-shard{shard}-{YYYYMMDDHHIISS}", replicaRestoreEnv(map[string]string{
		envBackupName: "20261112021503", envUnchangedExitCode: "3"})))
	require.Equal(t, 3, code, out)
	require.Contains(t, out, "holds {prefix}: name the backup in full")
}

// With a naming template the shards of the target cluster may share a storage path, where the template names the
// backup of each apart, as the Altinity Cloud Manager keeps them; another cluster still may not share it.
func TestJobScriptLetsTheShardsOfANamingTemplateShareAPath(t *testing.T) {
	s3 := storageSettings("s3", "S3_BUCKET", "backups", "S3_PATH", "acm/{cluster}")
	env := withEnv(scriptNamingEnv(api.BackupNamingACM, map[string]string{
		envOperation: operationBackup, envBackupName: "20261113T101500", envBackupHosts: "a0 b0", envUnchangedExitCode: "3",
	}), pathCheckTestEnv(pathCluster{name: "main", shards: "0 1", settings: s3}))
	s := newSidecarStub(t, map[string]*stubHost{"a0": {}, "b0": {}})
	code, out := s.run(env)
	require.Zero(t, code, out)
	require.Contains(t, out, "storage path check: the shards of cluster main share a path")

	// Without the template the shards would find each other's backups
	code, out = newSidecarStub(t, map[string]*stubHost{"a0": {}, "b0": {}}).run(withEnv(env, map[string]string{
		envBackupNaming: "", envBackupName: "bk-1"}))
	require.Equal(t, 3, code, out)
	require.Contains(t, out, "ERROR: the storage path 'acm/main' of shard 1 of cluster main is that of shard 0 of cluster main too")

	// Nor may another cluster share it
	code, out = newSidecarStub(t, map[string]*stubHost{"a0": {}, "b0": {}}).run(withEnv(env, pathCheckTestEnv(
		pathCluster{name: "main", shards: "0 1", settings: s3},
		pathCluster{name: "other", shards: "0", settings: storageSettings("s3", "S3_BUCKET", "backups", "S3_PATH", "acm/main")})))
	require.Equal(t, 3, code, out)
	require.Contains(t, out, "ERROR: the storage path 'acm/main' of shard 0 of cluster other is that of shard 0 of cluster main too")
}
