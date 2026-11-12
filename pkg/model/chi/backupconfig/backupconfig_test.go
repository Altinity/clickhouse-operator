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

package backupconfig

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

var (
	testCredentials = Credentials{
		APIUsername:        "clickhouse_backup",
		APIPassword:        `p:a"s's$(x)`,
		ClickHouseUsername: "clickhouse_backup",
		ClickHousePassword: "ch-password",
	}
	testDefaults = Defaults{
		Path:           "backup/{namespace}/{installation}/{cluster}/{shard}",
		ObjectDiskPath: "object_disks/{namespace}/{installation}/{cluster}/{shard}",
	}
)

// parse reads a configuration file back
func parse(t *testing.T, file []byte) map[string]any {
	t.Helper()
	parsed := map[string]any{}
	require.NoError(t, yaml.Unmarshal(file, &parsed))
	return parsed
}

// The configuration file keeps every storage setting as given, unknown ones included, so that a newer
// clickhouse-backup takes settings the operator knows nothing of. It takes the credentials over whatever the storage
// settings hold, and keeps env vars in force, which the operator sets what the backup Jobs rely on with.
func TestBuild(t *testing.T) {
	storageConfig := []byte(`
general:
  remote_storage: s3
  disable_environment_override: true
  some_future_setting: [a, b]
s3:
  bucket: backups
  endpoint: http://minio:9000
  path: own/{cluster}/{shard}
  force_path_style: true
  access_key: key
api:
  username: admin
  password: admin
  allow_parallel: true
clickhouse:
  username: default
  timeout: 5m
`)
	built, err := Build(storageConfig, testCredentials, testDefaults)
	require.NoError(t, err)
	file := parse(t, built.File)

	general := file["general"].(map[string]any)
	require.Equal(t, "s3", general["remote_storage"])
	require.Equal(t, false, general["disable_environment_override"], "the env vars the operator sets must take precedence")
	require.Equal(t, []any{"a", "b"}, general["some_future_setting"])

	s3 := file["s3"].(map[string]any)
	require.Equal(t, "backups", s3["bucket"])
	require.Equal(t, "own/{cluster}/{shard}", s3["path"], "a path set is kept")
	require.Equal(t, testDefaults.ObjectDiskPath, s3["object_disk_path"], "an unset path is defaulted")
	require.Equal(t, true, s3["force_path_style"])
	require.Equal(t, "key", s3["access_key"])

	api := file["api"].(map[string]any)
	require.Equal(t, testCredentials.APIUsername, api["username"])
	require.Equal(t, testCredentials.APIPassword, api["password"])
	require.Equal(t, true, api["allow_parallel"], "the env var the operator sets takes precedence over it")

	clickHouse := file["clickhouse"].(map[string]any)
	require.Equal(t, testCredentials.ClickHouseUsername, clickHouse["username"])
	require.Equal(t, testCredentials.ClickHousePassword, clickHouse["password"])
	require.Equal(t, "5m", clickHouse["timeout"])

	for _, section := range storageSections {
		paths := file[section].(map[string]any)
		require.Contains(t, paths, keyPath, section)
		require.Contains(t, paths, keyObjectDiskPath, section)
	}
	require.Equal(t, testDefaults.Path, file["gcs"].(map[string]any)["path"], "every storage type gets the default paths")

	require.Equal(t, map[string]string{
		"REMOTE_STORAGE":          "s3",
		"S3_BUCKET":               "backups",
		"S3_ENDPOINT":             "http://minio:9000",
		"S3_PATH":                 "own/{cluster}/{shard}",
		"S3_OBJECT_DISK_PATH":     testDefaults.ObjectDiskPath,
		"GCS_PATH":                testDefaults.Path,
		"GCS_OBJECT_DISK_PATH":    testDefaults.ObjectDiskPath,
		"AZBLOB_PATH":             testDefaults.Path,
		"AZBLOB_OBJECT_DISK_PATH": testDefaults.ObjectDiskPath,
		"COS_PATH":                testDefaults.Path,
		"COS_OBJECT_DISK_PATH":    testDefaults.ObjectDiskPath,
		"FTP_PATH":                testDefaults.Path,
		"FTP_OBJECT_DISK_PATH":    testDefaults.ObjectDiskPath,
		"SFTP_PATH":               testDefaults.Path,
		"SFTP_OBJECT_DISK_PATH":   testDefaults.ObjectDiskPath,
	}, built.Settings)
}

// A path set empty is the root of the storage, which is kept; one set null is unset, which is defaulted. Settings
// other than strings reach the path check as clickhouse-backup reads them from an env var.
func TestBuildSettings(t *testing.T) {
	built, err := Build([]byte(`
s3:
  path: ""
  object_disk_path: null
sftp:
  address: sftp.example.com
  port: 2222
`), testCredentials, testDefaults)
	require.NoError(t, err)
	require.Equal(t, "", built.Settings["S3_PATH"])
	require.Equal(t, testDefaults.ObjectDiskPath, built.Settings["S3_OBJECT_DISK_PATH"])
	require.Equal(t, "2222", built.Settings["SFTP_PORT"])
	require.Equal(t, "sftp.example.com", built.Settings["SFTP_ADDRESS"])
	require.NotContains(t, built.Settings, "REMOTE_STORAGE", "a setting the file does not set is not given")
}

// No storage settings, none at all or an empty Secret key, make a file with the credentials and the default paths
func TestBuildWithoutStorageSettings(t *testing.T) {
	for name, storageConfig := range map[string][]byte{"none": nil, "empty": []byte(""), "null": []byte("~\n")} {
		t.Run(name, func(t *testing.T) {
			built, err := Build(storageConfig, testCredentials, testDefaults)
			require.NoError(t, err)
			file := parse(t, built.File)
			require.Equal(t, testCredentials.APIPassword, file["api"].(map[string]any)["password"])
			require.Equal(t, testDefaults.Path, built.Settings["S3_PATH"])
		})
	}
}

// The same settings always make the same file, so that a Secret is not updated when nothing changed
func TestBuildIsStable(t *testing.T) {
	storageConfig := []byte("s3:\n  bucket: b\n  path: p\ngeneral:\n  remote_storage: s3\n")
	first, err := Build(storageConfig, testCredentials, testDefaults)
	require.NoError(t, err)
	for range 10 {
		again, err := Build(storageConfig, testCredentials, testDefaults)
		require.NoError(t, err)
		require.Equal(t, string(first.File), string(again.File))
	}
}

// Storage settings that are no mapping of sections are refused
func TestBuildRefusesInvalidStorageSettings(t *testing.T) {
	for name, storageConfig := range map[string]string{
		"not YAML":                   "s3: [",
		"a list":                     "- s3\n",
		"a scalar":                   "s3",
		"a section that is a list":   "s3:\n  - bucket\n",
		"a section that is a string": "general: s3\n",
		"a setting that is a list":   "s3:\n  bucket: [a, b]\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Build([]byte(storageConfig), testCredentials, testDefaults)
			require.ErrorIs(t, err, ErrInvalidStorageConfig)
		})
	}
}

// The fingerprint of the credentials of the REST API changes with them alone, and gives none of them away
func TestBuildAPIFingerprint(t *testing.T) {
	first, err := Build([]byte("s3:\n  bucket: a\n"), testCredentials, testDefaults)
	require.NoError(t, err)
	require.Len(t, first.APIFingerprint, 64)
	require.NotContains(t, first.APIFingerprint, testCredentials.APIPassword)

	otherStorage, err := Build([]byte("s3:\n  bucket: b\n"), testCredentials, testDefaults)
	require.NoError(t, err)
	require.Equal(t, first.APIFingerprint, otherStorage.APIFingerprint, "storage settings are reloaded without a restart")

	otherClickHouse := testCredentials
	otherClickHouse.ClickHousePassword = "other"
	built, err := Build(nil, otherClickHouse, testDefaults)
	require.NoError(t, err)
	require.Equal(t, first.APIFingerprint, built.APIFingerprint, "the ClickHouse credentials are reloaded without a restart")

	for name, mutate := range map[string]func(*Credentials){
		"username": func(c *Credentials) { c.APIUsername = "other" },
		"password": func(c *Credentials) { c.APIPassword = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			credentials := testCredentials
			mutate(&credentials)
			built, err := Build(nil, credentials, testDefaults)
			require.NoError(t, err)
			require.NotEqual(t, first.APIFingerprint, built.APIFingerprint)
		})
	}
}

// The ClickHouse credentials of a configuration file are read back as Build wrote them
func TestClickHouseCredentials(t *testing.T) {
	built, err := Build([]byte("clickhouse:\n  username: other\n  password: other\n"), testCredentials, testDefaults)
	require.NoError(t, err)
	username, password, ok := ClickHouseCredentials(built.File)
	require.True(t, ok)
	require.Equal(t, testCredentials.ClickHouseUsername, username)
	require.Equal(t, testCredentials.ClickHousePassword, password)

	for name, tc := range map[string]struct {
		file               string
		username, password string
		ok                 bool
	}{
		"no section":  {file: "s3:\n  bucket: a\n", ok: true},
		"no password": {file: "clickhouse:\n  username: a\n", username: "a", ok: true},
		"empty":       {file: "", ok: true},
		"invalid":     {file: "clickhouse: ["},
	} {
		t.Run(name, func(t *testing.T) {
			username, password, ok := ClickHouseCredentials([]byte(tc.file))
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.username, username)
			require.Equal(t, tc.password, password)
		})
	}
}

// ClickHouse credentials the backup Secret does not set leave the file with what the storage settings say, or none
// at all, which clickhouse-backup takes its own defaults for: the operator has none of its own
func TestBuildWithoutClickHouseCredentials(t *testing.T) {
	credentials := testCredentials
	credentials.ClickHouseUsername, credentials.ClickHousePassword = "", ""
	built, err := Build([]byte("s3:\n  bucket: a\n"), credentials, testDefaults)
	require.NoError(t, err)
	file := map[string]any{}
	require.NoError(t, yaml.Unmarshal(built.File, &file))
	require.NotContains(t, file, "clickhouse")

	built, err = Build([]byte("clickhouse:\n  username: own\n  host: 127.0.0.1\n"), credentials, testDefaults)
	require.NoError(t, err)
	username, password, ok := ClickHouseCredentials(built.File)
	require.True(t, ok)
	require.Equal(t, "own", username)
	require.Empty(t, password)

	credentials.ClickHousePassword = "only-password"
	built, err = Build(nil, credentials, testDefaults)
	require.NoError(t, err)
	username, password, _ = ClickHouseCredentials(built.File)
	require.Empty(t, username)
	require.Equal(t, "only-password", password)
}

// UsersFileHasPassword reads the users file as the operator renders it, the backup user named by a dotted username
// included, and compares the SHA-256 of the password of the very user named
func TestUsersFileHasPassword(t *testing.T) {
	sha := func(password string) *api.Setting {
		sum := sha256.Sum256([]byte(password))
		return api.NewSettingScalar(hex.EncodeToString(sum[:]))
	}
	users := api.NewSettings()
	users.Set("backup.operator-1/password_sha256_hex", sha("new"))
	users.Set("backup.operator-1/networks/ip", api.NewSettingVector([]string{"0.0.0.0/0"}))
	users.Set("reader/password_sha256_hex", sha("other"))
	file := []byte(users.ClickHouseConfig("users"))

	require.True(t, UsersFileHasPassword(file, "backup.operator-1", "new"), string(file))
	require.False(t, UsersFileHasPassword(file, "backup.operator-1", "old"))
	require.False(t, UsersFileHasPassword(file, "backup.operator-1", "other"), "another user's password is no match")
	require.False(t, UsersFileHasPassword(file, "clickhouse_backup", "new"), "another username is no match")
	require.False(t, UsersFileHasPassword(nil, "backup.operator-1", "new"))
	require.False(t, UsersFileHasPassword([]byte("<clickhouse><users>"), "backup.operator-1", "new"))
}

// WithoutClickHouseCredentials drops the ClickHouse username and password alone, and the section when nothing else
// is left in it
func TestWithoutClickHouseCredentials(t *testing.T) {
	read := func(file []byte) map[string]any {
		parsed := map[string]any{}
		require.NoError(t, yaml.Unmarshal(file, &parsed))
		return parsed
	}
	stripped := read(WithoutClickHouseCredentials([]byte("s3:\n  bucket: a\nclickhouse:\n  username: u\n  password: p\n")))
	require.NotContains(t, stripped, "clickhouse")
	require.Equal(t, map[string]any{"bucket": "a"}, stripped["s3"])

	stripped = read(WithoutClickHouseCredentials([]byte("clickhouse:\n  username: u\n  timeout: 5m\n")))
	require.Equal(t, map[string]any{"timeout": "5m"}, stripped["clickhouse"])

	require.Equal(t, "s3: [", string(WithoutClickHouseCredentials([]byte("s3: ["))), "a file that cannot be read is left as it is")
}

// The REST API always authenticates with the credentials the operator generates: no config.yml of the user turns
// its authentication off, whether it sets the credentials empty, null, or to its own, or leaves the api section out.
func TestBuildCannotDisableAPIAuth(t *testing.T) {
	for name, storageConfig := range map[string]string{
		"empty credentials":   "api:\n  username: \"\"\n  password: \"\"\n",
		"null credentials":    "api:\n  username: null\n  password: null\n",
		"own credentials":     "api:\n  username: admin\n  password: admin\n",
		"no api section":      "s3:\n  bucket: b\n",
		"empty api section":   "api: {}\n",
		"null api section":    "api: ~\n",
		"env override wanted": "general:\n  disable_environment_override: true\napi:\n  username: \"\"\n",
	} {
		t.Run(name, func(t *testing.T) {
			built, err := Build([]byte(storageConfig), testCredentials, testDefaults)
			require.NoError(t, err)
			file := parse(t, built.File)
			apiSection := file["api"].(map[string]any)
			require.Equal(t, testCredentials.APIUsername, apiSection["username"])
			require.Equal(t, testCredentials.APIPassword, apiSection["password"])
			require.Equal(t, false, file["general"].(map[string]any)["disable_environment_override"])
		})
	}
	t.Run("api section that is no mapping", func(t *testing.T) {
		_, err := Build([]byte("api: [username]\n"), testCredentials, testDefaults)
		require.ErrorIs(t, err, ErrInvalidStorageConfig)
	})
}

// Retention is done by the orchestrator alone, and an incremental base may be deleted: the file always keeps every
// remote backup and rebases on delete, whatever the user's general section says
func TestBuildForcesRetentionSettings(t *testing.T) {
	for name, storageConfig := range map[string]string{
		"unset":     "s3:\n  bucket: b\n",
		"user keep": "general:\n  backups_to_keep_remote: 7\n  rebase_during_delete: false\n  rebase_before_remove_old_remote: false\n",
		"null":      "general:\n  backups_to_keep_remote: null\n  rebase_during_delete: null\n",
		"empty":     "general: {}\n",
	} {
		t.Run(name, func(t *testing.T) {
			built, err := Build([]byte(storageConfig), testCredentials, testDefaults)
			require.NoError(t, err)
			general := parse(t, built.File)["general"].(map[string]any)
			require.Equal(t, 0, general["backups_to_keep_remote"])
			require.Equal(t, true, general["rebase_during_delete"])
			require.Equal(t, true, general["rebase_before_remove_old_remote"])
		})
	}
}
