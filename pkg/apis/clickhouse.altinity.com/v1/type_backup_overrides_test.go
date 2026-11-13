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

package v1

import (
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A config override is KEY=VALUE of a clickhouse-backup setting, the --env= flag it may be written with stripped,
// a single token of a command, of a key set once, which is neither one the operator sets nor one of where the backups
// are nor one of a secret; at most 16, and not on a backup that runs a schedule.
func TestValidateConfigOverrides(t *testing.T) {
	sixteen := make([]string, BackupConfigOverridesMaxItems+1)
	for i := range sixteen {
		sixteen[i] = fmt.Sprintf("S3_X%d=1", i)
	}
	for name, tc := range map[string]struct {
		items []string
		valid bool
	}{
		"a storage class":         {[]string{"S3_STORAGE_CLASS=STANDARD_IA"}, true},
		"with the flag":           {[]string{"--env=S3_STORAGE_CLASS=STANDARD_IA", "GCS_STORAGE_CLASS=NEARLINE"}, true},
		"a value with a comma":    {[]string{"S3_OBJECT_LABELS=a:1,b:2"}, true},
		"the flag and a blank":    {[]string{"--env S3_STORAGE_CLASS=STANDARD_IA"}, false},
		"a lowercase key":         {[]string{"s3_storage_class=x"}, false},
		"a blank":                 {[]string{"S3_STORAGE_CLASS=a b"}, false},
		"a dollar":                {[]string{"S3_STORAGE_CLASS=$(id)"}, false},
		"a quote":                 {[]string{`S3_STORAGE_CLASS="x"`}, false},
		"no value":                {[]string{"S3_STORAGE_CLASS="}, false},
		"a key twice":             {[]string{"S3_STORAGE_CLASS=a", "--env=S3_STORAGE_CLASS=b"}, false},
		"too many":                {sixteen, false},
		"a pinned setting":        {[]string{"BACKUPS_TO_KEEP_REMOTE=0"}, false},
		"a credential":            {[]string{"CLICKHOUSE_PASSWORD=x"}, false},
		"a password":              {[]string{"FTP_PASSWORD=x"}, false},
		"a secret key":            {[]string{"S3_SECRET_KEY=x"}, false},
		"the path":                {[]string{"S3_PATH=elsewhere"}, false},
		"the bucket":              {[]string{"S3_BUCKET=other"}, false},
		"the object disk path":    {[]string{"GCS_OBJECT_DISK_PATH=x"}, false},
		"a setting of the API":    {[]string{"API_FOO=1"}, false},
		"the storage":             {[]string{"REMOTE_STORAGE=gcs"}, false},
		"the host of the sidecar": {[]string{"CLICKHOUSE_HOST=x"}, false},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.valid, ValidateConfigOverrides("configOverrides", tc.items) == nil, "%v", tc.items)
			entry := BackupScheduleEntry{Period: BackupPeriodDaily, Time: "03:00", Keep: 1, ConfigOverrides: tc.items}
			require.Equal(t, tc.valid, entry.Validate() == nil)
			backup := ClickHouseBackupSpec{ClickHouseInstallation: "chi", ConfigOverrides: tc.items}
			require.Equal(t, tc.valid, backup.Validate() == nil)
		})
	}
	require.Equal(t, []string{"S3_STORAGE_CLASS=X", "S3_ACL=private"}, ConfigOverrides([]string{"--env=S3_STORAGE_CLASS=X", "S3_ACL=private"}))
	run := ClickHouseBackupSpec{ScheduleRef: &BackupScheduleRef{Name: "s1"}, ConfigOverrides: []string{"S3_STORAGE_CLASS=X"}}
	require.ErrorContains(t, run.Validate(), "takes no configOverrides")
}

// The Job script refuses the config overrides the spec validation refuses: the same settings, those of where the
// backups are, of the API and of secrets.
func TestJobScriptRefusesTheConfigOverridesOfTheTypes(t *testing.T) {
	raw, err := os.ReadFile(jobScriptPath)
	require.NoError(t, err)
	script := string(raw)
	forbidden := regexp.MustCompile(`(?m)^CONFIG_OVERRIDE_FORBIDDEN='([^']*)'$`).FindStringSubmatch(script)
	require.NotNil(t, forbidden, "the script no longer defines CONFIG_OVERRIDE_FORBIDDEN")
	keys := regexp.MustCompile(`(?s)PATH_CHECK_KEYS=\(\n(.*?)\n\)`).FindStringSubmatch(script)
	require.NotNil(t, keys)
	listed := append(strings.Fields(forbidden[1]), strings.Fields(keys[1])...)
	slices.Sort(listed)
	want := slices.Clone(BackupConfigOverrideForbidden)
	slices.Sort(want)
	require.Equal(t, want, listed)
	sensitive := regexp.MustCompile(`(?m)^CONFIG_OVERRIDE_SENSITIVE=\(([^)]*)\)$`).FindStringSubmatch(script)
	require.NotNil(t, sensitive)
	require.Equal(t, BackupConfigOverrideSensitiveTokens, strings.Fields(sensitive[1]))
	require.Contains(t, script, "\nCONFIG_OVERRIDES_MAX_ITEMS="+fmt.Sprint(BackupConfigOverridesMaxItems)+"\n")
}
