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
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/kubernetes-sigs/yaml"
	"github.com/stretchr/testify/require"
)

// backupExamplesDir holds the examples of operator-managed backup the docs point users to.
const backupExamplesDir = "../../../../docs/chb-examples"

// commentedField matches an optional field an example leaves commented out on a line of its own, as `# key: value`
// at the indentation the field takes, with an explanation after it.
var commentedField = regexp.MustCompile(`^(\s+)# ([A-Za-z]+:.*)$`)

// The examples of operator-managed backup use the API as it is: every document decodes into its type without a
// field the type lacks, which the API server would prune unseen, and the spec of every backup, schedule and restore
// passes validation. So does every optional field an example leaves commented out on a line of its own, enabled.
func TestBackupExamplesMatchTheAPI(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(backupExamplesDir, "*.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, file := range files {
		raw, err := os.ReadFile(file)
		require.NoError(t, err)
		lines := strings.Split(string(raw), "\n")
		t.Run(filepath.Base(file), func(t *testing.T) {
			requireExampleMatchesTheAPI(t, string(raw))
		})
		for i, line := range lines {
			match := commentedField.FindStringSubmatch(line)
			if match == nil {
				continue
			}
			enabled := slices.Clone(lines)
			enabled[i] = match[1] + match[2]
			field, _, _ := strings.Cut(match[2], ":")
			t.Run(fmt.Sprintf("%s/line %d %s", filepath.Base(file), i+1, field), func(t *testing.T) {
				requireExampleMatchesTheAPI(t, strings.Join(enabled, "\n"))
			})
		}
	}
}

// requireExampleMatchesTheAPI decodes every document of an example into its type, refusing a field the type lacks,
// and validates the spec of the backup kinds.
func requireExampleMatchesTheAPI(t *testing.T, example string) {
	t.Helper()
	for _, document := range strings.Split(example, "\n---") {
		kind := struct {
			Kind string `json:"kind"`
		}{}
		require.NoError(t, yaml.Unmarshal([]byte(document), &kind))
		switch kind.Kind {
		case ClickHouseBackupCRDResourceKind:
			chb := &ClickHouseBackup{}
			require.NoError(t, yaml.UnmarshalStrict([]byte(document), chb))
			require.NoError(t, chb.Spec.Validate())
		case ClickHouseBackupScheduleCRDResourceKind:
			chbs := &ClickHouseBackupSchedule{}
			require.NoError(t, yaml.UnmarshalStrict([]byte(document), chbs))
			require.NoError(t, chbs.Spec.Validate())
		case ClickHouseRestoreCRDResourceKind:
			chr := &ClickHouseRestore{}
			require.NoError(t, yaml.UnmarshalStrict([]byte(document), chr))
			require.NoError(t, chr.Spec.Validate())
		case ClickHouseInstallationCRDResourceKind:
			require.NoError(t, yaml.UnmarshalStrict([]byte(document), &ClickHouseInstallation{}))
		case "Secret", "":
		default:
			require.Failf(t, "unexpected kind", "an example of kind %s", kind.Kind)
		}
	}
}
