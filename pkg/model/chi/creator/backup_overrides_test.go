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

package creator

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// A config override of a backup never sets what the operator pins in the sidecar, nor the credentials of its
// configuration file, nor anything else it sets there, of a host secure or not.
func TestConfigOverridesNeverSetWhatTheOperatorSets(t *testing.T) {
	names := append(slices.Clone(backupPinnedEnvVarNames), backupConfigFileEnvVarNames...)
	for _, host := range []*chi.Host{
		{HostPorts: chi.HostPorts{TCPPort: types.NewInt32(9000)}},
		{HostPorts: chi.HostPorts{TCPPort: types.NewInt32(types.PortUnassigned()), TLSPort: types.NewInt32(9440)}},
	} {
		for _, env := range newBackupEnvVars(host) {
			names = append(names, env.Name)
		}
	}
	for _, name := range names {
		require.Contains(t, chi.BackupConfigOverrideForbidden, name)
	}
}
