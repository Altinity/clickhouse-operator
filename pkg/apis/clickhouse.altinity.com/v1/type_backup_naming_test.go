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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// A naming template names the backup of every shard by its number and the full time, which tell the backups of the
// shards apart at one storage path and order them; {shard} is kept apart by a literal that is no digit.
func TestValidateBackupNaming(t *testing.T) {
	for template, valid := range map[string]bool{
		"":                                       true,
		BackupNamingACM:                          true,
		"{prefix}-shard{shard}-{YYYYMMDDHHIISS}": true,
		"{cluster}.{chi}.{namespace}-s{shard}-{YYYY}-{MM}-{DD}_{HH}{II}{SS}": true,
		"{YYYYMMDD}{HHIISS}_{shard}":                                         true,
		"shard{shard}":                                                       false,
		"{YYYYMMDDHHIISS}":                                                   false,
		"shard{shard}-{YYYYMMDDHHII}":                                        false,
		"shard{shard}-{YYYYMMDDHHIISSSS}":                                    false,
		"shard{shard}-{YYYYMMDDHHIISS}{YYYY}":                                false,
		"shard{shard}{shard}-{YYYYMMDDHHIISS}":                               false,
		"{shard}{YYYYMMDDHHIISS}":                                            false,
		"shard{shard}1-{YYYYMMDDHHIISS}":                                     false,
		"1{shard}-{YYYYMMDDHHIISS}":                                          false,
		"{prefix}{shard}-{YYYYMMDDHHIISS}":                                   false,
		"shard{replica}-{YYYYMMDDHHIISS}":                                    false,
		"shard {shard}-{YYYYMMDDHHIISS}":                                     false,
		"shard/{shard}-{YYYYMMDDHHIISS}":                                     false,
		"shard{shard}-{yyyymmddhhiiss}":                                      false,
		"shard{{shard}}-{YYYYMMDDHHIISS}":                                    false,
	} {
		err := ValidateBackupNaming(template)
		require.Equal(t, valid, err == nil, "%q: %v", template, err)
		require.Equal(t, valid, (&ClusterBackup{Naming: types.NewString(template)}).Validate() == nil, template)
	}
}

// A template names the backup of a shard with the values of the cluster, and the time, with a T before the hour for
// a backup made at once.
func TestRenderBackupName(t *testing.T) {
	values := BackupNameValues{Shard: 2, Cluster: "main", Chi: "prod", Namespace: "db", Prefix: "nightly", Tag: "20261113020000"}
	for template, want := range map[string]string{
		BackupNamingACM: "shard2-20261113020000",
		"{prefix}-{cluster}-{chi}-{namespace}-s{shard}-{YYYY}.{MM}.{DD}_{HH}{II}{SS}": "nightly-main-prod-db-s2-2026.11.13_020000",
		"{DDMMYYYY}-{IIHHSS}-{shard}": "13112026-000200-2",
	} {
		name, err := RenderBackupName(template, values)
		require.NoError(t, err, template)
		require.Equal(t, want, name, template)
	}
	values.Tag = BackupTag(time.Date(2026, time.November, 13, 14, 5, 6, 0, time.UTC), true)
	require.Equal(t, "20261113T140506", values.Tag)
	name, err := RenderBackupName(BackupNamingACM, values)
	require.NoError(t, err)
	require.Equal(t, "shard2-20261113T140506", name)

	_, err = RenderBackupName(BackupNamingACM, BackupNameValues{Shard: 1, Tag: "2026111302"})
	require.Error(t, err, "no tag")
	_, err = RenderBackupName("{prefix}-shard{shard}-{YYYYMMDDHHIISS}", BackupNameValues{Shard: 1, Tag: "20261113020000",
		Prefix: string(make([]byte, 120))})
	require.Error(t, err, "no name")
	_, err = RenderBackupName("shard{shard}", values)
	require.Error(t, err, "no template")
}

// A cluster inherits the naming of the installation, as it does the mode.
func TestClusterBackupInheritsTheNaming(t *testing.T) {
	installation := &ClusterBackup{Naming: types.NewString(BackupNamingACM)}
	require.Equal(t, BackupNamingACM, (&ClusterBackup{}).MergeFrom(installation, MergeTypeFillEmptyValues).GetNaming())
	own := &ClusterBackup{Naming: types.NewString("s{shard}-{YYYYMMDDHHIISS}")}
	require.Equal(t, "s{shard}-{YYYYMMDDHHIISS}", own.MergeFrom(installation, MergeTypeFillEmptyValues).GetNaming())
	require.Empty(t, (*ClusterBackup)(nil).GetNaming())
}
