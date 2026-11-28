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

// A naming template names the backup of every shard by its name, {shard}, or its number, {shardNum}, and the full
// time, which tell the backups of the shards apart at one storage path and order them; either is kept apart by a
// literal that is no digit.
func TestValidateBackupNaming(t *testing.T) {
	for template, valid := range map[string]bool{
		"":                                       true,
		BackupNamingShardTime:                    true,
		"{prefix}-shard{shard}-{YYYYMMDDHHIISS}": true,
		"{cluster}.{chi}.{namespace}-s{shard}-{YYYY}-{MM}-{DD}_{HH}{II}{SS}": true,
		"{YYYYMMDD}{HHIISS}_{shard}":                                         true,
		"shard{shard}-{YYYYMMDDHHIISS}":                                      true,
		"{shard}-n{shardNum}-{YYYYMMDDHHIISS}":                               true,
		"{prefix}-shard{shardNum}-{YYYYMMDDHHIISS}":                          true,
		"shard{shardNum}":                                                    false,
		"shard{shardNum}{shardNum}-{YYYYMMDDHHIISS}":                         false,
		"{shard}{shardNum}-{YYYYMMDDHHIISS}":                                 false,
		"shard{shardNum}1-{YYYYMMDDHHIISS}":                                  false,
		"{prefix}{shardNum}-{YYYYMMDDHHIISS}":                                false,
		"shard{shardnum}-{YYYYMMDDHHIISS}":                                   false,
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
	values := BackupNameValues{Shard: "1", ShardNum: 2, Cluster: "main", Chi: "prod", Namespace: "db", Prefix: "nightly",
		Tag: "20261113020000"}
	for template, want := range map[string]string{
		BackupNamingShardTime:           "shard2-20261113020000",
		"shard{shard}-{YYYYMMDDHHIISS}": "shard1-20261113020000",
		"{prefix}-{cluster}-{chi}-{namespace}-s{shardNum}-{YYYY}.{MM}.{DD}_{HH}{II}{SS}": "nightly-main-prod-db-s2-2026.11.13_020000",
		"{DDMMYYYY}-{IIHHSS}-{shard}":          "13112026-000200-1",
		"{shard}-n{shardNum}-{YYYYMMDDHHIISS}": "1-n2-20261113020000",
	} {
		name, err := RenderBackupName(template, values)
		require.NoError(t, err, template)
		require.Equal(t, want, name, template)
	}
	values.Tag = BackupTag(time.Date(2026, time.November, 13, 14, 5, 6, 0, time.UTC), true)
	require.Equal(t, "20261113T140506", values.Tag)
	name, err := RenderBackupName(BackupNamingShardTime, values)
	require.NoError(t, err)
	require.Equal(t, "shard2-20261113T140506", name)

	_, err = RenderBackupName(BackupNamingShardTime, BackupNameValues{ShardNum: 1, Tag: "2026111302"})
	require.Error(t, err, "no tag")
	_, err = RenderBackupName(BackupNamingShardTime, BackupNameValues{Shard: "0", Tag: "20261113020000"})
	require.Error(t, err, "no shard number")
	_, err = RenderBackupName("shard{shard}-{YYYYMMDDHHIISS}", BackupNameValues{ShardNum: 1, Tag: "20261113020000"})
	require.Error(t, err, "no shard name")
	_, err = RenderBackupName("{prefix}-shard{shardNum}-{YYYYMMDDHHIISS}", BackupNameValues{ShardNum: 1, Tag: "20261113020000",
		Prefix: string(make([]byte, 120))})
	require.Error(t, err, "no name")
	_, err = RenderBackupName("shard{shard}", values)
	require.Error(t, err, "no template")
}

// A name tells the shard by its name with {shard}, one of the shards of the cluster, and by its number with
// {shardNum}; the shard names are custom or the default indexes from 0.
func TestParseBackupName(t *testing.T) {
	values := BackupNameValues{Shards: []string{"0", "1"}, Cluster: "main"}
	parsed, ok := ParseBackupName(BackupNamingShardTime, values, "shard2-20261113020000")
	require.True(t, ok)
	require.Equal(t, ParsedBackupName{ShardNum: 2, Tag: "20261113020000"}, parsed)
	parsed, ok = ParseBackupName("shard{shard}-{YYYYMMDDHHIISS}", values, "shard0-20261113T020000")
	require.True(t, ok)
	require.Equal(t, ParsedBackupName{Shard: "0", Tag: "20261113T020000"}, parsed)
	for template, name := range map[string]string{
		BackupNamingShardTime:           "shard0-20261113020000",
		"shard{shard}-{YYYYMMDDHHIISS}": "shard2-20261113020000",
	} {
		_, ok := ParseBackupName(template, values, name)
		require.False(t, ok, "%s %s", template, name)
	}
	_, ok = ParseBackupName("shard{shard}-{YYYYMMDDHHIISS}", BackupNameValues{}, "shard0-20261113020000")
	require.False(t, ok, "no shards")

	values.Shards = []string{"eu.west", "eu", "eu-1"}
	for name, want := range map[string]ParsedBackupName{
		"eu.west-n1-20261113020000": {Shard: "eu.west", ShardNum: 1, Tag: "20261113020000"},
		"eu-n2-20261113020000":      {Shard: "eu", ShardNum: 2, Tag: "20261113020000"},
		"eu-1-n3-20261113020000":    {Shard: "eu-1", ShardNum: 3, Tag: "20261113020000"},
	} {
		parsed, ok := ParseBackupName("{shard}-n{shardNum}-{YYYYMMDDHHIISS}", values, name)
		require.True(t, ok, name)
		require.Equal(t, want, parsed, name)
		rendered, err := RenderBackupName("{shard}-n{shardNum}-{YYYYMMDDHHIISS}",
			BackupNameValues{Shard: want.Shard, ShardNum: want.ShardNum, Tag: want.Tag})
		require.NoError(t, err)
		require.Equal(t, name, rendered)
	}
	_, ok = ParseBackupName("{shard}-n{shardNum}-{YYYYMMDDHHIISS}", values, "euwest-n1-20261113020000")
	require.False(t, ok, "a '.' of a shard name is no wildcard")
}

// A cluster inherits the naming of the installation.
func TestClusterBackupInheritsTheNaming(t *testing.T) {
	installation := &ClusterBackup{Naming: types.NewString(BackupNamingShardTime)}
	require.Equal(t, BackupNamingShardTime, (&ClusterBackup{}).MergeFrom(installation, MergeTypeFillEmptyValues).GetNaming())
	own := &ClusterBackup{Naming: types.NewString("s{shard}-{YYYYMMDDHHIISS}")}
	require.Equal(t, "s{shard}-{YYYYMMDDHHIISS}", own.MergeFrom(installation, MergeTypeFillEmptyValues).GetNaming())
	require.Empty(t, (*ClusterBackup)(nil).GetNaming())
}
