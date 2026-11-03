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

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// Backup turned on is on only where the spec names where the storage settings of the sidecar come from:
// without them the sidecar could never upload or download a backup, so backup stays off there instead.
func TestClusterBackupNeedsStorage(t *testing.T) {
	const secret = "s3-credentials"
	for name, tc := range map[string]struct {
		backup    *ClusterBackup
		requested bool
		enabled   bool
	}{
		"none":        {},
		"nothing set": {backup: &ClusterBackup{}},
		"turned off":  {backup: &ClusterBackup{Enabled: types.NewStringBool(false), Storages: []BackupStorage{storageOf("main", secret)}}},
		"not a bool":  {backup: &ClusterBackup{Enabled: ptr.To(types.StringBool("maybe")), PodTemplate: types.NewString("backup")}},
		"turned on":   {backup: &ClusterBackup{Enabled: types.NewStringBool(true)}, requested: true},
		"with a Secret": {
			backup:    &ClusterBackup{Enabled: types.NewStringBool(true), Storages: []BackupStorage{storageOf("main", secret)}},
			requested: true,
			enabled:   true,
		},
		"with a pod template": {
			backup:    &ClusterBackup{Enabled: types.NewStringBool(true), PodTemplate: types.NewString("backup")},
			requested: true,
			enabled:   true,
		},
		"with a storage of no Secret": {
			backup:    &ClusterBackup{Enabled: types.NewStringBool(true), Storages: []BackupStorage{{Name: "main"}}},
			requested: true,
		},
		"with no storage named": {
			backup: &ClusterBackup{
				Enabled:  types.NewStringBool(true),
				Storages: []BackupStorage{storageOf("main", secret)},
				Storage:  types.NewString(""),
			},
			requested: true,
		},
		"with a storage it does not declare": {
			backup: &ClusterBackup{
				Enabled:  types.NewStringBool(true),
				Storages: []BackupStorage{storageOf("main", secret)},
				Storage:  types.NewString("dr"),
			},
			requested: true,
		},
		"with a pod template of no name": {
			backup:    &ClusterBackup{Enabled: types.NewStringBool(true), PodTemplate: types.NewString("")},
			requested: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.requested, tc.backup.IsRequested(), "requested")
			require.Equal(t, tc.enabled, tc.backup.IsEnabled(), "enabled")
			require.Equal(t, tc.requested && !tc.enabled, tc.backup.LacksStorage(), "lacks storage")
		})
	}
}

// storageOf declares a storage of the given Secret
func storageOf(name, secret string) BackupStorage {
	return BackupStorage{Name: name, SecretRef: &core.LocalObjectReference{Name: secret}}
}

// The storage the sidecar uses is the one the backup names, or else the only one it declares. A name it does not
// declare, or several storages and no name, cannot be resolved: the sidecar would take no storage, or another one
// than meant.
func TestClusterBackupResolveStorage(t *testing.T) {
	for name, tc := range map[string]struct {
		backup *ClusterBackup
		want   string
		err    error
	}{
		"none":                         {},
		"none declared":                {backup: &ClusterBackup{Enabled: types.NewStringBool(true)}},
		"the only one declared":        {backup: &ClusterBackup{Storages: []BackupStorage{storageOf("main", "s3")}}, want: "main"},
		"the one named":                {backup: &ClusterBackup{Storages: []BackupStorage{storageOf("main", "s3"), storageOf("dr", "s3-dr")}, Storage: types.NewString("dr")}, want: "dr"},
		"none named explicitly":        {backup: &ClusterBackup{Storages: []BackupStorage{storageOf("main", "s3")}, Storage: types.NewString("")}},
		"one named, none declared":     {backup: &ClusterBackup{Storage: types.NewString("main")}, err: ErrBackupStorageNotFound},
		"one named, another declared":  {backup: &ClusterBackup{Storages: []BackupStorage{storageOf("main", "s3")}, Storage: types.NewString("dr")}, err: ErrBackupStorageNotFound},
		"several declared, none named": {backup: &ClusterBackup{Storages: []BackupStorage{storageOf("main", "s3"), storageOf("dr", "s3-dr")}}, err: ErrBackupStorageNotChosen},
	} {
		t.Run(name, func(t *testing.T) {
			storage, err := tc.backup.ResolveStorage()
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, tc.want, storage.GetName())
			require.Equal(t, tc.want, tc.backup.GetStorageName())
		})
	}
}

// Storages pair by name, and the storage each level uses is taken as that level tells it - the one it names, or
// else its only one - so that the storage of a level wins over the one of the level it takes others from: a
// cluster's over the installation's, an installation's over a template's. A storage nobody declared twice is never
// lost.
func TestClusterBackupMergeStorages(t *testing.T) {
	installation := &ClusterBackup{Storages: []BackupStorage{storageOf("main", "s3")}}
	for name, tc := range map[string]struct {
		to, from *ClusterBackup
		_type    MergeType
		storage  string
		secret   string
		names    []string
	}{
		"a cluster of nothing": {
			to: &ClusterBackup{}, from: installation, _type: MergeTypeFillEmptyValues,
			storage: "main", secret: "s3", names: []string{"main"},
		},
		"a cluster of a storage of its own": {
			to: &ClusterBackup{Storages: []BackupStorage{storageOf("cold", "s3-cold")}}, from: installation, _type: MergeTypeFillEmptyValues,
			storage: "cold", secret: "s3-cold", names: []string{"cold", "main"},
		},
		"a cluster of its own storage of the installation's name": {
			to: &ClusterBackup{Storages: []BackupStorage{storageOf("main", "s3-cold")}}, from: installation, _type: MergeTypeFillEmptyValues,
			storage: "main", secret: "s3-cold", names: []string{"main"},
		},
		"a cluster of its own storage of the installation's name, without a Secret": {
			to: &ClusterBackup{Storages: []BackupStorage{{Name: "main"}}}, from: installation, _type: MergeTypeFillEmptyValues,
			storage: "main", secret: "s3", names: []string{"main"},
		},
		"a cluster naming the installation's storage over its own": {
			to:   &ClusterBackup{Storages: []BackupStorage{storageOf("cold", "s3-cold")}, Storage: types.NewString("main")},
			from: installation, _type: MergeTypeFillEmptyValues,
			storage: "main", secret: "s3", names: []string{"cold", "main"},
		},
		"a cluster of no storage": {
			to: &ClusterBackup{Storage: types.NewString("")}, from: installation, _type: MergeTypeFillEmptyValues,
			names: []string{"main"},
		},
		"an installation over a template": {
			to:   &ClusterBackup{Storages: []BackupStorage{storageOf("shared", "s3-shared")}},
			from: &ClusterBackup{Storages: []BackupStorage{storageOf("own", "s3-own")}}, _type: MergeTypeOverrideByNonEmptyValues,
			storage: "own", secret: "s3-own", names: []string{"shared", "own"},
		},
		"an installation of nothing over a template": {
			to: &ClusterBackup{Storages: []BackupStorage{storageOf("shared", "s3-shared")}}, from: &ClusterBackup{Enabled: types.NewStringBool(true)},
			_type: MergeTypeOverrideByNonEmptyValues, storage: "shared", secret: "s3-shared", names: []string{"shared"},
		},
		"an installation of a template's storage without a Secret": {
			to:   &ClusterBackup{Storages: []BackupStorage{storageOf("main", "s3-shared")}},
			from: &ClusterBackup{Storages: []BackupStorage{{Name: "main"}}}, _type: MergeTypeOverrideByNonEmptyValues,
			storage: "main", secret: "s3-shared", names: []string{"main"},
		},
		"an installation's Secret over a template's of the same storage": {
			to:   &ClusterBackup{Storages: []BackupStorage{storageOf("main", "s3-shared")}},
			from: &ClusterBackup{Storages: []BackupStorage{storageOf("main", "s3-own")}}, _type: MergeTypeOverrideByNonEmptyValues,
			storage: "main", secret: "s3-own", names: []string{"main"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := tc.to.DeepCopy().MergeFrom(tc.from, tc._type)
			require.Equal(t, tc.storage, got.GetStorageName())
			require.Equal(t, tc.secret, got.GetStorageSecretName())
			var names []string
			for _, storage := range got.GetStorages() {
				names = append(names, storage.Name)
			}
			require.Equal(t, tc.names, names)
			require.Equal(t, got, got.DeepCopy().MergeFrom(tc.from, tc._type), "inheriting again changes nothing")
		})
	}
}

// Merging never writes into the storages of either side: those of a cached template, or of the installation
// level, are read for every cluster and every installation.
func TestClusterBackupMergeStoragesLeavesTheSourcesAlone(t *testing.T) {
	cached := make([]BackupStorage, 1, 4)
	cached[0] = storageOf("main", "s3-shared")
	to := &ClusterBackup{Storages: cached}
	from := &ClusterBackup{Storages: []BackupStorage{storageOf("main", "s3-own"), storageOf("dr", "s3-dr")}}

	to.MergeFrom(from, MergeTypeOverrideByNonEmptyValues)
	require.Equal(t, "s3-shared", cached[0].SecretRef.Name, "the cached storage keeps its Secret")
	require.Equal(t, BackupStorage{}, cached[:2][1], "nothing is appended into the cached storages")
	require.Equal(t, []BackupStorage{storageOf("main", "s3-own"), storageOf("dr", "s3-dr")}, from.Storages)
}
