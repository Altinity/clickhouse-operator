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

// Backup turned on is on only where the spec names the Secret of the clickhouse-backup configuration, the only source
// of the storage settings and of the REST API credentials: a pod template alone only overrides the container.
func TestClusterBackupNeedsStorage(t *testing.T) {
	const secret = "s3-credentials"
	for name, tc := range map[string]struct {
		backup    *ClusterBackup
		requested bool
		enabled   bool
	}{
		"none":        {},
		"nothing set": {backup: &ClusterBackup{}},
		"turned off":  {backup: &ClusterBackup{Enabled: types.NewStringBool(false), StorageConfig: NewStorageConfig(secret, "config.yml")}},
		"not a bool":  {backup: &ClusterBackup{Enabled: ptr.To(types.StringBool("maybe")), PodTemplate: types.NewString("backup")}},
		"turned on":   {backup: &ClusterBackup{Enabled: types.NewStringBool(true)}, requested: true},
		"with a Secret": {
			backup:    &ClusterBackup{Enabled: types.NewStringBool(true), StorageConfig: NewStorageConfig(secret, "config.yml")},
			requested: true,
			enabled:   true,
		},
		"with a pod template": {
			backup:    &ClusterBackup{Enabled: types.NewStringBool(true), PodTemplate: types.NewString("backup")},
			requested: true,
		},
		"with a pod template and a Secret": {
			backup: &ClusterBackup{Enabled: types.NewStringBool(true), PodTemplate: types.NewString("backup"),
				StorageConfig: NewStorageConfig(secret, "config.yml")},
			requested: true,
			enabled:   true,
		},
		"with a Secret of no name": {
			backup:    &ClusterBackup{Enabled: types.NewStringBool(true), StorageConfig: NewStorageConfig("", "config.yml")},
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

// secretRef references the named Secret
func secretRef(name string) *core.LocalObjectReference {
	return &core.LocalObjectReference{Name: name}
}

// The configuration Secret of a level wins over the one of the level it takes others from: a cluster's over the
// installation's, an installation's over a template's. An explicit name "" on a cluster leaves it without one.
func TestClusterBackupMergeConfigSecret(t *testing.T) {
	installation := &ClusterBackup{StorageConfig: NewStorageConfig("s3", "config.yml")}
	for name, tc := range map[string]struct {
		to, from *ClusterBackup
		_type    MergeType
		secret   string
	}{
		"a cluster of nothing": {
			to: &ClusterBackup{}, from: installation, _type: MergeTypeFillEmptyValues, secret: "s3",
		},
		"a cluster of a Secret of its own": {
			to: &ClusterBackup{StorageConfig: NewStorageConfig("s3-cold", "config.yml")}, from: installation, _type: MergeTypeFillEmptyValues,
			secret: "s3-cold",
		},
		"a cluster of no Secret": {
			to: &ClusterBackup{StorageConfig: NewStorageConfig("", "config.yml")}, from: installation, _type: MergeTypeFillEmptyValues,
		},
		"an installation over a template": {
			to: &ClusterBackup{StorageConfig: NewStorageConfig("s3-shared", "config.yml")}, from: &ClusterBackup{StorageConfig: NewStorageConfig("s3-own", "config.yml")},
			_type: MergeTypeOverrideByNonEmptyValues, secret: "s3-own",
		},
		"an installation of nothing over a template": {
			to: &ClusterBackup{StorageConfig: NewStorageConfig("s3-shared", "config.yml")}, from: &ClusterBackup{Enabled: types.NewStringBool(true)},
			_type: MergeTypeOverrideByNonEmptyValues, secret: "s3-shared",
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := tc.to.DeepCopy().MergeFrom(tc.from, tc._type)
			require.Equal(t, tc.secret, got.GetStorageConfigSecretName())
			require.Equal(t, got, got.DeepCopy().MergeFrom(tc.from, tc._type), "inheriting again changes nothing")
		})
	}
}

// Merging never shares the reference of the source: that of a cached template, or of the installation level, is
// read for every cluster and every installation.
func TestClusterBackupMergeConfigSecretLeavesTheSourceAlone(t *testing.T) {
	from := &ClusterBackup{StorageConfig: NewStorageConfig("s3-shared", "config.yml")}
	got := (&ClusterBackup{}).MergeFrom(from, MergeTypeFillEmptyValues)
	got.StorageConfig.ValueFrom.SecretKeyRef.Name = "changed"
	require.Equal(t, "s3-shared", from.GetStorageConfigSecretName())
}

// The storage config names a Secret and a key of it, both required for a storage; a hot reload and an optional key
// are refused, and a name "" is the way a cluster leaves itself without a storage, which needs no key.
func TestClusterBackupStorageConfig(t *testing.T) {
	secretKey := func(name, key string, optional *bool) *StorageConfig {
		return &StorageConfig{ValueFrom: &types.DataSource{
			SecretKeyRef: &core.SecretKeySelector{LocalObjectReference: core.LocalObjectReference{Name: name}, Key: key, Optional: optional},
		}}
	}
	for name, tc := range map[string]struct {
		config  *StorageConfig
		secret  string
		key     string
		invalid string
		enabled bool
	}{
		"none":              {},
		"no source":         {config: &StorageConfig{}},
		"a Secret key":      {config: secretKey("s3", "storage.yaml", nil), secret: "s3", key: "storage.yaml", enabled: true},
		"an optional false": {config: secretKey("s3", "config.yml", ptr.To(false)), secret: "s3", key: "config.yml", enabled: true},
		"a Secret of no key": {
			config: secretKey("s3", "", nil), secret: "s3", invalid: "backup.storageConfig.valueFrom.secretKeyRef.key",
		},
		"no Secret": {config: secretKey("", "", nil)},
		"an optional Secret": {
			config: secretKey("s3", "config.yml", ptr.To(true)), secret: "s3", key: "config.yml",
			invalid: "backup.storageConfig.valueFrom.secretKeyRef.optional", enabled: true,
		},
		"a hot reload": {
			config: &StorageConfig{ValueFrom: &types.DataSource{HotReload: true}}, invalid: "backup.storageConfig.valueFrom.hotReload",
		},
	} {
		t.Run(name, func(t *testing.T) {
			backup := &ClusterBackup{Enabled: types.NewStringBool(true), StorageConfig: tc.config}
			require.Equal(t, tc.secret, backup.GetStorageConfigSecretName())
			require.Equal(t, tc.key, backup.GetStorageConfigSecretKey())
			require.Equal(t, tc.enabled, backup.IsEnabled())
			require.Equal(t, !tc.enabled, backup.LacksStorage())
			if tc.invalid == "" {
				require.NoError(t, backup.Validate())
			} else {
				require.ErrorIs(t, backup.Validate(), ErrInvalidBackupSpec)
				require.ErrorContains(t, backup.Validate(), tc.invalid)
			}
		})
	}
}
