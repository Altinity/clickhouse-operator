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
		"turned off":  {backup: &ClusterBackup{Enabled: types.NewStringBool(false), ConfigSecretRef: secretRef(secret)}},
		"not a bool":  {backup: &ClusterBackup{Enabled: ptr.To(types.StringBool("maybe")), PodTemplate: types.NewString("backup")}},
		"turned on":   {backup: &ClusterBackup{Enabled: types.NewStringBool(true)}, requested: true},
		"with a Secret": {
			backup:    &ClusterBackup{Enabled: types.NewStringBool(true), ConfigSecretRef: secretRef(secret)},
			requested: true,
			enabled:   true,
		},
		"with a pod template": {
			backup:    &ClusterBackup{Enabled: types.NewStringBool(true), PodTemplate: types.NewString("backup")},
			requested: true,
			enabled:   true,
		},
		"with a Secret of no name": {
			backup:    &ClusterBackup{Enabled: types.NewStringBool(true), ConfigSecretRef: secretRef("")},
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
	installation := &ClusterBackup{ConfigSecretRef: secretRef("s3")}
	for name, tc := range map[string]struct {
		to, from *ClusterBackup
		_type    MergeType
		secret   string
	}{
		"a cluster of nothing": {
			to: &ClusterBackup{}, from: installation, _type: MergeTypeFillEmptyValues, secret: "s3",
		},
		"a cluster of a Secret of its own": {
			to: &ClusterBackup{ConfigSecretRef: secretRef("s3-cold")}, from: installation, _type: MergeTypeFillEmptyValues,
			secret: "s3-cold",
		},
		"a cluster of no Secret": {
			to: &ClusterBackup{ConfigSecretRef: secretRef("")}, from: installation, _type: MergeTypeFillEmptyValues,
		},
		"an installation over a template": {
			to: &ClusterBackup{ConfigSecretRef: secretRef("s3-shared")}, from: &ClusterBackup{ConfigSecretRef: secretRef("s3-own")},
			_type: MergeTypeOverrideByNonEmptyValues, secret: "s3-own",
		},
		"an installation of nothing over a template": {
			to: &ClusterBackup{ConfigSecretRef: secretRef("s3-shared")}, from: &ClusterBackup{Enabled: types.NewStringBool(true)},
			_type: MergeTypeOverrideByNonEmptyValues, secret: "s3-shared",
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := tc.to.DeepCopy().MergeFrom(tc.from, tc._type)
			require.Equal(t, tc.secret, got.GetConfigSecretName())
			require.Equal(t, got, got.DeepCopy().MergeFrom(tc.from, tc._type), "inheriting again changes nothing")
		})
	}
}

// Merging never shares the reference of the source: that of a cached template, or of the installation level, is
// read for every cluster and every installation.
func TestClusterBackupMergeConfigSecretLeavesTheSourceAlone(t *testing.T) {
	from := &ClusterBackup{ConfigSecretRef: secretRef("s3-shared")}
	got := (&ClusterBackup{}).MergeFrom(from, MergeTypeFillEmptyValues)
	got.ConfigSecretRef.Name = "changed"
	require.Equal(t, "s3-shared", from.ConfigSecretRef.Name)
}
