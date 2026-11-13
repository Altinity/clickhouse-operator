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
		},
		"with a pod template and a Secret": {
			backup: &ClusterBackup{Enabled: types.NewStringBool(true), PodTemplate: types.NewString("backup"),
				ConfigSecretRef: secretRef(secret)},
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

// The mode of a cluster is sidecar unless it says pod, it is inherited like every other field, and Validate refuses any
// other value.
func TestClusterBackupMode(t *testing.T) {
	require.Equal(t, BackupModeSidecar, (*ClusterBackup)(nil).GetMode())
	require.Equal(t, BackupModeSidecar, (&ClusterBackup{}).GetMode())
	require.Equal(t, BackupModeSidecar, (&ClusterBackup{Mode: types.NewString("")}).GetMode())
	require.Equal(t, BackupModePod, (&ClusterBackup{Mode: types.NewString(BackupModePod)}).GetMode())
	require.True(t, (&ClusterBackup{Mode: types.NewString(BackupModePod)}).IsPodMode())
	require.False(t, (&ClusterBackup{}).IsPodMode())

	for mode, valid := range map[string]bool{"": true, BackupModeSidecar: true, BackupModePod: true, "Pod": false, "job": false} {
		err := (&ClusterBackup{Mode: types.NewString(mode)}).Validate()
		if valid {
			require.NoError(t, err, mode)
		} else {
			require.ErrorIs(t, err, ErrInvalidBackupSpec, mode)
		}
	}
	require.NoError(t, (*ClusterBackup)(nil).Validate())
	require.ErrorIs(t, (&ClusterBackup{Enabled: ptr.To(types.StringBool("maybe"))}).Validate(), ErrInvalidBackupSpec)

	installation := &ClusterBackup{Mode: types.NewString(BackupModePod)}
	require.Equal(t, BackupModePod, (&ClusterBackup{}).MergeFrom(installation, MergeTypeFillEmptyValues).GetMode(),
		"a cluster inherits the mode")
	own := &ClusterBackup{Mode: types.NewString(BackupModeSidecar)}
	require.Equal(t, BackupModeSidecar, own.DeepCopy().MergeFrom(installation, MergeTypeFillEmptyValues).GetMode(),
		"a cluster keeps a mode of its own")
	require.Equal(t, BackupModePod, own.DeepCopy().MergeFrom(installation, MergeTypeOverrideByNonEmptyValues).GetMode())
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

// The orchestrator ConfigMap of a recovery is inherited like its backup name: a cluster that names none takes the
// installation's, an explicit "" resets the cluster to the operator's script, and the reference is copied.
func TestBackupRecoveryMergeOrchestratorConfigMap(t *testing.T) {
	installation := &ClusterBackup{Recovery: &BackupRecovery{OrchestratorConfigMapRef: &core.LocalObjectReference{Name: "my-orchestrator"}}}
	for name, tc := range map[string]struct {
		to        *ClusterBackup
		_type     MergeType
		configMap string
	}{
		"a cluster of nothing": {&ClusterBackup{}, MergeTypeFillEmptyValues, "my-orchestrator"},
		"a cluster of its own": {&ClusterBackup{Recovery: &BackupRecovery{OrchestratorConfigMapRef: &core.LocalObjectReference{Name: "cold"}}},
			MergeTypeFillEmptyValues, "cold"},
		"a cluster reset to the operator's": {&ClusterBackup{Recovery: &BackupRecovery{OrchestratorConfigMapRef: &core.LocalObjectReference{Name: ""}}},
			MergeTypeFillEmptyValues, ""},
		"an installation over a template": {&ClusterBackup{Recovery: &BackupRecovery{OrchestratorConfigMapRef: &core.LocalObjectReference{Name: "shared"}}},
			MergeTypeOverrideByNonEmptyValues, "my-orchestrator"},
	} {
		t.Run(name, func(t *testing.T) {
			got := tc.to.DeepCopy().MergeFrom(installation, tc._type)
			require.Equal(t, tc.configMap, got.GetRecovery().GetOrchestratorConfigMapName())
		})
	}
	got := (&ClusterBackup{}).MergeFrom(installation, MergeTypeFillEmptyValues)
	got.Recovery.OrchestratorConfigMapRef.Name = "changed"
	require.Equal(t, "my-orchestrator", installation.GetRecovery().GetOrchestratorConfigMapName(), "the source is left alone")
	require.Equal(t, "", (*BackupRecovery)(nil).GetOrchestratorConfigMapName())
	require.Equal(t, "backup-orchestrator.sh", BackupOrchestratorScriptKey)
}

// The operations name their orchestrator ConfigMap; a backup that runs a schedule takes the schedule's, as it
// recorded it, unless it names its own.
func TestOperationsNameTheirOrchestratorConfigMap(t *testing.T) {
	ref := func(name string) *core.LocalObjectReference { return &core.LocalObjectReference{Name: name} }
	require.Equal(t, "", (*ClickHouseBackup)(nil).OrchestratorConfigMap())
	require.Equal(t, "", (&ClickHouseBackup{}).OrchestratorConfigMap())
	require.Equal(t, "own", (&ClickHouseBackup{Spec: ClickHouseBackupSpec{OrchestratorConfigMapRef: ref("own")}}).OrchestratorConfigMap())

	scheduled := &ClickHouseBackup{Spec: ClickHouseBackupSpec{ScheduleRef: &BackupScheduleRef{Name: "nightly"}}}
	require.Equal(t, "", scheduled.OrchestratorConfigMap(), "nothing recorded yet")
	scheduled.Status.Schedule = &BackupScheduleRecord{Name: "nightly", OrchestratorConfigMapRef: ref("schedules")}
	require.Equal(t, "schedules", scheduled.OrchestratorConfigMap())
	scheduled.Spec.OrchestratorConfigMapRef = ref("own")
	require.Equal(t, "own", scheduled.OrchestratorConfigMap())
	scheduled.Spec.OrchestratorConfigMapRef = ref("")
	require.Equal(t, "", scheduled.OrchestratorConfigMap(), "an explicit empty name runs the operator's script")

	require.Equal(t, "s", (&ClickHouseBackupScheduleSpec{OrchestratorConfigMapRef: ref("s")}).GetOrchestratorConfigMapName())
	require.Equal(t, "", (*ClickHouseBackupScheduleSpec)(nil).GetOrchestratorConfigMapName())
	require.Equal(t, "r", (&ClickHouseRestoreSpec{OrchestratorConfigMapRef: ref("r")}).GetOrchestratorConfigMapName())
	require.Equal(t, "", (*ClickHouseRestoreSpec)(nil).GetOrchestratorConfigMapName())

	scheduled.Spec.OrchestratorConfigMapRef = ref("own")
	copied := scheduled.DeepCopy()
	copied.Spec.OrchestratorConfigMapRef.Name = "changed"
	copied.Status.Schedule.OrchestratorConfigMapRef.Name = "changed"
	require.Equal(t, "own", scheduled.Spec.OrchestratorConfigMapRef.Name)
	require.Equal(t, "schedules", scheduled.Status.Schedule.OrchestratorConfigMapRef.Name)
}
