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

package creator_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	chiCreator "github.com/altinity/clickhouse-operator/pkg/model/chi/creator"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

// podMode enables backup in backup.mode pod
func podMode() *chi.ClusterBackup {
	backup := enabled()
	backup.Mode = types.NewString(chi.BackupModePod)
	return backup
}

// A cluster backed up by backup Pods runs no sidecar: its pods get neither the container, nor the volume of the
// configuration file, nor the port of the REST API on their Service - the backup Pods serve the API instead. The
// container those serve it with is the sidecar as it would be built.
func TestBackupPodModeHasNoSidecar(t *testing.T) {
	cr := normalize(t, newCR(newCluster(backedUpCluster, podMode())))
	sts, host := createStatefulSet(t, cr, backedUpCluster)
	requireNoBackupContainer(t, sts)
	for _, volume := range sts.Spec.Template.Spec.Volumes {
		require.NotEqual(t, config.ClickHouseBackupConfigVolumeName, volume.Name)
		require.NotEqual(t, config.ClickHouseBackupSharedDataVolumeName, volume.Name)
	}
	require.False(t, chiCreator.HostServesBackupAPI(host))
	svc := buildCreator(cr).CreateService(interfaces.ServiceHost, host).First()
	require.False(t, chiCreator.ServiceHasBackupPort(svc))

	containers := managers.NewContainerManager(managers.ContainerManagerTypeClickHouse)
	_, ok := containers.NewBackupContainer(host)
	require.False(t, ok)
	server, ok := containers.NewBackupServerContainer(host)
	require.True(t, ok)
	require.Equal(t, config.ClickHouseBackupContainerName, server.Name)
	require.True(t, chiCreator.IsBackupSidecarCommand(&server))

	sidecar := normalize(t, newCR(newCluster(backedUpCluster, enabled())))
	sidecarHost := findHost(t, sidecar, backedUpCluster)
	require.True(t, chiCreator.HostServesBackupAPI(sidecarHost))
	require.True(t, chiCreator.ServiceHasBackupPort(buildCreator(sidecar).CreateService(interfaces.ServiceHost, sidecarHost).First()))
}

// The sidecar runs the stable clickhouse-backup and pulls it Always, as the tag moves; an image or a pull policy
// the clickhouse-backup container of the backup pod template sets wins
func TestBackupSidecarImagePullPolicy(t *testing.T) {
	cr := normalize(t, newCR(newCluster(backedUpCluster, enabled())))
	sts, _ := createStatefulSet(t, cr, backedUpCluster)
	sidecar := requireBackupContainer(t, sts)
	require.Equal(t, "altinity/clickhouse-backup:stable", sidecar.Image)
	require.Equal(t, core.PullAlways, sidecar.ImagePullPolicy)

	cluster := newCluster(backedUpCluster, enabled())
	own := newCR(cluster)
	sidecarOn(own, cluster, core.Container{Image: "registry/backup:2.6", ImagePullPolicy: core.PullIfNotPresent})
	sts, _ = createStatefulSet(t, normalize(t, own), backedUpCluster)
	sidecar = requireBackupContainer(t, sts)
	require.Equal(t, "registry/backup:2.6", sidecar.Image)
	require.Equal(t, core.PullIfNotPresent, sidecar.ImagePullPolicy)
}

// Retention is done by the orchestrator alone: the sidecar keeps every remote backup and rebases on delete, over
// whatever the clickhouse-backup container of the backup pod template sets - the env vars win over the file
func TestBackupSidecarPinsRetentionSettings(t *testing.T) {
	cluster := newCluster(backedUpCluster, enabled())
	cr := newCR(cluster)
	sidecarOn(cr, cluster, core.Container{Env: []core.EnvVar{
		{Name: config.ClickHouseBackupEnvBackupsToKeepRemote, Value: "7"},
		{Name: config.ClickHouseBackupEnvRebaseDuringDelete, Value: "false"},
		{Name: config.ClickHouseBackupEnvRebaseBeforeRemoveOldRemote, Value: "false"},
	}})
	sts, _ := createStatefulSet(t, normalize(t, cr), backedUpCluster)
	sidecar := requireBackupContainer(t, sts)
	require.Equal(t, "0", envValue(t, sidecar, config.ClickHouseBackupEnvBackupsToKeepRemote))
	require.Equal(t, "true", envValue(t, sidecar, config.ClickHouseBackupEnvRebaseDuringDelete))
	require.Equal(t, "true", envValue(t, sidecar, config.ClickHouseBackupEnvRebaseBeforeRemoveOldRemote))
}
