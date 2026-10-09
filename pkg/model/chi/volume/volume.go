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

package volume

import (
	"fmt"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
)

type Manager struct {
	cr    api.ICustomResource
	namer *namer.Namer
}

func NewManager() *Manager {
	return &Manager{
		namer: namer.New(),
	}
}

func (m *Manager) SetupVolumes(what interfaces.VolumeType, statefulSet *apps.StatefulSet, host *api.Host) {
	switch what {
	case interfaces.VolumesForConfigMaps:
		m.stsSetupVolumesForConfigMaps(statefulSet, host)
		return
	case interfaces.VolumesUserDataWithFixedPaths:
		m.stsSetupVolumesUserDataWithFixedPaths(statefulSet, host)
		return
	}
	panic("unknown volume type")
}

func (m *Manager) SetCR(cr api.ICustomResource) {
	m.cr = cr
}

// stsSetupVolumesForConfigMaps adds to each container in the Pod VolumeMount objects
func (m *Manager) stsSetupVolumesForConfigMaps(statefulSet *apps.StatefulSet, host *api.Host) {
	configMapCommonName := m.namer.Name(interfaces.NameConfigMapCommon, m.cr)
	configMapCommonUsersName := m.namer.Name(interfaces.NameConfigMapCommonUsers, m.cr)
	configMapHostName := m.namer.Name(interfaces.NameConfigMapHost, host)

	// Add all ConfigMap objects as Volume objects of type ConfigMap
	k8s.StatefulSetAppendVolumes(
		statefulSet,
		k8s.CreateVolumeForConfigMap(configMapCommonName),
		k8s.CreateVolumeForConfigMap(configMapCommonUsersName),
		k8s.CreateVolumeForConfigMap(configMapHostName),
	)

	// config.d and host conf.d are not credential-bearing and stay on every container.
	k8s.StatefulSetAppendVolumeMountsInAllContainers(
		statefulSet,
		k8s.CreateVolumeMount(configMapCommonName, config.DirPathConfigCommon),
		k8s.CreateVolumeMount(configMapHostName, config.DirPathConfigHost),
	)

	if m.cr.GetRuntime().GetAttributes().GetHotReloadUsers() {
		secretName := m.namer.Name(interfaces.NameSecretCommonUsers, m.cr)
		if err := appendHotReloadUsersVolume(statefulSet, configMapCommonUsersName, secretName); err != nil {
			log.New().F().Error("unable to mount hot-reload users configuration: %s", err)
		}
		return
	}

	// No hot-reload credentials. Mount users.d on every container, as before.
	k8s.StatefulSetAppendVolumeMountsInAllContainers(
		statefulSet,
		k8s.CreateVolumeMount(configMapCommonUsersName, config.DirPathConfigUsers),
	)
}

// appendHotReloadUsersVolume projects the users ConfigMap and the hot-reload users
// Secret into users.d on the ClickHouse container only. The ConfigMap keeps
// chop-generated-users.xml for users that do not opt in. The Secret adds
// chop-generated-hot-reload-users.xml. Sidecars get the ConfigMap alone.
// The application container is the one named clickhouse, or the first container
// when a PodTemplate renames it.
func appendHotReloadUsersVolume(statefulSet *apps.StatefulSet, configMapName, secretName string) error {
	volumeName := secretName
	k8s.StatefulSetAppendVolumes(
		statefulSet,
		k8s.CreateProjectedConfigAndSecretVolume(
			volumeName,
			configMapName,
			secretName,
			config.ChopGeneratedHotReloadUsersConfigFilename(),
		),
	)
	app, ok := k8s.StatefulSetContainerGet(statefulSet, config.ClickHouseContainerName, 0)
	k8s.StatefulSetWalkContainers(statefulSet, func(container *core.Container) {
		if ok && container == app {
			return
		}
		k8s.ContainerAppendVolumeMounts(container, k8s.CreateVolumeMount(configMapName, config.DirPathConfigUsers))
	})
	if !ok {
		return fmt.Errorf("application container not found")
	}
	k8s.ContainerAppendVolumeMounts(app, k8s.CreateVolumeMount(volumeName, config.DirPathConfigUsers))
	return nil
}

// stsSetupVolumesUserDataWithFixedPaths
// appends VolumeMounts for Data and Log VolumeClaimTemplates on all containers.
// Creates VolumeMounts for Data and Log volumes in case these volume templates are specified in `templates`.
func (m *Manager) stsSetupVolumesUserDataWithFixedPaths(statefulSet *apps.StatefulSet, host *api.Host) {
	// Mount all named (data and log so far) VolumeClaimTemplates into all containers
	k8s.StatefulSetAppendVolumeMountsInAllContainers(
		statefulSet,
		k8s.CreateVolumeMount(host.GetTemplates().GetDataVolumeClaimTemplate(), config.DirPathDataStorage),
		k8s.CreateVolumeMount(host.GetTemplates().GetLogVolumeClaimTemplate(), config.DirPathLogStorage),
	)
}
