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
	apps "k8s.io/api/apps/v1"

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

func secretProjections(files []api.SecretConfigFile) []k8s.SecretProjection {
	out := make([]k8s.SecretProjection, 0, len(files))
	for _, file := range files {
		out = append(out, k8s.SecretProjection{Secret: file.Secret, Key: file.Key, Path: file.Path})
	}
	return out
}

func (m *Manager) SetCR(cr api.ICustomResource) {
	m.cr = cr
}

// stsSetupVolumesForConfigMaps adds to each container in the Pod VolumeMount objects
func (m *Manager) stsSetupVolumesForConfigMaps(statefulSet *apps.StatefulSet, host *api.Host) {
	configMapCommonName := m.namer.Name(interfaces.NameConfigMapCommon, m.cr)
	configMapCommonUsersName := m.namer.Name(interfaces.NameConfigMapCommonUsers, m.cr)
	configMapHostName := m.namer.Name(interfaces.NameConfigMapHost, host)

	// mappingType=file secrets are projected next to the ConfigMap that serves
	// them: config.d, users.d, or that host's conf.d. An empty list keeps the
	// plain ConfigMap volume.
	attrs := m.cr.GetRuntime().GetAttributes()
	k8s.StatefulSetAppendVolumes(
		statefulSet,
		k8s.CreateConfigVolume(configMapCommonName, secretProjections(attrs.SecretConfigFiles(api.SecretConfigFileTargetCommon, ""))),
		k8s.CreateConfigVolume(configMapCommonUsersName, secretProjections(attrs.SecretConfigFiles(api.SecretConfigFileTargetUsers, ""))),
		k8s.CreateConfigVolume(configMapHostName, secretProjections(attrs.SecretConfigFiles(api.SecretConfigFileTargetHost, host.GetName()))),
	)

	// And reference these Volumes in each Container via VolumeMount
	// So Pod will have ConfigMaps mounted as Volumes in each Container
	k8s.StatefulSetAppendVolumeMountsInAllContainers(
		statefulSet,
		k8s.CreateVolumeMount(configMapCommonName, config.DirPathConfigCommon),
		k8s.CreateVolumeMount(configMapCommonUsersName, config.DirPathConfigUsers),
		k8s.CreateVolumeMount(configMapHostName, config.DirPathConfigHost),
	)
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
