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
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/common/creator"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
)

type VolumeManagerClickHouse struct {
	cr    api.ICustomResource
	namer *namer.NamerClickHouse
}

func NewVolumeManagerClickHouse() *VolumeManagerClickHouse {
	return &VolumeManagerClickHouse{
		namer: namer.NewClickHouse(),
	}
}

func (m *VolumeManagerClickHouse) SetupVolumes(what creator.VolumeType, statefulSet *apps.StatefulSet, host *api.Host) {
	switch what {
	case creator.VolumesForConfigMaps:
		m.stsSetupVolumesForConfigMaps(statefulSet, host)
		return
	case creator.VolumesUserDataWithFixedPaths:
		m.stsSetupVolumesUserDataWithFixedPaths(statefulSet, host)
		return
	}
	panic("unknown volume type")
}

func (m *VolumeManagerClickHouse) SetCR(cr api.ICustomResource) {
	m.cr = cr
}

// stsSetupVolumesForConfigMaps adds to each container in the Pod VolumeMount objects
func (m *VolumeManagerClickHouse) stsSetupVolumesForConfigMaps(statefulSet *apps.StatefulSet, host *api.Host) {
	configMapHostName := m.namer.Name(namer.NameConfigMapHost, host)
	configMapCommonName := m.namer.Name(namer.NameConfigMapCommon, m.cr)
	configMapCommonUsersName := m.namer.Name(namer.NameConfigMapCommonUsers, m.cr)

	// Add all ConfigMap objects as Volume objects of type ConfigMap
	k8s.StatefulSetAppendVolumes(
		statefulSet,
		k8s.CreateVolumeForConfigMap(configMapCommonName),
		k8s.CreateVolumeForConfigMap(configMapCommonUsersName),
		k8s.CreateVolumeForConfigMap(configMapHostName),
		//createVolumeForConfigMap(configMapHostMigrationName),
	)

	// And reference these Volumes in each Container via VolumeMount
	// So Pod will have ConfigMaps mounted as Volumes in each Container
	k8s.StatefulSetAppendVolumeMountsInAllContainers(
		statefulSet,
		k8s.CreateVolumeMount(configMapCommonName, config.DirPathCommonConfig),
		k8s.CreateVolumeMount(configMapCommonUsersName, config.DirPathUsersConfig),
		k8s.CreateVolumeMount(configMapHostName, config.DirPathHostConfig),
	)
}

// stsSetupVolumesUserDataWithFixedPaths
// appends VolumeMounts for Data and Log VolumeClaimTemplates on all containers.
// Creates VolumeMounts for Data and Log volumes in case these volume templates are specified in `templates`.
func (m *VolumeManagerClickHouse) stsSetupVolumesUserDataWithFixedPaths(statefulSet *apps.StatefulSet, host *api.Host) {
	// Mount all named (data and log so far) VolumeClaimTemplates into all containers
	k8s.StatefulSetAppendVolumeMountsInAllContainers(
		statefulSet,
		k8s.CreateVolumeMount(host.Templates.GetDataVolumeClaimTemplate(), config.DirPathClickHouseData),
		k8s.CreateVolumeMount(host.Templates.GetLogVolumeClaimTemplate(), config.DirPathClickHouseLog),
	)
}
