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

package creator

import (
	apps "k8s.io/api/apps/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

type VolumeType string

const (
	VolumesForConfigMaps          VolumeType = "VolumesForConfigMaps"
	VolumesUserDataWithFixedPaths VolumeType = "VolumesUserDataWithFixedPaths"
)

type IVolumeManager interface {
	SetupVolumes(what VolumeType, statefulSet *apps.StatefulSet, host *api.Host)
	SetCR(cr api.ICustomResource)
}

type VolumeManagerType string

const (
	VolumeManagerTypeClickHouse VolumeManagerType = "clickhouse"
	VolumeManagerTypeKeeper     VolumeManagerType = "keeper"
)

func NewVolumeManager(what VolumeManagerType) IVolumeManager {
	switch what {
	case VolumeManagerTypeClickHouse:
		return NewVolumeManagerClickHouse()
	}
	panic("unknown volume manager type")
}
