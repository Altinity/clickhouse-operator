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

package model

import (
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// HostHasTablesCreated checks whether host has tables listed as already created
func HostHasTablesCreated(host *api.Host) bool {
	return util.InArray(
		managers.NewNameManager(managers.NameManagerTypeClickHouse).Name(namer.NameFQDN, host),
		host.GetCR().EnsureStatus().GetHostsWithTablesCreated(),
	)
}

// HostFindVolumeClaimTemplateUsedForVolumeMount searches for possible VolumeClaimTemplate which was used to build volume,
// mounted via provided 'volumeMount'. It is not necessarily that VolumeClaimTemplate would be found, because
// some volumeMounts references volumes that were not created from VolumeClaimTemplate.
func HostFindVolumeClaimTemplateUsedForVolumeMount(host *api.Host, volumeMount *core.VolumeMount) (*api.VolumeClaimTemplate, bool) {
	volumeClaimTemplateName := volumeMount.Name

	volumeClaimTemplate, found := host.GetCR().GetVolumeClaimTemplate(volumeClaimTemplateName)
	// Sometimes it is impossible to find VolumeClaimTemplate related to specified volumeMount.
	// May be this volumeMount is not created from VolumeClaimTemplate, it may be a reference to a ConfigMap
	return volumeClaimTemplate, found
}
