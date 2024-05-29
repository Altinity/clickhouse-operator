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
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

type iNamer interface {
	Name(what interfaces.NameType, params ...any) string
}

type PVCDeleter struct {
	namer iNamer
}

func NewPVCDeleter(namer iNamer) *PVCDeleter {
	return &PVCDeleter{
		namer: namer,
	}
}

// HostCanDeletePVC checks whether PVC on a host can be deleted
func (d *PVCDeleter) HostCanDeletePVC(host *api.Host, pvcName string) bool {
	// In any unknown cases just delete PVC with unclear bindings
	policy := api.PVCReclaimPolicyDelete

	// What host, VolumeMount and VolumeClaimTemplate this PVC is made from?
	host.WalkVolumeMounts(api.CurStatefulSet, func(volumeMount *core.VolumeMount) {
		volumeClaimTemplate, ok := GetVolumeClaimTemplate(host, volumeMount)
		if !ok {
			// No this is not a reference to VolumeClaimTemplate
			return
		}

		if pvcName == d.namer.Name(interfaces.NamePVCNameByVolumeClaimTemplate, host, volumeClaimTemplate) {
			// This PVC is made from these host, VolumeMount and VolumeClaimTemplate
			// So, what policy does this PVC have?
			policy = GetPVCReclaimPolicy(host, volumeClaimTemplate)
			return
		}
	})

	// Delete all explicitly specified as deletable PVCs and all PVCs of un-templated or unclear origin
	return policy == api.PVCReclaimPolicyDelete
}

// HostCanDeleteAllPVCs checks whether all PVCs can be deleted
func (d *PVCDeleter) HostCanDeleteAllPVCs(host *api.Host) bool {
	canDeleteAllPVCs := true
	host.GetCR().WalkVolumeClaimTemplates(func(template *api.VolumeClaimTemplate) {
		if GetPVCReclaimPolicy(host, template) == api.PVCReclaimPolicyRetain {
			// At least one template wants to keep its PVC
			canDeleteAllPVCs = false
		}
	})

	return canDeleteAllPVCs
}
