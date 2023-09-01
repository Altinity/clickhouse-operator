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
	"k8s.io/api/core/v1"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// HostCanDeletePVC checks whether PVC on a host can be deleted
func HostCanDeletePVC(host *chiv1.ChiHost, pvcName string) bool {
	// In any unknown cases just delete PVC with unclear bindings
	policy := chiv1.PVCReclaimPolicyDelete

	// What host, VolumeMount and VolumeClaimTemplate this PVC is made from?
	host.WalkVolumeMounts(chiv1.CurStatefulSet, func(volumeMount *v1.VolumeMount) {
		volumeClaimTemplate, ok := GetVolumeClaimTemplate(host, volumeMount)
		if !ok {
			// No this is not a reference to VolumeClaimTemplate
			return
		}

		if pvcName == CreatePVCNameByVolumeClaimTemplate(host, volumeClaimTemplate) {
			// This PVC is made from these host, VolumeMount and VolumeClaimTemplate
			// So, what policy does this PVC have?
			policy = getPVCReclaimPolicy(host, volumeClaimTemplate)
			return
		}
	})

	// Delete all explicitly specified as deletable PVCs and all PVCs of un-templated or unclear origin
	return policy == chiv1.PVCReclaimPolicyDelete
}

// HostCanDeleteAllPVCs checks whether all PVCs can be deleted
func HostCanDeleteAllPVCs(host *chiv1.ChiHost) bool {
	canDeleteAllPVCs := true
	host.CHI.WalkVolumeClaimTemplates(func(template *chiv1.ChiVolumeClaimTemplate) {
		if getPVCReclaimPolicy(host, template) == chiv1.PVCReclaimPolicyRetain {
			// At least one template wants to keep its PVC
			canDeleteAllPVCs = false
		}
	})

	return canDeleteAllPVCs
}
