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
	host.WalkVolumeMounts(func(volumeMount *v1.VolumeMount) {
		volumeClaimTemplateName := volumeMount.Name
		volumeClaimTemplate, ok := host.CHI.GetVolumeClaimTemplate(volumeClaimTemplateName)
		if !ok {
			// No this is not a reference to VolumeClaimTemplate
			return
		}

		if pvcName == CreatePVCName(host, volumeMount, volumeClaimTemplate) {
			// This PVC is made from these host, VolumeMount and VolumeClaimTemplate
			// So, what policy does this VolumeClaimTemplate have?
			policy = volumeClaimTemplate.PVCReclaimPolicy
			return
		}
	})

	// Delete all explicitly specified as deletable PVCs and all PVCs of un-templated or unclear origin
	return policy == chiv1.PVCReclaimPolicyDelete
}
