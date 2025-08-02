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
)

func GetVolumeClaimTemplate(host *api.Host, volumeMount *core.VolumeMount) (*api.VolumeClaimTemplate, bool) {
	volumeClaimTemplateName := volumeMount.Name
	volumeClaimTemplate, found := host.GetCR().GetVolumeClaimTemplate(volumeClaimTemplateName)
	// Sometimes it is impossible to find VolumeClaimTemplate related to specified volumeMount.
	// May be this volumeMount is not created from VolumeClaimTemplate, it may be a reference to a ConfigMap
	return volumeClaimTemplate, found
}

func GetPVCReclaimPolicy(host *api.Host, template *api.VolumeClaimTemplate) api.PVCReclaimPolicy {
	// Order by priority

	// VolumeClaimTemplate.PVCReclaimPolicy, in case specified
	if template.PVCReclaimPolicy.IsSpecified() {
		return template.PVCReclaimPolicy
	}

	if host.GetCR().GetSpec().GetDefaults().StorageManagement.PVCReclaimPolicy.IsSpecified() {
		return host.GetCR().GetSpec().GetDefaults().StorageManagement.PVCReclaimPolicy
	}

	// Default value
	return api.PVCReclaimPolicyDelete
}

func GetPVCProvisioner(host *api.Host, template *api.VolumeClaimTemplate) api.PVCProvisioner {
	// Order by priority

	// Own PVCProvisioner from the template (VolumeClaimTemplate.PVCProvisioner) has top priority, in case specified
	if template.PVCProvisioner.IsSpecified() {
		return template.PVCProvisioner
	}

	// Then try PVCProvisioner from the CR, in case specified
	if host.GetCR().GetSpec().GetDefaults().StorageManagement.PVCProvisioner.IsSpecified() {
		return host.GetCR().GetSpec().GetDefaults().StorageManagement.PVCProvisioner
	}

	// Default value
	return api.PVCProvisionerStatefulSet
}

// OperatorShouldCreatePVC checks whether operator should create PVC for specified volumeCLimaTemplate
func OperatorShouldCreatePVC(host *api.Host, volumeClaimTemplate *api.VolumeClaimTemplate) bool {
	return GetPVCProvisioner(host, volumeClaimTemplate) == api.PVCProvisionerOperator
}
