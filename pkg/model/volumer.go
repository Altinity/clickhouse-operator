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
	coreV1 "k8s.io/api/core/v1"

	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

func (c *Creator) getVolumeClaimTemplate(volumeMount *coreV1.VolumeMount) (*chop.ChiVolumeClaimTemplate, bool) {
	volumeClaimTemplateName := volumeMount.Name
	volumeClaimTemplate, ok := c.chi.GetVolumeClaimTemplate(volumeClaimTemplateName)
	// Sometimes it is impossible to find VolumeClaimTemplate related to specified volumeMount.
	// May be this volumeMount is not created from VolumeClaimTemplate, it may be a reference to a ConfigMap
	return volumeClaimTemplate, ok
}

func GetVolumeClaimTemplate(host *chop.ChiHost, volumeMount *coreV1.VolumeMount) (*chop.ChiVolumeClaimTemplate, bool) {
	volumeClaimTemplateName := volumeMount.Name
	volumeClaimTemplate, ok := host.CHI.GetVolumeClaimTemplate(volumeClaimTemplateName)
	// Sometimes it is impossible to find VolumeClaimTemplate related to specified volumeMount.
	// May be this volumeMount is not created from VolumeClaimTemplate, it may be a reference to a ConfigMap
	return volumeClaimTemplate, ok
}

func getPVCReclaimPolicy(host *chop.ChiHost, template *chop.ChiVolumeClaimTemplate) chop.PVCReclaimPolicy {
	// Order by priority

	// VolumeClaimTemplate.PVCReclaimPolicy, in case specified
	if template.PVCReclaimPolicy != chop.PVCReclaimPolicyUnspecified {
		return template.PVCReclaimPolicy
	}

	if host.CHI.Spec.Defaults.StorageManagement.PVCReclaimPolicy != chop.PVCReclaimPolicyUnspecified {
		return host.CHI.Spec.Defaults.StorageManagement.PVCReclaimPolicy
	}

	// Default value
	return chop.PVCReclaimPolicyDelete
}

func getPVCProvisioner(host *chop.ChiHost, template *chop.ChiVolumeClaimTemplate) chop.PVCProvisioner {
	// Order by priority

	// VolumeClaimTemplate.PVCProvisioner, in case specified
	if template.PVCProvisioner != chop.PVCProvisionerUnspecified {
		return template.PVCProvisioner
	}

	if host.CHI.Spec.Defaults.StorageManagement.PVCProvisioner != chop.PVCProvisionerUnspecified {
		return host.CHI.Spec.Defaults.StorageManagement.PVCProvisioner
	}

	// Default value
	return chop.PVCProvisionerStatefulSet
}
