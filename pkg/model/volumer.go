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
	v1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

func getPVCReclaimPolicy(host *v1.ChiHost, template *v1.ChiVolumeClaimTemplate) v1.PVCReclaimPolicy {
	// Order by priority

	// VolumeClaimTemplate.PVCReclaimPolicy, in case specified
	if template.PVCReclaimPolicy != v1.PVCReclaimPolicyUnspecified {
		return template.PVCReclaimPolicy
	}

	if host.CHI.Spec.Defaults.StorageManagement.PVCReclaimPolicy != v1.PVCReclaimPolicyUnspecified {
		return host.CHI.Spec.Defaults.StorageManagement.PVCReclaimPolicy
	}

	// Default value
	return v1.PVCReclaimPolicyDelete
}

func getPVCProvisioner(host *v1.ChiHost, template *v1.ChiVolumeClaimTemplate) v1.PVCProvisioner {
	// Order by priority

	// VolumeClaimTemplate.PVCProvisioner, in case specified
	if template.PVCProvisioner != v1.PVCProvisionerUnspecified {
		return template.PVCProvisioner
	}

	if host.CHI.Spec.Defaults.StorageManagement.PVCProvisioner != v1.PVCProvisionerUnspecified {
		return host.CHI.Spec.Defaults.StorageManagement.PVCProvisioner
	}

	// Default value
	return v1.PVCProvisionerStatefulSet
}
