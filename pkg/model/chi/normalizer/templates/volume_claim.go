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

package templates

import api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"

// NormalizeVolumeClaimTemplate normalizes .spec.templates.volumeClaimTemplates
func NormalizeVolumeClaimTemplate(template *api.ChiVolumeClaimTemplate) {
	// Check name
	// Skip for now

	// StorageManagement
	normalizeStorageManagement(&template.StorageManagement)

	// Check Spec
	// Skip for now
}

// normalizeStorageManagement normalizes StorageManagement
func normalizeStorageManagement(storage *api.StorageManagement) {
	// Check PVCProvisioner
	if !storage.PVCProvisioner.IsValid() {
		storage.PVCProvisioner = api.PVCProvisionerUnspecified
	}

	// Check PVCReclaimPolicy
	if !storage.PVCReclaimPolicy.IsValid() {
		storage.PVCReclaimPolicy = api.PVCReclaimPolicyUnspecified
	}
}
