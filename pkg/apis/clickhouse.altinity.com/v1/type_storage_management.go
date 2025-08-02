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

package v1

// StorageManagement defines storage management config
type StorageManagement struct {
	PVCProvisioner   PVCProvisioner   `json:"provisioner,omitempty"   yaml:"provisioner,omitempty"`
	PVCReclaimPolicy PVCReclaimPolicy `json:"reclaimPolicy,omitempty" yaml:"reclaimPolicy,omitempty"`
}

// NewStorageManagement creates new StorageManagement
func NewStorageManagement() *StorageManagement {
	return new(StorageManagement)
}

// MergeFrom merges from specified object
func (storageManagement *StorageManagement) MergeFrom(from *StorageManagement, _type MergeType) *StorageManagement {
	if from == nil {
		return storageManagement
	}

	if storageManagement == nil {
		storageManagement = &StorageManagement{}
	}

	switch _type {
	case MergeTypeFillEmptyValues:
		return storageManagement.mergeFromFillEmptyValues(from)
	case MergeTypeOverrideByNonEmptyValues:
		return storageManagement.mergeFromOverwriteByNonEmptyValues(from)
	}

	return storageManagement
}

// mergeFromFillEmptyValues fills empty values
func (storageManagement *StorageManagement) mergeFromFillEmptyValues(from *StorageManagement) *StorageManagement {
	if storageManagement.PVCProvisioner.IsUnspecified() {
		storageManagement.PVCProvisioner = from.PVCProvisioner
	}
	if storageManagement.PVCReclaimPolicy.IsUnspecified() {
		storageManagement.PVCReclaimPolicy = from.PVCReclaimPolicy
	}
	return storageManagement
}

// mergeFromOverwriteByNonEmptyValues overwrites by non-empty values
func (storageManagement *StorageManagement) mergeFromOverwriteByNonEmptyValues(from *StorageManagement) *StorageManagement {
	if from.PVCProvisioner.IsSpecified() {
		storageManagement.PVCProvisioner = from.PVCProvisioner
	}
	if from.PVCReclaimPolicy.IsSpecified() {
		storageManagement.PVCReclaimPolicy = from.PVCReclaimPolicy
	}
	return storageManagement
}
