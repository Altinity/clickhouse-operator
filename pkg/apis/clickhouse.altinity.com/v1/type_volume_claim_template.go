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

import (
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// VolumeClaimTemplate defines PersistentVolumeClaim Template
type VolumeClaimTemplate struct {
	Name string `json:"name" yaml:"name"`
	StorageManagement
	ObjectMeta meta.ObjectMeta                `json:"metadata,omitempty"      yaml:"metadata,omitempty"`
	Spec       core.PersistentVolumeClaimSpec `json:"spec,omitempty"          yaml:"spec,omitempty"`
}

// PVCProvisioner defines PVC provisioner
type PVCProvisioner string

// Possible values of PVC provisioner
const (
	PVCProvisionerUnspecified PVCProvisioner = ""
	PVCProvisionerStatefulSet PVCProvisioner = "StatefulSet"
	PVCProvisionerOperator    PVCProvisioner = "Operator"
)

// NewPVCProvisionerFromString creates new PVCProvisioner from string
func NewPVCProvisionerFromString(s string) PVCProvisioner {
	return PVCProvisioner(s)
}

// IsValid checks whether PVCProvisioner is valid
func (v PVCProvisioner) IsValid() bool {
	switch v {
	case
		PVCProvisionerUnspecified,
		PVCProvisionerStatefulSet,
		PVCProvisionerOperator:
		return true
	}
	return false
}

// IsUnspecified checks whether PVCProvisioner is unspecified
func (v PVCProvisioner) IsUnspecified() bool {
	return v == PVCProvisionerUnspecified
}

// IsSpecified checks whether PVCProvisioner is specified
func (v PVCProvisioner) IsSpecified() bool {
	return v.IsValid() && !v.IsUnspecified()
}

// String returns string value for PVCProvisioner
func (v PVCProvisioner) String() string {
	return string(v)
}

// PVCReclaimPolicy defines PVC reclaim policy
type PVCReclaimPolicy string

// Possible values of PVC reclaim policy
const (
	PVCReclaimPolicyUnspecified PVCReclaimPolicy = ""
	PVCReclaimPolicyRetain      PVCReclaimPolicy = "Retain"
	PVCReclaimPolicyDelete      PVCReclaimPolicy = "Delete"
)

// NewPVCReclaimPolicyFromString creates new PVCReclaimPolicy from string
func NewPVCReclaimPolicyFromString(s string) PVCReclaimPolicy {
	return PVCReclaimPolicy(s)
}

// IsValid checks whether PVCReclaimPolicy is valid
func (v PVCReclaimPolicy) IsValid() bool {
	switch v {
	case
		PVCReclaimPolicyUnspecified,
		PVCReclaimPolicyRetain,
		PVCReclaimPolicyDelete:
		return true
	}
	return false
}

// IsUnspecified checks whether PVCReclaimPolicy is unspecified
func (v PVCReclaimPolicy) IsUnspecified() bool {
	return v == PVCReclaimPolicyUnspecified
}

// IsSpecified checks whether PVCReclaimPolicy is specified
func (v PVCReclaimPolicy) IsSpecified() bool {
	return v.IsValid() && !v.IsUnspecified()
}

// String returns string value for PVCReclaimPolicy
func (v PVCReclaimPolicy) String() string {
	return string(v)
}
