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

import "github.com/altinity/clickhouse-operator/pkg/apis/common/types"

// Defaults defines defaults section of .spec
type Defaults struct {
	ReplicasUseFQDN   *types.StringBool  `json:"replicasUseFQDN,omitempty"    yaml:"replicasUseFQDN,omitempty"`
	DistributedDDL    *DistributedDDL    `json:"distributedDDL,omitempty"     yaml:"distributedDDL,omitempty"`
	StorageManagement *StorageManagement `json:"storageManagement,omitempty"  yaml:"storageManagement,omitempty"`
	Templates         *TemplatesList     `json:"templates,omitempty"          yaml:"templates,omitempty"`
}

// NewDefaults creates new Defaults object
func NewDefaults() *Defaults {
	return new(Defaults)
}

func (d *Defaults) GetDistributedDDL() *DistributedDDL {
	if d == nil {
		return nil
	}
	return d.DistributedDDL
}

// MergeFrom merges from specified object
func (defaults *Defaults) MergeFrom(from *Defaults, _type MergeType) *Defaults {
	if from == nil {
		return defaults
	}

	if defaults == nil {
		defaults = NewDefaults()
	}

	switch _type {
	case MergeTypeFillEmptyValues:
		if !from.ReplicasUseFQDN.HasValue() {
			defaults.ReplicasUseFQDN = defaults.ReplicasUseFQDN.MergeFrom(from.ReplicasUseFQDN)
		}
	case MergeTypeOverrideByNonEmptyValues:
		if from.ReplicasUseFQDN.HasValue() {
			// Override by non-empty values only
			defaults.ReplicasUseFQDN = defaults.ReplicasUseFQDN.MergeFrom(from.ReplicasUseFQDN)
		}
	}

	defaults.DistributedDDL = defaults.DistributedDDL.MergeFrom(from.DistributedDDL, _type)
	defaults.StorageManagement = defaults.StorageManagement.MergeFrom(from.StorageManagement, _type)
	defaults.Templates = defaults.Templates.MergeFrom(from.Templates, _type)

	return defaults
}
