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

// NewChiDefaults creates new ChiDefaults object
func NewChiDefaults() *ChiDefaults {
	return new(ChiDefaults)
}

// MergeFrom merges from specified object
func (defaults *ChiDefaults) MergeFrom(from *ChiDefaults, _type MergeType) *ChiDefaults {
	if from == nil {
		return defaults
	}

	if defaults == nil {
		defaults = NewChiDefaults()
	}

	switch _type {
	case MergeTypeFillEmptyValues:
		if from.ReplicasUseFQDN == "" {
			defaults.ReplicasUseFQDN = from.ReplicasUseFQDN
		}
	case MergeTypeOverrideByNonEmptyValues:
		if from.ReplicasUseFQDN != "" {
			// Override by non-empty values only
			defaults.ReplicasUseFQDN = from.ReplicasUseFQDN
		}
	}

	defaults.DistributedDDL = defaults.DistributedDDL.MergeFrom(from.DistributedDDL, _type)
	defaults.Templates = defaults.Templates.MergeFrom(from.Templates, _type)

	return defaults
}
