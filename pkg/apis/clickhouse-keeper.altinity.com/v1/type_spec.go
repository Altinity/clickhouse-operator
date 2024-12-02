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
	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// ChkSpec defines spec section of ClickHouseKeeper resource
type ChkSpec struct {
	TaskID                 *types.String       `json:"taskID,omitempty"                 yaml:"taskID,omitempty"`
	NamespaceDomainPattern *types.String       `json:"namespaceDomainPattern,omitempty" yaml:"namespaceDomainPattern,omitempty"`
	Reconciling            *apiChi.Reconciling `json:"reconciling,omitempty"            yaml:"reconciling,omitempty"`
	Defaults               *apiChi.Defaults    `json:"defaults,omitempty"               yaml:"defaults,omitempty"`
	Configuration          *Configuration      `json:"configuration,omitempty"          yaml:"configuration,omitempty"`
	Templates              *apiChi.Templates   `json:"templates,omitempty"              yaml:"templates,omitempty"`
}

// HasTaskID checks whether task id is specified
func (spec *ChkSpec) HasTaskID() bool {
	if spec == nil {
		return false
	}
	return len(spec.TaskID.Value()) > 0
}

// GetTaskID gets task id as a string
func (spec *ChkSpec) GetTaskID() string {
	if spec == nil {
		return ""
	}
	return spec.TaskID.Value()
}

func (spec *ChkSpec) GetNamespaceDomainPattern() *types.String {
	if spec == nil {
		return (*types.String)(nil)
	}
	return spec.NamespaceDomainPattern
}

func (spec *ChkSpec) GetDefaults() *apiChi.Defaults {
	if spec == nil {
		return (*apiChi.Defaults)(nil)
	}
	return spec.Defaults
}

func (spec *ChkSpec) GetConfiguration() apiChi.IConfiguration {
	if spec == nil {
		return (*Configuration)(nil)
	}
	return spec.Configuration
}

func (spec *ChkSpec) GetTemplates() *apiChi.Templates {
	if spec == nil {
		return (*apiChi.Templates)(nil)
	}
	return spec.Templates
}

// MergeFrom merges from spec
func (spec *ChkSpec) MergeFrom(from *ChkSpec, _type apiChi.MergeType) {
	if from == nil {
		return
	}

	if spec == nil {
		spec = &ChkSpec{}
	}

	switch _type {
	case apiChi.MergeTypeFillEmptyValues:
		if !spec.HasTaskID() {
			spec.TaskID = spec.TaskID.MergeFrom(from.TaskID)
		}
		if !spec.NamespaceDomainPattern.HasValue() {
			spec.NamespaceDomainPattern = spec.NamespaceDomainPattern.MergeFrom(from.NamespaceDomainPattern)
		}
	case apiChi.MergeTypeOverrideByNonEmptyValues:
		if from.HasTaskID() {
			spec.TaskID = spec.TaskID.MergeFrom(from.TaskID)
		}
		if from.NamespaceDomainPattern.HasValue() {
			spec.NamespaceDomainPattern = spec.NamespaceDomainPattern.MergeFrom(from.NamespaceDomainPattern)
		}
	}

	spec.Reconciling = spec.Reconciling.MergeFrom(from.Reconciling, _type)
	spec.Defaults = spec.Defaults.MergeFrom(from.Defaults, _type)
	spec.Configuration = spec.Configuration.MergeFrom(from.Configuration, _type)
	spec.Templates = spec.Templates.MergeFrom(from.Templates, _type)
}
