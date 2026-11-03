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
	TaskID                 *types.Id               `json:"taskID,omitempty"                 yaml:"taskID,omitempty"`
	Stop                   *types.StringBool       `json:"stop,omitempty"                   yaml:"stop,omitempty"`
	NamespaceDomainPattern *types.String           `json:"namespaceDomainPattern,omitempty" yaml:"namespaceDomainPattern,omitempty"`
	Suspend                *types.StringBool       `json:"suspend,omitempty"                yaml:"suspend,omitempty"`
	Reconciling            *apiChi.ChiReconcile    `json:"reconciling,omitempty"            yaml:"reconciling,omitempty"`
	Reconcile              *apiChi.ChiReconcile    `json:"reconcile,omitempty"              yaml:"reconcile,omitempty"`
	Defaults               *apiChi.Defaults        `json:"defaults,omitempty"               yaml:"defaults,omitempty"`
	Configuration          *Configuration          `json:"configuration,omitempty"          yaml:"configuration,omitempty"`
	Templates              *apiChi.Templates       `json:"templates,omitempty"              yaml:"templates,omitempty"`
	Security               *apiChi.ClusterSecurity `json:"security,omitempty"            yaml:"security,omitempty"`
}

// HasTaskID checks whether task id is specified
func (spec *ChkSpec) HasTaskID() bool {
	if spec == nil {
		return false
	}
	return spec.TaskID.HasValue()
}

// GetTaskID gets task id as a string
func (spec *ChkSpec) GetTaskID() *types.Id {
	if spec == nil {
		return nil
	}
	return spec.TaskID
}

func (spec *ChkSpec) GetStop() *types.StringBool {
	if spec == nil {
		return (*types.StringBool)(nil)
	}
	return spec.Stop
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

// GetSecurity returns the spec-level Security block, nil-safe.
func (spec *ChkSpec) GetSecurity() *apiChi.ClusterSecurity {
	if spec == nil {
		return nil
	}
	return spec.Security
}

// MergeFrom merges from spec
func (spec *ChkSpec) MergeFrom(from *ChkSpec, _type apiChi.MergeType) {
	if from == nil {
		return
	}

	if spec == nil {
		spec = &ChkSpec{}
	}

	spec.TaskID = apiChi.MergeScalar(spec.TaskID, from.TaskID, _type)
	spec.Stop = apiChi.MergeScalar(spec.Stop, from.Stop, _type)
	spec.NamespaceDomainPattern = apiChi.MergeScalar(spec.NamespaceDomainPattern, from.NamespaceDomainPattern, _type)
	spec.Suspend = apiChi.MergeScalar(spec.Suspend, from.Suspend, _type)

	spec.Reconcile = spec.Reconcile.MergeFrom(from.Reconcile, _type)
	spec.Defaults = spec.Defaults.MergeFrom(from.Defaults, _type)
	spec.Configuration = spec.Configuration.MergeFrom(from.Configuration, _type)
	spec.Templates = spec.Templates.MergeFrom(from.Templates, _type)

	if fromSecurity := from.GetSecurity(); fromSecurity != nil {
		if spec.Security == nil {
			spec.Security = &apiChi.ClusterSecurity{}
		}
		spec.Security.ClickHouse = spec.Security.ClickHouse.MergeFrom(fromSecurity.GetClickHouse(), _type)
		spec.Security.Zookeeper = spec.Security.Zookeeper.MergeFrom(fromSecurity.GetZookeeper(), _type)
	}
}
