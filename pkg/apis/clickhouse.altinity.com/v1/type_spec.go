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

// ChiSpec defines spec section of ClickHouseInstallation resource
type ChiSpec struct {
	TaskID                 *types.String     `json:"taskID,omitempty"                 yaml:"taskID,omitempty"`
	Stop                   *types.StringBool `json:"stop,omitempty"                   yaml:"stop,omitempty"`
	Restart                *types.String     `json:"restart,omitempty"                yaml:"restart,omitempty"`
	Troubleshoot           *types.StringBool `json:"troubleshoot,omitempty"           yaml:"troubleshoot,omitempty"`
	NamespaceDomainPattern *types.String     `json:"namespaceDomainPattern,omitempty" yaml:"namespaceDomainPattern,omitempty"`
	Templating             *ChiTemplating    `json:"templating,omitempty"             yaml:"templating,omitempty"`
	Reconciling            *Reconciling      `json:"reconciling,omitempty"            yaml:"reconciling,omitempty"`
	Defaults               *Defaults         `json:"defaults,omitempty"               yaml:"defaults,omitempty"`
	Configuration          *Configuration    `json:"configuration,omitempty"          yaml:"configuration,omitempty"`
	Templates              *Templates        `json:"templates,omitempty"              yaml:"templates,omitempty"`
	UseTemplates           []*TemplateRef    `json:"useTemplates,omitempty"           yaml:"useTemplates,omitempty"`
}

// HasTaskID checks whether task id is specified
func (spec *ChiSpec) HasTaskID() bool {
	return len(spec.TaskID.Value()) > 0
}

// GetTaskID gets task id as a string
func (spec *ChiSpec) GetTaskID() string {
	return spec.TaskID.Value()
}

func (spec *ChiSpec) GetStop() *types.StringBool {
	return spec.Stop
}

func (spec *ChiSpec) GetRestart() *types.String {
	return spec.Restart
}

func (spec *ChiSpec) GetTroubleshoot() *types.StringBool {
	return spec.Troubleshoot
}

func (spec *ChiSpec) GetNamespaceDomainPattern() *types.String {
	return spec.NamespaceDomainPattern
}

func (spec *ChiSpec) GetTemplating() *ChiTemplating {
	return spec.Templating
}

func (spec *ChiSpec) GetDefaults() *Defaults {
	return spec.Defaults
}

func (spec *ChiSpec) GetConfiguration() IConfiguration {
	return spec.Configuration
}

func (spec *ChiSpec) GetTemplates() *Templates {
	return spec.Templates
}

// MergeFrom merges from spec
func (spec *ChiSpec) MergeFrom(from *ChiSpec, _type MergeType) {
	if from == nil {
		return
	}

	switch _type {
	case MergeTypeFillEmptyValues:
		if !spec.HasTaskID() {
			spec.TaskID = spec.TaskID.MergeFrom(from.TaskID)
		}
		if !spec.Stop.HasValue() {
			spec.Stop = spec.Stop.MergeFrom(from.Stop)
		}
		if !spec.Restart.HasValue() {
			spec.Restart = spec.Restart.MergeFrom(from.Restart)
		}
		if !spec.Troubleshoot.HasValue() {
			spec.Troubleshoot = spec.Troubleshoot.MergeFrom(from.Troubleshoot)
		}
		if !spec.NamespaceDomainPattern.HasValue() {
			spec.NamespaceDomainPattern = spec.NamespaceDomainPattern.MergeFrom(from.NamespaceDomainPattern)
		}
	case MergeTypeOverrideByNonEmptyValues:
		if from.HasTaskID() {
			spec.TaskID = spec.TaskID.MergeFrom(from.TaskID)
		}
		if from.Stop.HasValue() {
			// Override by non-empty values only
			spec.Stop = from.Stop
		}
		if from.Restart.HasValue() {
			// Override by non-empty values only
			spec.Restart = spec.Restart.MergeFrom(from.Restart)
		}
		if from.Troubleshoot.HasValue() {
			// Override by non-empty values only
			spec.Troubleshoot = from.Troubleshoot
		}
		if from.NamespaceDomainPattern.HasValue() {
			spec.NamespaceDomainPattern = spec.NamespaceDomainPattern.MergeFrom(from.NamespaceDomainPattern)
		}
	}

	spec.Templating = spec.Templating.MergeFrom(from.Templating, _type)
	spec.Reconciling = spec.Reconciling.MergeFrom(from.Reconciling, _type)
	spec.Defaults = spec.Defaults.MergeFrom(from.Defaults, _type)
	spec.Configuration = spec.Configuration.MergeFrom(from.Configuration, _type)
	spec.Templates = spec.Templates.MergeFrom(from.Templates, _type)
	// TODO may be it would be wiser to make more intelligent merge
	spec.UseTemplates = append(spec.UseTemplates, from.UseTemplates...)
}
