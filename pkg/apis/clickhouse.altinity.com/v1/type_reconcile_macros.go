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
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

type ReconcileMacros struct {
	Sections MacrosSections `json:"sections,omitempty" yaml:"sections,omitempty"`
}

// MergeFrom merges from specified reconciling
func (t ReconcileMacros) MergeFrom(from ReconcileMacros, _type MergeType) ReconcileMacros {
	t.Sections = t.Sections.MergeFrom(from.Sections, _type)
	return t
}

type MacrosSections struct {
	Users    MacrosSection `json:"users,omitempty"    yaml:"users,omitempty"`
	Profiles MacrosSection `json:"profiles,omitempty" yaml:"profiles,omitempty"`
	Quotas   MacrosSection `json:"quotas,omitempty"   yaml:"quotas,omitempty"`
	Settings MacrosSection `json:"settings,omitempty" yaml:"settings,omitempty"`
	Files    MacrosSection `json:"files,omitempty"    yaml:"files,omitempty"`
}

// MergeFrom merges from specified reconciling
func (t MacrosSections) MergeFrom(from MacrosSections, _type MergeType) MacrosSections {
	t.Users = t.Users.MergeFrom(from.Users, _type)
	t.Profiles = t.Profiles.MergeFrom(from.Profiles, _type)
	t.Quotas = t.Quotas.MergeFrom(from.Quotas, _type)
	t.Settings = t.Settings.MergeFrom(from.Settings, _type)
	t.Files = t.Files.MergeFrom(from.Files, _type)
	return t
}

type MacrosSection struct {
	Enabled *types.StringBool `json:"enabled,omitempty"    yaml:"enabled,omitempty"`
}

// MergeFrom merges from specified reconciling
func (t MacrosSection) MergeFrom(from MacrosSection, _type MergeType) MacrosSection {
	t.Enabled = t.Enabled.MergeFrom(from.Enabled)
	return t
}
