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

import "strings"

// Configuration sections
// Each section renders into separate ConfigMap mapped into Pod as ClickHouse configuration file
var (
	SectionEmpty  SettingsSection = ""
	SectionCommon SettingsSection = "{common}"
	SectionUsers  SettingsSection = "{users}"
	SectionHost   SettingsSection = "{host}"
)

// SettingsSection specifies settings section
type SettingsSection string

// NewSettingsSectionFromString creates SettingsSection from a string
func NewSettingsSectionFromString(section string) SettingsSection {
	switch {
	case strings.EqualFold(section, SectionCommon.String()):
		return SectionCommon
	case strings.EqualFold(section, SectionUsers.String()):
		return SectionUsers
	case strings.EqualFold(section, SectionHost.String()):
		return SectionHost
	default:
		return SectionEmpty
	}
}

// In checks whether needle is in haystack
func (s SettingsSection) In(haystack []SettingsSection) bool {
	for _, item := range haystack {
		if item == s {
			return true
		}
	}
	return false
}

// String implements stringer
func (s SettingsSection) String() string {
	return string(s)
}

// Equal checks two SettingsSection for equality
func (s SettingsSection) Equal(another SettingsSection) bool {
	return s.String() == another.String()
}
