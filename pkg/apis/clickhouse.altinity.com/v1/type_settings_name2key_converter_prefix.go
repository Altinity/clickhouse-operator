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

// SettingsName2KeyConverterPrefix implements prefix name to key conversion
type SettingsName2KeyConverterPrefix struct {
	prefix    string
	separator string
}

// NewSettingsName2KeyConverterPrefix is a constructor
func NewSettingsName2KeyConverterPrefix(prefix string, separators ...string) SettingsName2KeyConverterPrefix {
	// Default separator is "/"
	separator := "/"
	if len(separators) > 0 {
		separator = separators[0]
	}
	return SettingsName2KeyConverterPrefix{
		prefix:    prefix,
		separator: separator,
	}
}

// Name2Key converts name to storage key. This is the opposite to Key2Name
func (s SettingsName2KeyConverterPrefix) Name2Key(name string) string {
	return s.Prefix() + name
}

// Key2Name converts storage key to name. This is the opposite to Name2Key
func (s SettingsName2KeyConverterPrefix) Key2Name(key string) string {
	return strings.TrimPrefix(key, s.Prefix())
}

// Prefix gets full prefix
func (s SettingsName2KeyConverterPrefix) Prefix() string {
	return s.prefix + s.separator
}

// DeepCopySettingsName2KeyConverter is required for code auto-generator
func (s SettingsName2KeyConverterPrefix) DeepCopySettingsName2KeyConverter() SettingsName2KeyConverter {
	return s
}
