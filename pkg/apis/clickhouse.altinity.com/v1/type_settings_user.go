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

// SettingsUser specifies settings of a user
type SettingsUser struct {
	*Settings
	username string
}

// NewSettings creates new settings
func NewSettingsUser(settings *Settings, username string) *SettingsUser {
	s := &SettingsUser{
		Settings: settings,
		username: username,
	}

	return s
}

// Name2Key converts name to storage key. This is the opposite to Key2Name
func (s *SettingsUser) Name2Key(name string) string {
	return name
}

// Key2Name converts storage key to name. This is the opposite to Name2Key
func (s *SettingsUser) Key2Name(key string) string {
	return key
}

// Username gets username
func (s *SettingsUser) Username() string {
	if s == nil {
		return ""
	}
	return s.username
}
