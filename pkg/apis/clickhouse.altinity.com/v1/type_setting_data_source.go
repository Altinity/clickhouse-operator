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
	"encoding/json"
	"strings"

	core "k8s.io/api/core/v1"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// Hot-reload user authentication fields. The Secret key is the credential value.
const (
	hotReloadFieldPassword           = "password"
	hotReloadFieldPasswordSHA256     = "password_sha256_hex"
	hotReloadFieldPasswordDoubleSHA1 = "password_double_sha1_hex"
)

// IsHotReloadUserAuthField reports whether name is a user authentication field
// that may opt into hotReload. name is either the field itself or username/field.
func IsHotReloadUserAuthField(name string) bool {
	field := name
	if i := strings.LastIndex(name, "/"); i >= 0 {
		if strings.Contains(name[:i], "/") {
			return false
		}
		field = name[i+1:]
	}
	switch field {
	case hotReloadFieldPassword, hotReloadFieldPasswordSHA256, hotReloadFieldPasswordDoubleSHA1:
		return true
	default:
		return false
	}
}

// WithoutHotReloadUsers returns settings for chop-generated-users.xml.
// A user that opts a password into hotReload is omitted entirely, including
// non-password fields, so that user is published in one file. The receiver is
// not modified. When no user opts in, the receiver is returned.
func (s *Settings) WithoutHotReloadUsers() *Settings {
	return s.partitionHotReloadUsers(false)
}

// OnlyHotReloadUsers returns users that opt a password into hotReload, including
// their other fields. The receiver is not modified.
func (s *Settings) OnlyHotReloadUsers() *Settings {
	return s.partitionHotReloadUsers(true)
}

func (s *Settings) partitionHotReloadUsers(hotReloadOnly bool) *Settings {
	if s == nil {
		return nil
	}
	hot := map[string]struct{}{}
	for _, name := range s.HotReloadUsernames() {
		hot[name] = struct{}{}
	}
	if len(hot) == 0 {
		if hotReloadOnly {
			return NewSettings()
		}
		return s
	}
	dst := NewSettings()
	if s.HasConverter() {
		dst.SetConverter(s.GetConverter())
	}
	s.WalkKeys(func(key string, setting *Setting) {
		_, isHot := hot[usernameOf(s.Key2Name(key))]
		if isHot == hotReloadOnly {
			dst.SetKey(key, setting.Clone())
		}
	})
	return dst
}

// HotReloadUsernames returns users that opt a password field into hotReload.
// The receiver is not modified.
func (s *Settings) HotReloadUsernames() []string {
	if s == nil {
		return nil
	}
	seen := map[string]struct{}{}
	var names []string
	s.Walk(func(name string, setting *Setting) {
		if !setting.IsHotReload() || !IsHotReloadUserAuthField(name) {
			return
		}
		username := usernameOf(name)
		if _, ok := seen[username]; ok {
			return
		}
		seen[username] = struct{}{}
		names = append(names, username)
	})
	return names
}

func usernameOf(name string) string {
	if i := strings.Index(name, "/"); i >= 0 {
		return name[:i]
	}
	return name
}

// SettingSource defines setting as a ref to some data source
type SettingSource struct {
	ValueFrom *types.DataSource `json:"valueFrom,omitempty" yaml:"valueFrom,omitempty"`
}

// NewSettingSource makes new source Setting
func NewSettingSource(src *SettingSource) *Setting {
	return &Setting{
		_type: SettingTypeSource,
		src:   src,
	}
}

// NewSettingSourceFromAny makes new source Setting from untyped
func NewSettingSourceFromAny(untyped any) (*Setting, bool) {
	if srcValue, ok := parseSettingSourceValue(untyped); ok {
		return NewSettingSource(srcValue), true
	}

	return nil, false
}

// GetNameKey gets name and key from the secret ref
// 1. The name of the secret to select from. Namespace is expected to be provided externally
// 2. The key of the secret to select from.
func (s *SettingSource) GetNameKey() (string, string) {
	if ref := s.GetSecretKeyRef(); ref != nil {
		return ref.Name, ref.Key
	}
	return "", ""
}

// GetSecretKeyRef gets SecretKeySelector (typically named as SecretKeyRef) or nil
func (s *SettingSource) GetSecretKeyRef() *core.SecretKeySelector {
	if s == nil {
		return nil
	}
	if s.ValueFrom == nil {
		return nil
	}
	return s.ValueFrom.SecretKeyRef
}

// HasSecretKeyRef checks whether SecretKeySelector (typically named as SecretKeyRef) is available
func (s *SettingSource) HasSecretKeyRef() bool {
	return s.GetSecretKeyRef() != nil
}

// HasValue checks whether SettingSource has no value
func (s *SettingSource) HasValue() bool {
	if s == nil {
		return false
	}
	if s.ValueFrom == nil {
		return false
	}
	return s.HasSecretKeyRef()
}

// sourceAsAny gets source value of a setting as any
func (s *Setting) sourceAsAny() any {
	if s == nil {
		return nil
	}

	return s.src
}

// IsSource checks whether setting is a source value
func (s *Setting) IsSource() bool {
	return s.Type() == SettingTypeSource
}

// GetNameKey gets name and key of source setting
// 1. The name of the secret to select from. Namespace is expected to be provided externally
// 2. The key of the secret to select from.
func (s *Setting) GetNameKey() (string, string) {
	if ref := s.GetSecretKeyRef(); ref != nil {
		return ref.Name, ref.Key
	}
	return "", ""
}

// GetSecretKeyRef gets SecretKeySelector (typically named as SecretKeyRef) or nil
func (s *Setting) GetSecretKeyRef() *core.SecretKeySelector {
	if s == nil {
		return nil
	}
	if !s.IsSource() {
		return nil
	}

	return s.src.GetSecretKeyRef()
}

// HasSecretKeyRef checks whether SecretKeySelector (typically named as SecretKeyRef) is available
func (s *Setting) HasSecretKeyRef() bool {
	if s == nil {
		return false
	}
	if !s.IsSource() {
		return false
	}

	return s.GetSecretKeyRef() != nil
}

// IsHotReload reports whether this setting asks for a Secret-backed password
// to be applied without restarting ClickHouse.
func (s *Setting) IsHotReload() bool {
	if s == nil || !s.IsSource() || s.src == nil {
		return false
	}
	return s.src.ValueFrom.IsHotReload()
}

func parseSettingSourceValue(untyped any) (*SettingSource, bool) {
	jsonStr, err := json.Marshal(untyped)
	if err != nil {
		return nil, false
	}

	// Convert json string to struct
	var settingSource SettingSource
	if err := json.Unmarshal(jsonStr, &settingSource); err != nil {
		return nil, false
	}

	return &settingSource, true
}
