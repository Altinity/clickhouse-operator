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

	core "k8s.io/api/core/v1"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

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
