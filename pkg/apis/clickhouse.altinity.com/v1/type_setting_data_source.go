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
	"reflect"

	core "k8s.io/api/core/v1"
)

// SettingSource defines setting as a ref to some data source
type SettingSource struct {
	ValueFrom *DataSource `json:"valueFrom,omitempty" yaml:"valueFrom,omitempty"`
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

func parseSettingSourceValue(untyped any) (*SettingSource, bool) {
	typeOf := reflect.TypeOf(untyped)
	if typeOf == nil {
		// Unable to determine type of the value
		return nil, false
	}

	switch untyped.(type) {
	case SettingSource:
		src := untyped.(SettingSource)
		return &src, true
	case *SettingSource:
		src := untyped.(*SettingSource)
		return src, true
	}

	return nil, false
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
