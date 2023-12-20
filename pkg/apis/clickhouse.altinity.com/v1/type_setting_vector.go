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
)

// NewSettingVector makes new vector Setting
func NewSettingVector(vector []string) *Setting {
	return &Setting{
		isScalar: false,
		vector:   vector,
	}
}

// NewSettingVectorFromAny makes new vector Setting from untyped
func NewSettingVectorFromAny(untyped any) (*Setting, bool) {
	var vectorValue []string
	var isKnownType bool

	typeOf := reflect.TypeOf(untyped)
	if typeOf == nil {
		// Unable to determine type of the value
		return nil, false
	}

	switch untyped.(type) {
	case // vector
		[]interface{}:
		for _, _untyped := range untyped.([]interface{}) {
			if scalarValue, ok := parseScalar(_untyped); ok {
				vectorValue = append(vectorValue, scalarValue)
			}
		}
		isKnownType = true
	}

	if isKnownType {
		return NewSettingVector(vectorValue), true
	}
	return nil, false
}

// IsVector checks whether setting is a vector value
func (s *Setting) IsVector() bool {
	if s == nil {
		return false
	}
	return !s.isScalar
}

// VectorString gets vector values of a setting
func (s *Setting) VectorString() []string {
	if s == nil {
		return nil
	}
	return s.vector
}

// VectorAny gets vector value of a setting as any
func (s *Setting) VectorAny() any {
	if s == nil {
		return nil
	}

	return s.vector
}

// AsVectorString gets value of a setting as vector. ScalarString value is casted to vector
func (s *Setting) AsVectorString() []string {
	if s == nil {
		return nil
	}
	if s.isScalar {
		return []string{
			s.scalar,
		}
	}
	return s.vector
}

// CastToVector returns either Setting in case it is vector or newly created Setting with value casted to VectorString
func (s *Setting) CastToVector() *Setting {
	if s == nil {
		return nil
	}
	if s.isScalar {
		return NewSettingVector(s.AsVectorString())
	}
	return s
}
