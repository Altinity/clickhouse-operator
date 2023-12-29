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
		_type:  SettingTypeVector,
		vector: vector,
	}
}

// NewSettingVectorFromAny makes new vector Setting from untyped
func NewSettingVectorFromAny(untyped any) (*Setting, bool) {
	if vector, ok := parseSettingVectorValue(untyped); ok {
		return NewSettingVector(vector), true
	}

	return nil, false
}

func parseSettingVectorValue(untyped any) ([]string, bool) {
	var vectorValue []string

	typeOf := reflect.TypeOf(untyped)
	if typeOf == nil {
		// Unable to determine type of the value
		return nil, false
	}

	if vector, ok := untyped.([]any); ok {
		for _, possibleScalar := range vector {
			if scalarValue, ok := parseSettingScalarValue(possibleScalar); ok {
				vectorValue = append(vectorValue, scalarValue)
			}
		}
		return vectorValue, true
	}

	return nil, false
}

// IsVector checks whether setting is a vector value
func (s *Setting) IsVector() bool {
	return s.Type() == SettingTypeVector
}

// VectorOfStrings gets vector values of a setting
func (s *Setting) VectorOfStrings() []string {
	if s == nil {
		return nil
	}
	return s.vector
}

// vectorAsAny gets vector value of a setting as any
func (s *Setting) vectorAsAny() any {
	if s == nil {
		return nil
	}

	return s.vector
}

// AsVectorOfStrings gets value of a setting as vector. ScalarString value is casted to vector
func (s *Setting) AsVectorOfStrings() []string {
	if s == nil {
		return nil
	}
	if s.IsScalar() {
		return []string{
			s.ScalarString(),
		}
	}
	return s.vector
}
