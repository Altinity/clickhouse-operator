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
	"fmt"
	"math"
	"reflect"
	"strconv"
)

// NewSettingScalar makes new scalar Setting
func NewSettingScalar(scalar string) *Setting {
	return &Setting{
		_type:  SettingTypeScalar,
		scalar: scalar,
	}
}

// NewSettingScalarFromAny makes new scalar Setting from untyped
func NewSettingScalarFromAny(untyped any) (*Setting, bool) {
	if scalar, ok := parseSettingScalarValue(untyped); ok {
		return NewSettingScalar(scalar), true
	}

	return nil, false
}

const (
	// Float with fractional part less than ignoreThreshold is considered to be int and is casted to int
	ignoreThreshold = 0.001
)

func parseSettingScalarValue(untyped any) (string, bool) {
	var scalarValue string
	var isKnownType bool

	typeOf := reflect.TypeOf(untyped)
	if typeOf == nil {
		// Unable to determine type of the value
		return "", false
	}

	switch untyped.(type) {
	case // scalar
		int, uint,
		int8, uint8,
		int16, uint16,
		int32, uint32,
		int64, uint64,
		bool,
		string:
		scalarValue = fmt.Sprintf("%v", untyped)
		isKnownType = true
	case // scalar
		float32:
		floatVal := untyped.(float32)
		// What is the fractional part of the float value?
		// If it is too small, we can consider the value to be an int value
		_, frac := math.Modf(float64(floatVal))
		if frac > ignoreThreshold {
			// Consider it float
			scalarValue = fmt.Sprintf("%f", untyped)
		} else {
			// Consider it int
			intVal := int64(floatVal)
			scalarValue = fmt.Sprintf("%v", intVal)
		}
		isKnownType = true
	case // scalar
		float64:
		floatVal := untyped.(float64)
		// What is the fractional part of the float value?
		// If it is too small, we can consider the value to be an int value
		_, frac := math.Modf(floatVal)
		if frac > ignoreThreshold {
			// Consider it float
			scalarValue = fmt.Sprintf("%f", untyped)
		} else {
			// Consider it int
			intVal := int64(floatVal)
			scalarValue = fmt.Sprintf("%v", intVal)
		}
		isKnownType = true
	}

	if isKnownType {
		return scalarValue, true
	}
	return "", false
}

// IsScalar checks whether setting is a scalar value
func (s *Setting) IsScalar() bool {
	return s.Type() == SettingTypeScalar
}

// ScalarString gets string scalar value of a setting
func (s *Setting) ScalarString() string {
	if s == nil {
		return ""
	}
	return s.scalar
}

// ScalarInt gets int scalar value of a setting
func (s *Setting) ScalarInt() int {
	if s == nil {
		return 0
	}
	if value, err := strconv.Atoi(s.scalar); err == nil {
		return value
	}

	return 0
}

// ScalarInt gets int scalar value of a setting
func (s *Setting) ScalarInt32Ptr() *Int32 {
	if s == nil {
		return nil
	}
	if value, err := strconv.Atoi(s.scalar); err == nil {
		return NewInt32(int32(value))
	}

	return nil
}

// scalarAsAny gets scalar value of a setting as any
func (s *Setting) scalarAsAny() any {
	if s == nil {
		return nil
	}

	return s.scalar
}
