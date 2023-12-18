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
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Settings value can be one of:
// 1. scalar value (string, int, bool, etc).
//		Ex.:
//			user1/networks/ip: "::/0"
// 2. vector of scalars
//		Ex.:
//			user1/networks/ip:
//				- "127.0.0.1"
//				- "192.168.1.2"
// We do not know types of these scalars in advance also

// Setting represents one settings, which can be either a sting or a vector of strings
type Setting struct {
	isScalar   bool
	scalar     string
	vector     []string
	attributes map[string]string
}

// Ensure required interface implementation
var _ yaml.Marshaler = &Setting{}

// NewSettingScalar makes new scalar Setting
func NewSettingScalar(scalar string) *Setting {
	return &Setting{
		isScalar: true,
		scalar:   scalar,
	}
}

// NewSettingScalarFromAny makes new scalar Setting from untyped
func NewSettingScalarFromAny(untyped any) (*Setting, bool) {
	if scalarValue, ok := parseScalar(untyped); ok {
		return NewSettingScalar(scalarValue), true
	}

	return nil, false
}

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

const (
	// Float with fractional part less than ignoreThreshold is considered to be int and is casted to int
	ignoreThreshold = 0.001
)

func parseScalar(untyped any) (string, bool) {
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
	if s == nil {
		return false
	}
	return s.isScalar
}

// IsVector checks whether setting is a vector value
func (s *Setting) IsVector() bool {
	if s == nil {
		return false
	}
	return !s.isScalar
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

// ScalarAny gets scalar value of a setting as any
func (s *Setting) ScalarAny() any {
	if s == nil {
		return nil
	}

	return s.scalar
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

// AsAny gets value of a setting as vector. ScalarString value is casted to vector
func (s *Setting) AsAny() any {
	if s == nil {
		return nil
	}
	if s.IsScalar() {
		return s.ScalarAny()
	}
	return s.VectorAny()
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

// SetAttribute sets attribute of the setting
func (s *Setting) SetAttribute(name, value string) *Setting {
	if s == nil {
		return nil
	}
	if s.attributes == nil {
		s.attributes = make(map[string]string)
	}
	s.attributes[name] = value
	return s
}

// HasAttribute checks whether setting has specified attribute
func (s *Setting) HasAttribute(name string) bool {
	if s == nil {
		return false
	}
	if s.attributes == nil {
		return false
	}
	_, ok := s.attributes[name]
	return ok
}

// HasAttributes checks whether setting has attributes
func (s *Setting) HasAttributes() bool {
	if s == nil {
		return false
	}
	return len(s.attributes) > 0
}

// Attributes returns string form of attributes - used to config tag creation
func (s *Setting) Attributes() string {
	if s == nil {
		return ""
	}
	a := ""
	for name, value := range s.attributes {
		a += fmt.Sprintf(` %s="%s"`, name, value)
	}
	return a
}

// Len returns number of entries in the Setting (be it scalar or vector)
func (s *Setting) Len() int {
	if s.IsVector() {
		return len(s.vector)
	}
	if s.IsScalar() {
		return 1
	}
	return 0
}

// MergeFrom merges from specified source
func (s *Setting) MergeFrom(from *Setting) *Setting {
	// Need to have something to merge from
	if from == nil {
		return s
	}

	// Can merge from Vector only
	from = from.CastToVector()

	// Reasonable to merge from non-zero vector only
	if from.Len() < 1 {
		return s
	}

	// In case recipient does not exist just copy values from source
	if s == nil {
		new := NewSettingVector(from.VectorString())
		new.attributes = util.MergeStringMapsPreserve(new.attributes, from.attributes)
		return new
	}

	s.vector = util.MergeStringArrays(s.vector, from.vector)
	s.attributes = util.MergeStringMapsPreserve(s.attributes, from.attributes)

	return s
}

// String gets string value of a setting. Vector is combined into one string
func (s *Setting) String() string {
	if s == nil {
		return ""
	}

	attributes := ""
	if s.HasAttributes() {
		attributes = ":[" + s.Attributes() + "]"
	}

	if s.isScalar {
		return s.scalar + attributes
	}

	return "[" + strings.Join(s.vector, ", ") + "]" + attributes
}

// MarshalYAML implements yaml.Marshaler interface
func (s *Setting) MarshalYAML() (interface{}, error) {
	return s.String(), nil
}
