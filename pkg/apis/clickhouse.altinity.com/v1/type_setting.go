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
	switch {
	case s.IsVector():
		return len(s.vector)
	case s.IsScalar():
		return 1
	default:
		return 0
	}
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
