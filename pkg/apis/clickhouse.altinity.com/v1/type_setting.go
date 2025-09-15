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
	"fmt"
	"strings"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
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
	_type      SettingType
	scalar     string
	vector     []string
	src        *SettingSource
	attributes map[string]string
	embed      bool
}

type SettingType string

// List of possible setting types
const (
	SettingTypeUnknown SettingType = "unknown"
	SettingTypeScalar  SettingType = "scalar"
	SettingTypeVector  SettingType = "vector"
	SettingTypeSource  SettingType = "source"
)

// Ensure required interface implementation
var (
	// unimplemented
	//_ yaml.Marshaler = &Setting{}
	//_ yaml.Unmarshaler = &Setting{}
	_ json.Marshaler   = &Setting{}
	_ json.Unmarshaler = &Setting{}
)

// List of returned errors
var (
	ErrDataSourceAddressHasIncorrectFormat = fmt.Errorf("data source address has incorrect format")
	ErrUnableToUnmarshal                   = fmt.Errorf("unable to unmarshal setting")
	ErrUnableToUnmarshalIntoNil            = fmt.Errorf("unable to unmarshal into nil")
)

// NewSettingFromAny builds new setting from either of:
// 1. scalar
// 2. vector
// 3. source
// In case of being unable to build new setting an error is returned
func NewSettingFromAny(untyped any) (*Setting, error) {
	if scalarSetting, ok := NewSettingScalarFromAny(untyped); ok && scalarSetting.HasValue() {
		return scalarSetting, nil
	}

	if vectorSetting, ok := NewSettingVectorFromAny(untyped); ok && vectorSetting.HasValue() {
		return vectorSetting, nil
	}

	if srcSetting, ok := NewSettingSourceFromAny(untyped); ok && srcSetting.HasValue() {
		return srcSetting, nil
	}

	return nil, ErrUnableToUnmarshal
}

// IsEmpty checks whether settings ia an empty one
func (s *Setting) IsEmpty() bool {
	return s == nil
}

// AsAny gets value of a setting as untyped
func (s *Setting) AsAny() any {
	if s == nil {
		return nil
	}
	switch s.Type() {
	case SettingTypeScalar:
		return s.scalarAsAny()
	case SettingTypeVector:
		return s.vectorAsAny()
	case SettingTypeSource:
		return s.sourceAsAny()
	}
	return nil
}

// Type gets type of the setting
func (s *Setting) Type() SettingType {
	if s == nil {
		return SettingTypeUnknown
	}
	return s._type
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

// Len returns number of entries in the Setting (be it a scalar or a vector)
func (s *Setting) Len() int {
	switch s.Type() {
	case SettingTypeScalar:
		return 1
	case SettingTypeVector:
		return len(s.vector)
	case SettingTypeSource:
		return 1
	default:
		return 0
	}
}

// HasValue checks whether setting has non zero-value (some value)
func (s *Setting) HasValue() bool {
	switch s.Type() {
	case SettingTypeScalar:
		return s.Len() > 0
	case SettingTypeVector:
		return s.Len() > 0
	case SettingTypeSource:
		return s.src.HasValue()
	default:
		return false
	}
}

// ApplyMacros applies macros on the Setting
func (s *Setting) ApplyMacros(macros *util.Replacer) {
	if s == nil {
		return
	}

	switch s.Type() {
	case SettingTypeScalar:
		s.scalar = macros.Line(s.scalar)
	case SettingTypeVector:
		s.vector = macros.Slice(s.vector)
	case SettingTypeSource:
		// Unimplemented
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

	// Reasonable to merge values only
	if !from.HasValue() {
		return s
	}

	// In case recipient does not exist just copy values from source
	if s == nil {
		new := NewSettingVector(from.VectorOfStrings())
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

	switch s.Type() {
	case SettingTypeScalar:
		return s.ScalarString()
	case SettingTypeVector:
		return "[" + strings.Join(s.vector, ", ") + "]"
	case SettingTypeSource:
		return "data source"
	}

	return ""
}

// String gets string value of a setting. Vector is combined into one string
func (s *Setting) StringFull() string {
	if s == nil {
		return ""
	}

	attributes := ""
	if s.HasAttributes() {
		attributes = ":[" + s.Attributes() + "]"
	}

	return s.String() + attributes
}

// CastToVector returns either Setting in case it is vector or newly created Setting with value casted to VectorOfStrings
func (s *Setting) CastToVector() *Setting {
	if s == nil {
		return nil
	}
	switch s.Type() {
	case SettingTypeScalar:
		return NewSettingVector(s.AsVectorOfStrings())
	}
	return s
}

// FetchDataSourceAddress fetches data source address from the setting.
// defaultNamespace specifies default namespace to be used in case there is no namespace specified in data source address.
func (s *Setting) FetchDataSourceAddress(defaultNamespace string, parseScalarString bool) (types.ObjectAddress, error) {
	switch s.Type() {
	case SettingTypeScalar:
		if parseScalarString {
			// Fetch k8s address of the field from the string
			return s.parseDataSourceAddress(s.String(), defaultNamespace)
		}
	case SettingTypeSource:
		// Fetch k8s address of the field from the source ref
		// 1. The name of the secret to select from. Namespace is expected to be provided externally
		// 2. The key of the secret to select from.
		name, key := s.GetNameKey()
		return types.ObjectAddress{
			Namespace: defaultNamespace,
			Name:      name,
			Key:       key,
		}, nil
	}

	return types.ObjectAddress{}, fmt.Errorf("%w - unknown setting type", ErrDataSourceAddressHasIncorrectFormat)
}

// parseDataSourceAddress parses address into namespace, name, key triple
func (s *Setting) parseDataSourceAddress(dataSourceAddress, defaultNamespace string) (addr types.ObjectAddress, err error) {
	// Extract data source's namespace and name and then field name within the data source,
	// by splitting namespace/name/field (aka key) triple. Namespace can be omitted though
	switch tags := strings.Split(dataSourceAddress, "/"); len(tags) {
	case 3:
		// All components are in place. Expect to have namespace/name/key triple
		addr.Namespace = tags[0]
		addr.Name = tags[1]
		addr.Key = tags[2]
	case 2:
		// Assume namespace is omitted. Expect to have name/key pair
		addr.Namespace = defaultNamespace
		addr.Name = tags[0]
		addr.Key = tags[1]
	default:
		// Skip incorrect entry
		return types.ObjectAddress{}, fmt.Errorf("%w, dataSourceAddress: %s", ErrDataSourceAddressHasIncorrectFormat, dataSourceAddress)
	}

	// Sanity check for all address components being in place
	if addr.AnyEmpty() {
		return types.ObjectAddress{}, fmt.Errorf(
			"%w, %s/%s/%s",
			ErrDataSourceAddressHasIncorrectFormat,
			addr.Namespace, addr.Name, addr.Key,
		)
	}

	return addr, nil
}

func (s *Setting) SetEmbed() *Setting {
	if s == nil {
		return nil
	}
	s.embed = true
	return s
}

func (s *Setting) IsEmbed() bool {
	if s == nil {
		return false
	}
	return s.embed
}

// UnmarshalJSON unmarshal JSON
func (s *Setting) UnmarshalJSON(data []byte) error {
	return s.Unmarshal(data, json.Unmarshal)
}

// MarshalJSON marshals JSON
func (s *Setting) MarshalJSON() ([]byte, error) {
	return s.marshal(json.Marshal)
}

//// UnmarshalYAML unmarshal YAML
//func (s *Setting) UnmarshalYAML(data []byte) error {
//	return s.Unmarshal(data, yaml.Unmarshal)
//}
//
//// MarshalYAML marshals YAML
//func (s *Setting) MarshalYAML() ([]byte, error) {
//	return s.marshal(yaml.Marshal)
//}

// Unmarshal
func (s *Setting) Unmarshal(data []byte, unmarshaller func(data []byte, v any) error) error {
	if s == nil {
		return ErrUnableToUnmarshalIntoNil
	}

	// Prepare untyped map at first
	var untyped any

	// Provided binary data is expected to unmarshal into untyped map, because settings are map-like struct
	if err := unmarshaller(data, &untyped); err != nil {
		return err
	}

	// Build setting from untyped var
	setting, err := NewSettingFromAny(untyped)
	if err != nil {
		return err
	}

	// Copy typed value
	*s = *setting
	return nil
}

// marshal
func (s *Setting) marshal(marshaller func(v any) ([]byte, error)) ([]byte, error) {
	return marshaller(s.AsAny())
}

// Clone make clone of a setting
func (s *Setting) Clone() *Setting {
	if s == nil {
		return nil
	}

	// Build json bytes
	bytes, err := s.MarshalJSON()
	if err != nil {
		return nil
	}

	// Fill clone with unmarshalled value
	clone := &Setting{}
	err = clone.UnmarshalJSON(bytes)
	if err != nil {
		return nil
	}

	// Clone is ready
	return clone
}
