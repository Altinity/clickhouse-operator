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

import "strings"

// StringBool defines string representation of a bool type
type StringBool string

// Set of string boolean constants
const (
	StringBool0                    = "0"
	StringBool1                    = "1"
	StringBoolFalseFirstCapital    = "False"
	StringBoolFalseLowercase       = "false"
	StringBoolTrueFirstCapital     = "True"
	StringBoolTrueLowercase        = "true"
	StringBoolNoFirstCapital       = "No"
	StringBoolNoLowercase          = "no"
	StringBoolYesFirstCapital      = "Yes"
	StringBoolYesLowercase         = "yes"
	StringBoolOffFirstCapital      = "Off"
	StringBoolOffLowercase         = "off"
	StringBoolOnFirstCapital       = "On"
	StringBoolOnLowercase          = "on"
	StringBoolDisableFirstCapital  = "Disable"
	StringBoolDisableLowercase     = "disable"
	StringBoolEnableFirstCapital   = "Enable"
	StringBoolEnableLowercase      = "enable"
	StringBoolDisabledFirstCapital = "Disabled"
	StringBoolDisabledLowercase    = "disabled"
	StringBoolEnabledFirstCapital  = "Enabled"
	StringBoolEnabledLowercase     = "enabled"
)

// From casts bool to StringBool
func (s StringBool) From(value bool) StringBool {
	if value {
		return StringBoolTrueFirstCapital
	}

	return StringBoolFalseFirstCapital
}

// String casts StringBool to string
func (s StringBool) String() string {
	return string(s)
}

// Value gets bool value
func (s StringBool) Value() bool {
	if s.IsTrue() {
		return true
	}

	if s.IsFalse() {
		return false
	}

	// Default
	return false
}

// IsValid checks whether StringBool has a proper value
func (s StringBool) IsValid() bool {
	switch strings.ToLower(s.String()) {
	case
		StringBool0,
		StringBool1,

		StringBoolFalseLowercase,
		StringBoolTrueLowercase,

		StringBoolNoLowercase,
		StringBoolYesLowercase,

		StringBoolOffLowercase,
		StringBoolOnLowercase,

		StringBoolDisableLowercase,
		StringBoolEnableLowercase,

		StringBoolDisabledLowercase,
		StringBoolEnabledLowercase:
		return true

	default:
		return false
	}
}

// IsFalse checks whether str is a string as bool "false" value
func (s StringBool) IsFalse() bool {
	switch strings.ToLower(s.String()) {
	case
		StringBool0,
		StringBoolFalseLowercase,
		StringBoolNoLowercase,
		StringBoolOffLowercase,
		StringBoolDisableLowercase,
		StringBoolDisabledLowercase:
		return true

	default:
		return false
	}
}

// IsTrue checks whether str is a string as bool "true" value
func (s StringBool) IsTrue() bool {
	switch strings.ToLower(s.String()) {
	case
		StringBool1,
		StringBoolTrueLowercase,
		StringBoolYesLowercase,
		StringBoolOnLowercase,
		StringBoolEnableLowercase,
		StringBoolEnabledLowercase:
		return true

	default:
		return false
	}
}

// CastTo01 casts string-bool into string "0/1"
func (s StringBool) CastTo01(defaultValue bool) string {
	// True and False string values
	_true := StringBool1
	_false := StringBool0

	if s.IsTrue() {
		return _true
	}
	if s.IsFalse() {
		return _false
	}

	// String value unrecognized, return default value

	if defaultValue {
		return _true
	}

	return _false
}

// CastToStringTrueFalse casts string-bool into string "true/false"
func (s StringBool) CastToStringTrueFalse(defaultValue bool) string {
	// True and False values
	_true := StringBoolTrueLowercase
	_false := StringBoolFalseLowercase

	if s.IsTrue() {
		return _true
	}
	if s.IsFalse() {
		return _false
	}

	// String value unrecognized, return default value

	if defaultValue {
		return _true
	}

	return _false
}

// Normalize normalizes
func (s StringBool) Normalize(defaultValue bool) StringBool {
	// True and False values
	_true := StringBoolTrueLowercase
	_false := StringBoolFalseLowercase

	if s.IsValid() {
		return s
	}

	// String value unrecognized, return default value

	if defaultValue {
		return StringBool(_true)
	}

	return StringBool(_false)
}
