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

package util

import (
	// #nosec
	// G505 (CWE-327): Blocklisted import crypto/sha1: weak cryptographic primitive
	// It is good enough for string ID
	"crypto/sha1"
	"encoding/hex"
	"strings"
)

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

// IsStringBool checks whether str is a string as bool value
func IsStringBool(str string) bool {
	switch strings.ToLower(str) {
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

// IsStringBoolFalse checks whether str is a string as bool "false" value
func IsStringBoolFalse(str string) bool {
	switch strings.ToLower(str) {
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

// IsStringBoolTrue checks whether str is a string as bool "true" value
func IsStringBoolTrue(str string) bool {
	switch strings.ToLower(str) {
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

// CastStringBoolTo01 casts string-bool into string "0/1"
func CastStringBoolTo01(str string, defaultValue bool) string {
	// True and False string values
	_true := StringBool1
	_false := StringBool0

	if IsStringBoolTrue(str) {
		return _true
	}
	if IsStringBoolFalse(str) {
		return _false
	}

	// String value unrecognized, return default value

	if defaultValue {
		return _true
	}

	return _false
}

// CastStringBoolToStringTrueFalse casts string-bool into string "true/false"
func CastStringBoolToStringTrueFalse(str string, defaultValue bool) string {
	// True and False values
	_true := StringBoolTrueLowercase
	_false := StringBoolFalseLowercase

	if IsStringBoolTrue(str) {
		return _true
	}
	if IsStringBoolFalse(str) {
		return _false
	}

	// String value unrecognized, return default value

	if defaultValue {
		return _true
	}

	return _false
}

// CreateStringID creates HEX hash ID out of string.
// In case maxHashLen == 0 the whole hash is returned
func CreateStringID(str string, maxHashLen int) string {
	// #nosec
	// G401 (CWE-326): Use of weak cryptographic primitive
	// It is good enough for string ID
	sha := sha1.New()
	sha.Write([]byte(str))
	hash := hex.EncodeToString(sha.Sum(nil))

	if maxHashLen == 0 {
		// Explicitly requested to return everything
		return hash
	}

	if maxHashLen >= len(hash) {
		// Requested hash len is greater than we have
		// Return whole hash - everything what we have
		return hash
	}

	// Requested hash len is smaller that the hash
	// Return last part of the hash
	return hash[len(hash)-maxHashLen:]
}

// StringHead returns beginning of the string of requested length
func StringHead(str string, maxHeadLen int) string {
	if len(str) <= maxHeadLen {
		// String is shorter than head requested - return everything
		return str
	}

	// Return beginning of the string
	return str[:maxHeadLen]
}
