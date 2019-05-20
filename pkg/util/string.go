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
	"crypto/sha1"
	"encoding/hex"
	"math/rand"
	"strings"
	"time"
)

// RandomString generates random string
func RandomString() string {
	b := make([]byte, 3)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(b)
	return hex.EncodeToString(b)
}

// IsStringBool checks whether str is a string as bool value
func IsStringBool(str string) bool {
	switch strings.ToLower(str) {
	case "0": return true
	case "1": return true

	case "false": return true
	case "true": return true

	case "no": return true
	case "yes": return true

	case "off": return true
	case "on": return true

	case "disabled": return true
	case "enabled": return true

	default: return false
	}
}

// IsStringBool checks whether str is a string as bool "false" value
func IsStringBoolFalse(str string) bool {
	switch strings.ToLower(str) {
	case "0": return true

	case "false": return true

	case "no": return true

	case "off": return true

	case "disabled": return true

	default: return false
	}
}

// IsStringBool checks whether str is a string as bool "true" value
func IsStringBoolTrue(str string) bool {
	switch strings.ToLower(str) {
	case "1": return true

	case "true": return true

	case "yes": return true

	case "on": return true

	case "enabled": return true

	default: return false
	}
}

// CastStringBoolTo01 casts string-bool into string "0/1"
func CastStringBoolTo01(str string, defaultValue bool) string {
	// True and False values
	t := "1"
	f := "0"

	if IsStringBoolTrue(str) {
		return t
	}
	if IsStringBoolFalse(str) {
		return f
	}

	// String value unrecognized, return default value

	if defaultValue {
		return t
	} else {
		return f
	}
}

// CastStringBoolToTrueFalse casts string-bool into string "true/false"
func CastStringBoolToTrueFalse(str string, defaultValue bool) string {
	// True and False values
	t := "true"
	f := "false"

	if IsStringBoolTrue(str) {
		return t
	}
	if IsStringBoolFalse(str) {
		return f
	}

	// String value unrecognized, return default value

	if defaultValue {
		return t
	} else {
		return f
	}
}

// CreateStringID creates HEX hash ID out of string.
// In case hashLen == 0 the whole hash is returned
func CreateStringID(str string, hashLen int) string {
	hasher := sha1.New()
	hasher.Write([]byte(str))
	hash := hex.EncodeToString(hasher.Sum(nil))
	if hashLen == 0 {
		return hash
	} else {
		return hash[len(hash)-hashLen:]
	}
}
