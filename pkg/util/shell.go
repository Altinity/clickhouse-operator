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
	"crypto/md5"
	"encoding/hex"
	"regexp"
	"strings"
)

const shellEnvVarNameBaseMaxLength int = 63
const shellEnvVarNameFullMaxLength int = 127

var shellEnvVarNameRegexp = regexp.MustCompile("^[A-Z]([_A-Z0-9]*[A-Z0-9])?$")
var shellEnvVarNameStartRegexp = regexp.MustCompile("^[A-Z]")
var shellEnvVarNameNotAllowedCharsRegexp = regexp.MustCompile("[^_A-Z0-9]")
var shellEnvVarNameReplaceCharsRegexp = regexp.MustCompile("[/]")

func BuildShellEnvVarName(str string) (name string, ok bool) {
	// Do not touch original value
	name = str

	// Must be uppercase
	name = strings.ToUpper(name)

	// First char must comply to start regexp
	// Cut the first char until it is reasonable
	for len(name) > 0 {
		if shellEnvVarNameStartRegexp.MatchString(name) {
			break
		} else {
			name = name[1:]
		}
	}

	// Replace replaceable chars
	name = shellEnvVarNameReplaceCharsRegexp.ReplaceAllString(name, "_")
	// Remove not allowed chars
	name = shellEnvVarNameNotAllowedCharsRegexp.ReplaceAllString(name, "")

	// Must have limited length
	suffix := ""
	if len(name) > shellEnvVarNameBaseMaxLength {
		// Cut the name
		name = name[0:shellEnvVarNameBaseMaxLength]
		// Prepare fixed length suffix out of original string
		hash := md5.Sum([]byte(str))
		suffix = "_" + strings.ToUpper(hex.EncodeToString(hash[:]))
	}

	// Ensure no trailing underscores
	name = strings.TrimRight(name, "_")

	// Append suffix to keep name uniqueness
	name += suffix

	// It still has to be a valid env ma,e after all
	if IsShellEnvVarName(name) {
		return name, true
	}

	return "", false
}

// IsShellEnvVarName tests for a string that conforms to the definition of a shell ENV VAR name
func IsShellEnvVarName(value string) bool {
	if len(value) > shellEnvVarNameFullMaxLength {
		return false
	}
	if !shellEnvVarNameRegexp.MatchString(value) {
		return false
	}
	return true
}
