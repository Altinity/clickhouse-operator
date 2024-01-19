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
	"regexp"
	"strings"
)

const shellEnvVarNameFmt string = "[A-Z]([_A-Z0-9]*[A-Z0-9])?"
const shellEnvVarNameMaxLength int = 63

var shellEnvVarNameRegexp = regexp.MustCompile("^" + shellEnvVarNameFmt + "$")
var shellEnvVarNameStartRegexp = regexp.MustCompile("^" + "[A-Z]")
var shellEnvVarNameNotAllowedCharsRegexp = regexp.MustCompile("[^_A-Z0-9]")

func BuildShellEnvVarName(str string) (string, bool) {
	// Must be uppercase
	str = strings.ToUpper(str)
	// First char must comply to start regexp
	for len(str) > 0 {
		if shellEnvVarNameStartRegexp.MatchString(str) {
			break
		} else {
			str = str[1:]
		}
	}
	// Remove not allowed chars
	str = shellEnvVarNameNotAllowedCharsRegexp.ReplaceAllString(str, "")
	// Must have limited length
	if len(str) > shellEnvVarNameMaxLength {
		str = str[0:shellEnvVarNameMaxLength]
	}

	if IsShellEnvVarName(str) {
		return str, true
	}

	return "", false
}

// IsShellEnvVarName tests for a string that conforms to the definition of a shell ENV VAR name
func IsShellEnvVarName(value string) bool {
	if len(value) > shellEnvVarNameMaxLength {
		return false
	}
	if !shellEnvVarNameRegexp.MatchString(value) {
		return false
	}
	return true
}
