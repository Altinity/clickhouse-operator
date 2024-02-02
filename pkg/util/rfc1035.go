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

const dns1035LabelFmt string = "[a-z]([-a-z0-9]*[a-z0-9])?"
const dns1035LabelMaxLength int = 63

var dns1035LabelRegexp = regexp.MustCompile("^" + dns1035LabelFmt + "$")
var dns1035LabelStartRegexp = regexp.MustCompile("^" + "[a-z]")
var dns1035LabelNotAllowedCharsRegexp = regexp.MustCompile("[^-a-z0-9]")

func BuildRFC1035Label(str string) (string, bool) {
	// Must be lowercase
	str = strings.ToLower(str)
	// First char must comply to start regexp
	for len(str) > 0 {
		if dns1035LabelStartRegexp.MatchString(str) {
			break
		} else {
			str = str[1:]
		}
	}
	// Remove not allowed chars
	str = dns1035LabelNotAllowedCharsRegexp.ReplaceAllString(str, "")
	// Must have limited length
	if len(str) > dns1035LabelMaxLength {
		str = str[0:dns1035LabelMaxLength]
	}

	if IsDNS1035Label(str) {
		return str, true
	}

	return "", false
}

// IsDNS1035Label tests for a string that conforms to the definition of a label in DNS (RFC 1035).
func IsDNS1035Label(value string) bool {
	if len(value) > dns1035LabelMaxLength {
		return false
	}
	if !dns1035LabelRegexp.MatchString(value) {
		return false
	}
	return true
}
