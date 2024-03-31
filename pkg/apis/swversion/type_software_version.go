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

package swversion

import (
	"strings"

	"github.com/Masterminds/semver/v3"
)

// SoftWareVersion specifies software version and software semver
type SoftWareVersion struct {
	// Version specifies original software version, such as 21.9.6.24-alpha
	Version string
	// Semver specifies semver adaptation, truncated to 3 numbers, such as 21.9.6 for 21.9.6.24-alpha original version
	Semver string
}

// NewSoftWareVersion creates new software version
func NewSoftWareVersion(str string) *SoftWareVersion {
	// Need to have at least 3 parts in software version specification
	if parts := strings.Split(str, "."); len(parts) >= 3 {
		return &SoftWareVersion{
			Version: str,
			Semver:  strings.Join(parts[0:2], "."),
		}
	}
	return nil
}

// Matches checks whether software version matches specified constraint
func (v *SoftWareVersion) Matches(constraint string) bool {
	if v == nil {
		return false
	}

	c, err := semver.NewConstraint(constraint)
	if err != nil {
		return false
	}

	_semver, err := semver.NewVersion(v.Semver)
	if err != nil {
		return false
	}

	// Validate a version against a constraint.
	matches, _ := c.Validate(_semver)

	return matches
}

// IsUnknown checks whether software version is unknown
func (v *SoftWareVersion) IsUnknown() bool {
	if v == nil {
		return true
	}
	if len(v.Version) == 0 {
		return true
	}
	return false
}

// String makes string
func (v *SoftWareVersion) String() string {
	if v == nil {
		return ""
	}
	return v.Version
}
