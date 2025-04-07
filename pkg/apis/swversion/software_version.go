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
	"regexp"
	"strconv"
	"strings"

	"github.com/Masterminds/semver/v3"
)

// SoftWareVersion specifies software version and software semver
type SoftWareVersion struct {
	// original specifies original software version, such as 21.9.6.24-alpha
	original string
	// normalized specifies semver-compatible - version truncated to 3 numbers, such as 21.9.6 for 21.9.6.24-alpha
	normalized string
	// description specifies description if needed
	description string
}

// NewSoftWareVersion creates new software version
// version - specifies original software version, such as: 21 or 21.1 or 21.9.6.24-alpha
func NewSoftWareVersion(version string) *SoftWareVersion {
	if strings.TrimSpace(version) == "" {
		return nil
	}

	// Fetch comma-separated parts of the software version
	parts := strings.Split(version, ".")

	// Need to have at least something to as a major version
	if len(parts) < 1 {
		return nil
	}

	// Need to have at least 3 parts in software version specification
	for len(parts) < 3 {
		parts = append(parts, "0")
	}

	// Take first 3 parts and ensure they are digits
	parts = parts[0:3]
	for _, part := range parts {
		if _, err := strconv.Atoi(part); err != nil {
			return nil
		}
	}

	// Build version
	return &SoftWareVersion{
		original: version,
		normalized:  strings.Join(parts, "."),
	}
}

func NewSoftWareVersionFromTag(tag string) *SoftWareVersion {
	if strings.ToLower(strings.TrimSpace(tag)) == "latest" {
		return MaxVersion()
	}

	r := regexp.MustCompile(`\d+(\.\d+)+`)
	return NewSoftWareVersion(r.FindString(tag))
}

func MinVersion() *SoftWareVersion {
	return NewSoftWareVersion("0.0.1")
}

func MaxVersion() *SoftWareVersion {
	return NewSoftWareVersion("99.99.99")
}

// Matches checks whether software version matches specified constraint or not
func (v *SoftWareVersion) Matches(constraint string) bool {
	if v == nil {
		return false
	}

	c, err := semver.NewConstraint(constraint)
	if err != nil {
		return false
	}

	_semver, err := semver.NewVersion(v.normalized)
	if err != nil {
		return false
	}

	// Validate a version against a constraint.
	matches, _ := c.Validate(_semver)

	return matches
}

// IsUnknown checks whether software version is unknown or not
func (v *SoftWareVersion) IsUnknown() bool {
	if v == nil {
		return true
	}
	if len(v.normalized) == 0 {
		return true
	}
	return false
}

func (v *SoftWareVersion) SetDescription(desc string) *SoftWareVersion {
	if v == nil {
		return nil
	}
	v.description = desc
	return v
}

// String makes a string
func (v *SoftWareVersion) String() string {
	if v == nil {
		return ""
	}
	return v.normalized
}

// Render makes a string
func (v *SoftWareVersion) Render() string {
	if v == nil {
		return ""
	}
	return v.normalized + "[" + v.original + "/" + v.description + "]"
}
