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

package types

import (
	"github.com/altinity/clickhouse-operator/pkg/util"
	"strings"
)

// Strings defines set of strings representation with possibility to be optional
type Strings []string

// NewStrings creates new variable
func NewStrings(str []string) *Strings {
	s := new(Strings)
	*s = append(*s, str...)
	*s = util.Unique(*s)
	return s
}

// From casts string
func (s *Strings) From(value []string) *Strings {
	return NewStrings(value)
}

// HasValue checks whether value is specified
func (s *Strings) HasValue() bool {
	return s.IsValid() && (s.Len() > 0)
}

// Value returns value
func (s *Strings) Value() []string {
	if s.IsValid() {
		return *s
	}
	return nil
}

func (s *Strings) First() string {
	if s.HasValue() {
		return (*s)[0]
	}
	return ""
}

// String casts to a string
func (s *Strings) String() string {
	return strings.Join(s.Value(), " ")
}

// IsValid checks whether var has a proper value
func (s *Strings) IsValid() bool {
	return s != nil
}

// Len calculates len of the string
func (s *Strings) Len() int {
	if s.IsValid() {
		return len(*s)
	}
	return 0
}

// Normalize normalizes value with fallback to defaultValue in case initial value is incorrect
func (s *Strings) Normalize(defaultValue []string) *Strings {
	if s.IsValid() {
		return s
	}

	// Value is unrecognized, return default value
	return NewStrings(defaultValue)
}

// MergeFrom merges value from another variable
func (s *Strings) MergeFrom(from *Strings) *Strings {
	if from == nil {
		// Nothing to merge from, keep original value
		return s
	}

	// From now on we have `from` specified

	if s == nil {
		// Recipient is not specified, just use `from` value
		return from
	}

	// Both recipient and `from` are specified, need to merge

	*s = append(*s, from.Value()...)

	return NewStrings(s.Value())
}

func (s *Strings) Append(str string) *Strings {
	return s.MergeFrom(NewStrings([]string{str}))
}
