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

import "strings"

// String defines string representation with possibility to be optional
type String string

// NewString creates new variable
func NewString(str string) *String {
	s := new(String)
	*s = String(str)
	return s
}

// From casts string
func (s *String) From(value string) *String {
	return NewString(value)
}

// HasValue checks whether value is specified
func (s *String) HasValue() bool {
	return s != nil
}

// Value returns value
func (s *String) Value() string {
	if s == nil {
		return ""
	}

	return string(*s)
}

// String casts to a string
func (s *String) String() string {
	return s.Value()
}

// IsValid checks whether var has a proper value
func (s *String) IsValid() bool {
	return s.HasValue()
}

// Len calculates len of the string
func (s *String) Len() int {
	if s == nil {
		return 0
	}
	return len(s.String())
}

// Normalize normalizes value with fallback to defaultValue in case initial value is incorrect
func (s *String) Normalize(defaultValue string) *String {
	if s.IsValid() {
		return s
	}

	// Value is unrecognized, return default value
	return NewString(defaultValue)
}

// Equal reports whether s and other hold the same value.
// Nil is treated as a distinct "unset" state: nil == nil is true; nil vs a non-nil
// pointer (even one holding "") is FALSE — an unset pointer is not the same as an
// explicitly empty value. Comparison is case-sensitive; use EqualFold for the
// case-insensitive variant.
func (s *String) Equal(other *String) bool {
	if (s == nil) || (other == nil) {
		return s == other
	}
	return s.Value() == other.Value()
}

// EqualFold is the case-insensitive analogue of Equal — uses strings.EqualFold for
// the underlying value comparison. Same nil-vs-set semantics as Equal: nil == nil
// is true; nil != non-nil regardless of the non-nil's value.
func (s *String) EqualFold(other *String) bool {
	if (s == nil) || (other == nil) {
		return s == other
	}
	return strings.EqualFold(s.Value(), other.Value())
}

// EqualFoldString reports whether the String case-insensitively equals a plain string
// value (typically an enum const). Nil-safe: a nil String compares as "" (so it never
// equals a non-empty value). Lets callers fold enum casing without wrapping the const
// in a *String, e.g. field.EqualFoldString(api.RecoveryActionRetry).
func (s *String) EqualFoldString(value string) bool {
	return strings.EqualFold(s.Value(), value)
}

// MergeFrom merges value from another variable
func (s *String) MergeFrom(from *String) *String {
	if from == nil {
		// Nothing to merge from, keep original value
		return s
	}

	// From now on we have `from` specified

	if s == nil {
		// Recipient is not specified, just use `from` value
		return from
	}

	// Both recipient and `from` are specified, need to pick one value.
	// Prefer local value
	return s
}
