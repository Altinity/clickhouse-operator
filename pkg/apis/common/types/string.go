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

// String casts to a string
func (s *String) String() string {
	if s == nil {
		return ""
	}
	return s.Value()
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

// IsValid checks whether var has a proper value
func (s *String) IsValid() bool {
	return s.HasValue()
}

// Normalize normalizes value with fallback to defaultValue in case initial value is incorrect
func (s *String) Normalize(defaultValue string) *String {
	if s.IsValid() {
		return s
	}

	// Value is unrecognized, return default value
	return NewString(defaultValue)
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
