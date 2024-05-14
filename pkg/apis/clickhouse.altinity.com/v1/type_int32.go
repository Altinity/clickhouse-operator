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

package v1

import "strconv"

// Int32 defines int32 representation with possibility to be optional
type Int32 int32

// NewInt32 creates new variable
func NewInt32(i int32) *Int32 {
	i32 := new(Int32)
	*i32 = Int32(i)
	return i32
}

// From casts int32
func (i *Int32) From(value int32) *Int32 {
	return NewInt32(value)
}

// String casts to a string
func (i *Int32) String() string {
	if i == nil {
		return ""
	}
	return strconv.Itoa(i.IntValue())
}

// HasValue checks whether value is specified
func (i *Int32) HasValue() bool {
	return i != nil
}

// Value returns value
func (i *Int32) Value() int32 {
	if i == nil {
		return 0
	}

	return int32(*i)
}

// IntValue returns int value
func (i *Int32) IntValue() int {
	if i == nil {
		return 0
	}

	return int(*i)
}

// IsValid checks whether var has a proper value
func (i *Int32) IsValid() bool {
	return i.HasValue()
}

// Normalize normalizes value with fallback to defaultValue in case initial value is incorrect
func (i *Int32) Normalize(defaultValue int32) *Int32 {
	if i.IsValid() {
		return i
	}

	// Value is unrecognized, return default value
	return NewInt32(defaultValue)
}

// MergeFrom merges value from another variable
func (i *Int32) MergeFrom(from *Int32) *Int32 {
	if from == nil {
		// Nothing to merge from, keep original value
		return i
	}

	// From now on we have `from` specified

	if i == nil {
		// Recipient is not specified, just use `from` value
		return from
	}

	// Both recipient and `from` are specified, need to pick one value.
	// Prefer local value
	return i
}

// Equal checks whether is equal to another
func (i *Int32) Equal(to *Int32) bool {
	if (i == nil) && (to == nil) {
		// Consider nil equal
		return true
	}

	return i.EqualValue(to)
}

// EqualValue checks whether has equal values
func (i *Int32) EqualValue(to *Int32) bool {
	if !i.HasValue() || !to.HasValue() {
		// Need to compare values only
		return false
	}

	// Both have value available, comparable
	return i.Value() == to.Value()
}
