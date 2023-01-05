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

// Secure specifies secure data type.
type Secure bool

// HasValue checks whether secure has value specified
func (s *Secure) HasValue() bool {
	return s != nil
}

// Value gets bool value of secure
func (s *Secure) Value() bool {
	if s == nil {
		return false
	}

	return *s == true
}

// MergeFrom merges value from specified Secure
func (s *Secure) MergeFrom(from *Secure) *Secure {
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
