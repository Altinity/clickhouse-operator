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


// TargetSelector specifies target selector based on labels
type TargetSelector map[string]string

// Matches checks whether TargetSelector matches provided set of labels
func (s TargetSelector) Matches(labels map[string]string) bool {
	if s == nil {
		// Empty selector matches all labels
		return true
	}

	// Walk over selector keys
	for key, selectorValue := range s {
		if labelValue, ok := labels[key]; !ok {
			// Labels have no key specified in selector.
			// Selector does not match the labels
			return false
		} else if selectorValue != labelValue {
			// Labels have the key specified in selector, but selector value is not the same as labels value
			// Selector does not match the labels
			return false
		} else {
			// Selector value and label value are equal
			// So far label matches selector
			// Continue iteration to next value
		}
	}

	// All keys are in place with the same values
	// Selector matches the labels

	return true
}
