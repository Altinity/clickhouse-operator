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

import "strings"

// MapReplacer replaces a list of strings with replacements on a map.
type MapReplacer struct {
	*strings.Replacer
}

// NewMapReplacer creates new MapReplacer
func NewMapReplacer(r *strings.Replacer) *MapReplacer {
	return &MapReplacer{
		r,
	}
}

// Replace returns a copy of m with all replacements performed.
func (r *MapReplacer) Replace(m map[string]string) map[string]string {
	if len(m) == 0 {
		return m
	}
	result := make(map[string]string, len(m))
	for key := range m {
		result[key] = r.Replacer.Replace(m[key])
	}
	return result
}
