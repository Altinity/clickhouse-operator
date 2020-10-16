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
	"bytes"
	"sort"
)

// IncludeNonEmpty inserts (and overwrites) data into map object using specified key, if not empty value provided
func IncludeNonEmpty(dst map[string]string, key, src string) {
	// Do not include empty value
	if src == "" {
		return
	}

	// Include (and overwrite) value by specified key
	dst[key] = src

	return
}

// MergeStringMaps inserts (and overwrites) data into dst map object from src
func MergeStringMaps(dst, src map[string]string, keys ...string) map[string]string {
	if src == nil {
		// Nothing to merge from
		return dst
	}

	if dst == nil {
		dst = make(map[string]string)
	}

	// Place key->value pair from src into dst

	if len(keys) == 0 {
		// No explicitly specified keys to merge, just merge the whole src
		for key := range src {
			dst[key] = src[key]
		}
	} else {
		// We have explicitly specified list of keys to merge from src
		for _, key := range keys {
			if value, ok := src[key]; ok {
				dst[key] = value
			}
		}
	}

	return dst
}

// MapHasKeys checks whether map has all keys from specified list
func MapHasKeys(m map[string]string, keys ...string) bool {
	for _, needle := range keys {
		// Have we found this needle
		found := false
		for key := range m {
			if key == needle {
				found = true
				break // for
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// Map2String returns named map[string]string mas as a string
func Map2String(name string, m map[string]string) string {
	// Write map entries according to sorted keys
	// So we need to
	// 1. Extract and sort all keys
	// 2. Walk over keys and write map entries

	// 1. Sort keys
	var keys []string
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Walk over sorted keys
	b := &bytes.Buffer{}
	Fprintf(b, "%s (%d):\n", name, len(m))
	for _, key := range keys {
		Fprintf(b, "  - [%s]=%s\n", key, m[key])
	}

	return b.String()
}
