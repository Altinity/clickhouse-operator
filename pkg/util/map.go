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

// CopyMap creates a copy of the given map by copying over key by key.
// It doesn't perform a deep-copy.
func CopyMap(src map[string]string) map[string]string {
	result := make(map[string]string, len(src))
	for key, value := range src {
		result[key] = value
	}
	return result
}

// CopyMapFilter copies maps with keys filtering.
// Keys specified in 'include' are included,
// keys specified in 'exclude' are excluded.
// However, 'include' keys are applied only in case 'include' list is not empty.
func CopyMapFilter(src map[string]string, include, exclude []string) map[string]string {
	return CopyMapExclude(CopyMapInclude(src, include...), exclude...)
}

// CopyMapInclude creates a copy of the given map but will include the given set of keys only.
// However, keys are applied only in case list is not empty.
// In case of an empty list, no filtering is performed and all keys are copied.
func CopyMapInclude(src map[string]string, keys ...string) map[string]string {
	if len(keys) == 0 {
		// No include list specified, just copy the whole map
		return CopyMap(src)
	}

	// Include list specified, copy listed keys only
	result := make(map[string]string, len(keys))
	for _, key := range keys {
		if value, ok := src[key]; ok {
			result[key] = value
		}
	}
	return result
}

// CopyMapExclude creates a copy of the given map but will exclude the given set of keys.
func CopyMapExclude(src map[string]string, exceptKeys ...string) map[string]string {
	result := CopyMap(src)

	for _, key := range exceptKeys {
		delete(result, key)
	}

	return result
}

// MergeStringMapsOverwrite inserts (and overwrites) data into dst map object from src
func MergeStringMapsOverwrite(dst, src map[string]string, keys ...string) map[string]string {
	if len(src) == 0 {
		// Nothing to merge from
		return dst
	}

	var created bool
	if dst == nil {
		dst = make(map[string]string)
		created = true
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

	if created && (len(dst) == 0) {
		return nil
	}

	return dst
}

// MergeStringMapsPreserve inserts (and preserved existing) data into dst map object from src
func MergeStringMapsPreserve(dst, src map[string]string, keys ...string) map[string]string {
	if len(src) == 0 {
		// Nothing to merge from
		return dst
	}

	var created bool
	if dst == nil {
		dst = make(map[string]string)
		created = true
	}

	// Place key->value pair from src into dst

	if len(keys) == 0 {
		// No explicitly specified keys to merge, just merge the whole src
		for key := range src {
			if _, ok := dst[key]; !ok {
				dst[key] = src[key]
			}
		}
	} else {
		// We have explicitly specified list of keys to merge from src
		for _, key := range keys {
			if value, ok := src[key]; ok {
				if _, ok := dst[key]; !ok {
					dst[key] = value
				}
			}
		}
	}

	if created && (len(dst) == 0) {
		return nil
	}

	return dst
}

// SubtractStringMaps subtracts "delta" from "base" by keys
func SubtractStringMaps(base, delta map[string]string) map[string]string {
	if len(delta) == 0 {
		// Nothing to delete
		return base
	}
	if len(base) == 0 {
		// Nowhere to delete from
		return base
	}

	// Extract keys from delta and delete them from base
	for key := range delta {
		if _, ok := base[key]; ok {
			delete(base, key)
		}
	}

	return base
}

// MapDeleteKeys deletes multiple keys from the map
func MapDeleteKeys(base map[string]string, keys ...string) map[string]string {
	if len(keys) == 0 {
		// Nothing to delete
		return base
	}
	if len(base) == 0 {
		// Nowhere to delete from
		return base
	}

	// Extract delete keys from base
	for _, key := range keys {
		if _, ok := base[key]; ok {
			delete(base, key)
		}
	}

	return base
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
