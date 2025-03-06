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
	"reflect"
	"slices"

	"golang.org/x/exp/constraints"
)

// IncludeNonEmpty inserts (and overwrites) data into map object using specified key, if not empty value provided
func IncludeNonEmpty[TKey comparable, TValue any](dst map[TKey]TValue, key TKey, value TValue) {
	// Do not include empty value
	v := reflect.ValueOf(value)
	if v.IsZero() {
		return
	}

	// Include (and overwrite) value by specified key
	dst[key] = value
}

// CopyMap creates a copy of the given map by copying over key by key.
// It doesn't perform a deep-copy.
func CopyMap[TKey comparable, TValue any](src map[TKey]TValue) map[TKey]TValue {
	result := make(map[TKey]TValue, len(src))
	for key, value := range src {
		result[key] = value
	}
	return result
}

// CopyMapFilter copies maps with keys filtering.
// Keys specified in 'include' are included,
// keys specified in 'exclude' are excluded.
// However, 'include' keys are applied only in case 'include' list is not empty.
func CopyMapFilter[TKey comparable, TValue any](src map[TKey]TValue, includeKeys, excludeKeys []TKey) map[TKey]TValue {
	return CopyMapExclude(CopyMapInclude(src, includeKeys...), excludeKeys...)
}

// CopyMapInclude creates a copy of the given map but will include the given set of keys only.
// However, keys are applied only in case list is not empty.
// In case of an empty list, no filtering is performed and all keys are copied.
func CopyMapInclude[TKey comparable, TValue any](src map[TKey]TValue, includeKeys ...TKey) map[TKey]TValue {
	if len(includeKeys) == 0 {
		// No include list specified, just copy the whole map
		return CopyMap(src)
	}

	// Include list specified, copy listed keys only
	result := make(map[TKey]TValue, len(includeKeys))
	for _, key := range includeKeys {
		if value, ok := src[key]; ok {
			result[key] = value
		}
	}
	return result
}

// CopyMapExclude creates a copy of the given map but will exclude the given set of keys.
func CopyMapExclude[TKey comparable, TValue any](src map[TKey]TValue, excludeKeys ...TKey) map[TKey]TValue {
	result := CopyMap(src)

	for _, key := range excludeKeys {
		delete(result, key)
	}

	return result
}

// MergeStringMapsOverwrite inserts (and overwrites) data into dst map object from src
func MergeStringMapsOverwrite[TKey comparable, TValue any](dst, src map[TKey]TValue, keys ...TKey) map[TKey]TValue {
	if len(src) == 0 {
		// Nothing to merge from
		return dst
	}

	var created bool
	if dst == nil {
		dst = make(map[TKey]TValue)
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
func MergeStringMapsPreserve[TKey comparable, TValue any](dst, src map[TKey]TValue, keys ...TKey) map[TKey]TValue {
	if len(src) == 0 {
		// Nothing to merge from
		return dst
	}

	var created bool
	if dst == nil {
		dst = make(map[TKey]TValue)
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
func SubtractStringMaps[TKey comparable, TValue any](base, delta map[TKey]TValue) map[TKey]TValue {
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
func MapDeleteKeys[TKey comparable, TValue any](m map[TKey]TValue, keys ...TKey) map[TKey]TValue {
	if len(m) == 0 {
		// Nowhere to delete from
		return m
	}
	if len(keys) == 0 {
		// Nothing to delete
		return m
	}

	// Extract delete keys from base
	for _, key := range keys {
		if _, ok := m[key]; ok {
			delete(m, key)
		}
	}

	return m
}

// MapHasKeys checks whether map has all keys from specified list
func MapHasKeys[TKey comparable, TValue any](m map[TKey]TValue, keys ...TKey) bool {
	if len(m) == 0 {
		return false
	}
	if len(keys) == 0 {
		return false
	}

	for _, needle := range keys {
		// Have we found this needle
		if _, found := m[needle]; !found {
			// No, not found
			return false
		}
	}

	return true
}

func cmp[T constraints.Ordered](a, b T) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// Map2String returns named map as a string
func Map2String[TKey constraints.Ordered, TValue any](name string, m map[TKey]TValue) string {
	// Write map entries according to sorted keys
	// So we need to
	// 1. Extract and sort all keys
	// 2. Walk over keys and write map entries

	// 1. Sort keys
	keys := MapGetSortedKeys(m)

	// Walk over sorted keys
	b := &bytes.Buffer{}
	Fprintf(b, "%s (%d):\n", name, len(m))
	for _, key := range keys {
		Fprintf(b, "  - [%s]=%s\n", key, m[key])
	}

	return b.String()
}

func MapGetKeys[TKey comparable, TValue any](m map[TKey]TValue) (keys []TKey) {
	if m == nil {
		return nil
	}
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func MapGetSortedKeys[TKey constraints.Ordered, TValue any](m map[TKey]TValue) (keys []TKey) {
	if m == nil {
		return nil
	}
	for key := range m {
		keys = append(keys, key)
	}
	slices.SortStableFunc(keys, cmp)
	//sort.Strings(keys)
	return keys
}

func MapGetSortedKeysAndValues[TKey constraints.Ordered, TValue any](m map[TKey]TValue) (keys []TKey, values []TValue) {
	if m == nil {
		return nil, nil
	}
	keys = MapGetSortedKeys(m)
	for _, key := range keys {
		if value, ok := m[key]; ok {
			values = append(values, value)
		}
	}
	return keys, values
}

func MapMigrate[TKey constraints.Ordered, TValue any](cur, new, old map[TKey]TValue) map[TKey]TValue {
	removed := MapGetSortedKeys(SubtractStringMaps(CopyMap(old), new))
	return MapDeleteKeys(MergeStringMapsPreserve(new, cur), removed...)
}

func MapsAreTheSame[TKey comparable, TValue comparable](m1, m2 map[TKey]TValue) bool {
	if len(m1) != len(m2) {
		// Different set means not equal
		return false
	}

	for k1, v1 := range m1 {
		v2, found := m2[k1]
		if !found {
			// B has no key from A
			return false
		}
		// A and B has the same key
		// Values has to be the same to be considered equal
		if v1 != v2 {
			return false
		}
	}

	return true
}

func MapsIntersectKeys[TKey comparable, TValue any](m1, m2 map[TKey]TValue) (keysIntersection []TKey) {
	keys1 := MapGetKeys(m1)
	keys2 := MapGetKeys(m2)
	return SlicesIntersect(keys1, keys2)
}

func MapsHaveKeysIntersection[TKey comparable, TValue any](m1, m2 map[TKey]TValue) bool {
	return len(MapsIntersectKeys(m1, m2)) > 0
}

func MapsHaveSameKeyValuePairs[TKey comparable, TValue comparable](m1, m2 map[TKey]TValue, keys ...TKey) (same bool) {
	if len(keys) == 0 {
		return false
	}

	same = true
	for _, key := range keys {
		v1, ok1 := m1[key]
		v2, ok2 := m2[key]
		if ok1 && ok2 && (v1 == v2) {
				// The same
				continue
		}
		same = false
	}

	return same
}

func MapsHaveKeyValuePairsIntersection[TKey comparable, TValue comparable](m1, m2 map[TKey]TValue) bool {
	keys1 := MapGetKeys(m1)
	keys2 := MapGetKeys(m2)
	keys := SlicesIntersect(keys1, keys2)
	return MapsHaveSameKeyValuePairs(m1, m2, keys...)
}
