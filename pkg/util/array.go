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
	"fmt"
	"regexp"
	"sort"
)

// InArray checks whether the needle is in the haystack
func InArray(needle string, haystack []string) bool {
	for _, item := range haystack {
		if item == needle {
			return true
		}
	}
	return false
}

// InArrayWithRegexp checks whether the needle can be matched by haystack
func InArrayWithRegexp(needle string, haystack []string) bool {
	for _, item := range haystack {
		matched, _ := regexp.MatchString(item, needle)
		if item == needle || matched {
			return true
		}
	}
	return false
}

// MergeStringArrays appends into dst items from src that are not present in src. src items are being deduplicated
func MergeStringArrays(dst []string, src []string) []string {
	for _, str := range src {
		if !InArray(str, dst) {
			dst = append(dst, str)
		}
	}
	return dst
}

// RemoveFromArray removes removed the needle from the haystack
func RemoveFromArray(needle string, haystack []string) []string {
	result := []string{}

	for _, item := range haystack {
		if item == needle {
			continue
		}
		result = append(result, item)
	}

	return result
}

// Unzip makes two 1-value columns (slices) out of one 2-value column (slice)
func Unzip(slice [][]string) ([]string, []string) {
	col1 := make([]string, len(slice))
	col2 := make([]string, len(slice))
	for i := 0; i < len(slice); i++ {
		col1 = append(col1, slice[i][0])
		if len(slice[i]) > 1 {
			col2 = append(col2, slice[i][1])
		}
	}
	return col1, col2
}

// CastToSliceOfStrings makes slice of strings from map
func CastToSliceOfStrings(m map[string]interface{}) []string {
	res := make([]string, 0, 0)

	// Sort keys
	var keys []string
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Walk over sorted keys
	for _, key := range keys {
		res = append(res, key)

		switch m[key].(type) {
		case string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
			value := fmt.Sprint(m[key])
			res = append(res, value)
		case []string, []int, []int8, []int16, []int32, []int64, []uint, []uint8, []uint16, []uint32, []uint64, []float32, []float64, []interface{}:
			for _, v := range m[key].([]interface{}) {
				value := fmt.Sprint(v)
				res = append(res, value)
			}
		}
	}

	return res
}

// Slice2String returns named slice as a string
func Slice2String(name string, slice []string) string {
	b := &bytes.Buffer{}
	Fprintf(b, "%s (%d):\n", name, len(slice))
	for i := range slice {
		Fprintf(b, "  - %s\n", slice[i])
	}

	return b.String()
}
