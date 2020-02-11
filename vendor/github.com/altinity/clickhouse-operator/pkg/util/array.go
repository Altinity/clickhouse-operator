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

// InArray checks whether needle is in haystack
func InArray(needle string, haystack []string) bool {
	for _, b := range haystack {
		if b == needle {
			return true
		}
	}
	return false
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
