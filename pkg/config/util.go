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

package config

import (
	"os"
	"path/filepath"
	"strings"
)

// isDirOk returns whether the given path exists and is a dir
func isDirOk(path string) bool {
	if stat, err := os.Stat(path); (err == nil) && stat.IsDir() {
		// File object Stat-ed without errors - it exists and it is a dir
		return true
	}

	// Some kind of error has happened
	return false
}

// extToLower fetches and lowercases file extension. With dot, as '.xml'
func extToLower(file string) string {
	return strings.ToLower(filepath.Ext(file))
}

func inArray(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
