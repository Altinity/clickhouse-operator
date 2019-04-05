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
	"github.com/golang/glog"
	"io/ioutil"
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

// readConfigFiles reads config files from specified path into "file name->file content" map
// path - folder where to look for files
// isChConfigExt - accepts path to file return bool whether this file has config extension
func readConfigFiles(path string, isConfigExt func(string) bool) map[string]string {
	// Look in real path only
	if path == "" {
		return nil
	}

	// Result is a filename to content map
	var files map[string]string
	// Loop over all files in folder
	if matches, err := filepath.Glob(path + "/*"); err == nil {
		for i := range matches {
			// `file` comes with `path`-prefixed.
			// So in case `path` is an absolute path, `file` will be absolute path to file
			file := matches[i]
			if isConfigExt(file) {
				// Pick files with proper extensions only
				glog.Infof("CommonConfig file %s\n", file)
				if content, err := ioutil.ReadFile(file); err == nil {
					if files == nil {
						files = make(map[string]string)
					}
					files[filepath.Base(file)] = string(content)
				}
			}
		}
	}

	if len(files) > 0 {
		return files
	} else {
		return nil
	}
}

// extToLower fetches and lowercases file extension. With dot, as '.xml'
func extToLower(file string) string {
	return strings.ToLower(filepath.Ext(file))
}

// inArray checks whether a is in list
func inArray(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
