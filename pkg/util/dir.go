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

import "path/filepath"

// RelativeToBasePath returns absolute path relative to base
func RelativeToBasePath(basePath, relativePath string) string {
	if basePath == "" {
		// Relative base is not set, no base to be relative to, consider relative path to be absolute
		return relativePath
	}

	// Relative base is set - try to be relative to it
	if absPath, err := filepath.Abs(basePath + "/" + relativePath); err == nil {
		// Absolute path is fine
		return absPath
	}

	// Unable to build correct absolute path, this is an error
	return ""
}

// PreparePath - prepares path absolute/relative with default relative value
func PreparePath(path *string, basePath, defaultRelativePath string) {
	switch {
	case *path == "":
		// Path is not specified at all.
		// Build path as 'default' path relative to the specified base
		*path = RelativeToBasePath(basePath, defaultRelativePath)
	case filepath.IsAbs(*path):
		// Path is specified as an absolute path.
		// Absolute paths are considered to be prepared already.
		// Nothing to do here.
	default:
		// Path is specified as relative path.
		// Build result path as provided relative path relative to the base path
		*path = RelativeToBasePath(basePath, *path)
	}

	// Specified path should exist
	// In case of incorrect/unavailable path - make it empty
	if (*path != "") && !IsDirOk(*path) {
		*path = ""
	}
}
