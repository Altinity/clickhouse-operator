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

import (
	"regexp"
	"strconv"
	"strings"
)

type Settings map[string]interface{}

func NewSettings() Settings {
	return make(Settings)
}

func getSettingsValueAsInt(value interface{}) int32 {
	switch value.(type) {
	case string:
		intValue, err := strconv.Atoi(value.(string))
		if err == nil {
			return int32(intValue)
		}

	case int, int8, int16, int32, uint, uint8, uint32:
		return value.(int32)

	case int64:
		return int32(value.(int64))

	case uint64:
		return int32(value.(uint64))
	}

	return 0
}

func (settings Settings) getValueByName(name string) (interface{}, bool) {
	value, ok := settings[name]
	return value, ok
}

func (settings Settings) fetchPort(name string) int32 {
	if value, ok := settings.getValueByName(name); ok {
		return getSettingsValueAsInt(value)
	} else {
		return 0
	}
}

func (settings Settings) GetTCPPort() int32 {
	return settings.fetchPort("tcp_port")
}

func (settings Settings) GetHTTPPort() int32 {
	return settings.fetchPort("http_port")
}

func (settings Settings) GetInterserverHTTPPort() int32 {
	return settings.fetchPort("interserver_http_port")
}

// mapStringInterfaceMergeFrom merges into `dst` non-empty new-key-values from `src` in case no such `key` already in `src`
func (settings *Settings) MergeFrom(src Settings) {
	if src == nil {
		return
	}

	if *settings == nil {
		*settings = NewSettings()
	}

	for key, value := range src {
		if _, ok := (*settings)[key]; ok {
			// Such key already exists in dst
			continue
		}

		// No such a key in dst
		(*settings)[key] = value
	}
}

func (settings Settings) GetStringMap() map[string]string {
	m := make(map[string]string)

	for key := range settings {
		if stringValue, ok := settings[key].(string); ok {
			m[key] = stringValue
		}
	}

	return m
}

// NormalizePaths normalizes paths in settings
func (settings Settings) NormalizePaths() {
	pathsToNormalize := make([]string, 0, 0)

	// Find entries with paths to normalize
	for key := range settings {
		path := normalizeSettingsKeyAsPath(key)
		if len(path) != len(key) {
			// Normalization worked. These paths have to be normalized
			pathsToNormalize = append(pathsToNormalize, key)
		}
	}

	// Add entries with normalized paths
	for _, key := range pathsToNormalize {
		normalizedPath := normalizeSettingsKeyAsPath(key)
		settings[normalizedPath] = settings[key]
	}

	// Delete entries with un-normalized paths
	for _, key := range pathsToNormalize {
		delete(settings, key)
	}
}

// normalizeSettingsKeyAsPath normalizes path in .spec.configuration.{users, profiles, quotas, settings} section
// Normalized path looks like 'a/b/c'
func normalizeSettingsKeyAsPath(path string) string {
	// Normalize multi-'/' values (like '//') to single-'/'
	re := regexp.MustCompile("//+")
	path = re.ReplaceAllString(path, "/")

	// Cut all leading and trailing '/', so the result would be 'a/b/c'
	return strings.Trim(path, "/")
}
