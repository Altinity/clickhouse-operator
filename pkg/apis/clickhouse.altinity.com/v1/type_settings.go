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
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Settings value can be one of:
// 1. scalar value (string, int, bool, etc).
//    Ex.:
//    user1/networks/ip: "::/0"
// 2. vector of scalars
//    Ex.:
//    user1/networks/ip:
//      - "127.0.0.1"
//      - "192.168.1.2"
// We do not know types of these scalars in advance also

type Setting struct {
	isScalar bool
	scalar   string
	vector   []string
}

func NewScalarSetting(scalar string) *Setting {
	return &Setting{
		isScalar: true,
		scalar:   scalar,
	}
}

func NewVectorSetting(vector []string) *Setting {
	return &Setting{
		isScalar: false,
		vector:   vector,
	}
}

func (s *Setting) IsScalar() bool {
	return s.isScalar
}

func (s *Setting) Scalar() string {
	return s.scalar
}

func (s *Setting) Vector() []string {
	return s.vector
}

func (s *Setting) AsVector() []string {
	if s.isScalar {
		return []string{s.scalar}
	}
	return s.vector
}

func (s *Setting) String() string {
	if s.isScalar {
		return s.scalar
	}

	return strings.Join(s.vector, ",")
}

type Settings map[string]*Setting

func NewSettings() Settings {
	return make(Settings)
}

func (settings *Settings) UnmarshalJSON(data []byte) error {
	type rawType map[string]interface{}
	var raw rawType
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if *settings == nil {
		*settings = NewSettings()
	}

	for name, untyped := range raw {
		if scalar, ok := unmarshalScalar(untyped); ok {
			(*settings)[name] = NewScalarSetting(scalar)
		} else if vector, ok := unmarshalVector(untyped); ok {
			if len(vector) > 0 {
				(*settings)[name] = NewVectorSetting(vector)
			}
		}
	}

	return nil
}

func (settings Settings) MarshalJSON() ([]byte, error) {
	raw := make(map[string]interface{})

	for name, setting := range settings {
		if setting.isScalar {
			raw[name] = setting.scalar
		} else {
			raw[name] = setting.vector
		}
	}

	return json.Marshal(raw)
}

func unmarshalScalar(untyped interface{}) (string, bool) {
	switch untyped.(type) {
	case // scalar
		string,
		int, uint,
		int8, uint8,
		int16, uint16,
		int32, uint32,
		int64, uint64,
		bool:
		return fmt.Sprintf("%v", untyped), true
	}

	return "", false
}

func unmarshalVector(untyped interface{}) ([]string, bool) {
	var res []string
	switch untyped.(type) {
	case // vector
		[]interface{}:
		for _, _untyped := range untyped.([]interface{}) {
			if scalar, ok := unmarshalScalar(_untyped); ok {
				res = append(res, scalar)
			}
		}
		return res, true
	}

	return nil, false
}

func (settings Settings) getValueAsScalar(name string) (string, bool) {
	setting, ok := settings[name]
	if !ok {
		return "", false
	}
	if setting.isScalar {
		return setting.scalar, true
	}
	return "", false
}

func (settings Settings) getValueAsVector(name string) ([]string, bool) {
	setting, ok := settings[name]
	if !ok {
		return nil, false
	}
	if setting.isScalar {
		return nil, false
	}
	return setting.vector, true
}

func (settings Settings) getValueAsInt(name string) int {
	value, ok := settings.getValueAsScalar(name)
	if !ok {
		return 0
	}

	i, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}

	return i
}

func (settings Settings) fetchPort(name string) int32 {
	return int32(settings.getValueAsInt(name))
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
		if scalar, ok := settings.getValueAsScalar(key); ok {
			m[key] = scalar
		} else {
			// Skip vector for now
		}
	}

	return m
}

func (settings Settings) AsSortedSliceOfStrings() []string {
	// Sort keys
	var keys []string
	for key := range settings {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var s []string

	// Walk over sorted keys
	for _, key := range keys {
		s = append(s, key)
		s = append(s, settings[key].String())
	}

	return s
}

func (settings Settings) Normalize() {
	settings.normalizePaths()
}

// NormalizePaths normalizes paths in settings
func (settings Settings) normalizePaths() {
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
