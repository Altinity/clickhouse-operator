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
	"math"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	log "github.com/golang/glog"
	// log "k8s.io/klog"
)

const (
	// Floats with fractional part less than ignoreThreshold are considered to be ints and are casted to ints
	ignoreThreshold = 0.001
)

type SettingsSection string

var (
	SectionEmpty  SettingsSection = ""
	SectionCommon SettingsSection = "COMMON"
	SectionUsers  SettingsSection = "USERS"
	SectionHost   SettingsSection = "HOST"

	errorNoSectionSpecified = fmt.Errorf("no section specified")
)

// Settings value can be one of:
// 1. scalar value (string, int, bool, etc).
//		Ex.:
//			user1/networks/ip: "::/0"
// 2. vector of scalars
//		Ex.:
//			user1/networks/ip:
//				- "127.0.0.1"
//				- "192.168.1.2"
// We do not know types of these scalars in advance also

// Setting
type Setting struct {
	isScalar bool
	scalar   string
	vector   []string
}

// NewScalarSetting
func NewScalarSetting(scalar string) *Setting {
	return &Setting{
		isScalar: true,
		scalar:   scalar,
	}
}

// NewVectorSetting
func NewVectorSetting(vector []string) *Setting {
	return &Setting{
		isScalar: false,
		vector:   vector,
	}
}

// IsScalar
func (s *Setting) IsScalar() bool {
	return s.isScalar
}

// IsVector
func (s *Setting) IsVector() bool {
	return !s.isScalar
}

// Scalar
func (s *Setting) Scalar() string {
	return s.scalar
}

// Vector
func (s *Setting) Vector() []string {
	return s.vector
}

// AsVector
func (s *Setting) AsVector() []string {
	if s.isScalar {
		return []string{
			s.scalar,
		}
	}
	return s.vector
}

// String
func (s *Setting) String() string {
	if s.isScalar {
		return s.scalar
	}

	return strings.Join(s.vector, ",")
}

// Settings
type Settings map[string]*Setting

// NewSettings
func NewSettings() Settings {
	return make(Settings)
}

// UnmarshalJSON
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

// MarshalJSON
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

// unmarshalScalar
func unmarshalScalar(untyped interface{}) (string, bool) {
	typeOf := reflect.TypeOf(untyped)
	str := typeOf.String()
	log.V(3).Infof("%v", str)

	switch untyped.(type) {
	case // scalar
		int, uint,
		int8, uint8,
		int16, uint16,
		int32, uint32,
		int64, uint64,
		bool,
		string:
		return fmt.Sprintf("%v", untyped), true
	case // scalar
		float32:
		floatVal := untyped.(float32)
		_, frac := math.Modf(float64(floatVal))
		if frac < ignoreThreshold {
			// consider it int
			intVal := int64(floatVal)
			return fmt.Sprintf("%v", intVal), true
		}
		return fmt.Sprintf("%f", untyped), true
	case // scalar
		float64:
		floatVal := untyped.(float64)
		_, frac := math.Modf(floatVal)
		if frac < ignoreThreshold {
			// consider it int
			intVal := int64(floatVal)
			return fmt.Sprintf("%v", intVal), true
		}
		return fmt.Sprintf("%f", untyped), true
	}

	return "", false
}

// unmarshalVector
func unmarshalVector(untyped interface{}) ([]string, bool) {
	typeOf := reflect.TypeOf(untyped)
	str := typeOf.String()
	log.V(3).Infof("%v", str)

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

// getValueAsScalar
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

// getValueAsVector
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

// getValueAsInt
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

// fetchPort
func (settings Settings) fetchPort(name string) int32 {
	return int32(settings.getValueAsInt(name))
}

// GetTCPPort
func (settings Settings) GetTCPPort() int32 {
	return settings.fetchPort("tcp_port")
}

// GetHTTPPort
func (settings Settings) GetHTTPPort() int32 {
	return settings.fetchPort("http_port")
}

// GetInterserverHTTPPort
func (settings Settings) GetInterserverHTTPPort() int32 {
	return settings.fetchPort("interserver_http_port")
}

// MergeFrom merges into `dst` non-empty new-key-values from `src` in case no such `key` already in `src`
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

// GetStringMap
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

func (settings Settings) GetSectionStringMap(section SettingsSection, includeUnspecified bool) map[string]string {
	m := make(map[string]string)

	for path := range settings {
		_section, err := getSectionFromPath(path)
		if (err == nil) && (_section != section) {
			// This is not the section we are looking for, skip to next
			continue // for
		}
		if (err != nil) && (err != errorNoSectionSpecified) {
			// We have an complex error, skip to next
			continue // for
		}
		if (err == errorNoSectionSpecified) && !includeUnspecified {
			// We are not ready to include unspecified section, skip to next
			continue // for
		}

		if scalar, ok := settings.getValueAsScalar(path); ok {
			m[path] = scalar
		} else {
			// Skip vector for now
		}
	}

	return m
}

// AsSortedSliceOfStrings
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

// Normalize
func (settings Settings) Normalize() {
	settings.normalizePaths()
}

// normalizePaths normalizes paths in settings
func (settings Settings) normalizePaths() {
	pathsToNormalize := make([]string, 0, 0)

	// Find entries with paths to normalize
	for unNormalizedPath := range settings {
		normalizedPath := normalizeSettingsKeyAsPath(unNormalizedPath)
		if len(normalizedPath) != len(unNormalizedPath) {
			// Normalization changed something. This path has to be normalized
			pathsToNormalize = append(pathsToNormalize, unNormalizedPath)
		}
	}

	// Add entries with normalized paths
	for _, unNormalizedPath := range pathsToNormalize {
		normalizedPath := normalizeSettingsKeyAsPath(unNormalizedPath)
		settings[normalizedPath] = settings[unNormalizedPath]
	}

	// Delete entries with un-normalized paths
	for _, unNormalizedPath := range pathsToNormalize {
		delete(settings, unNormalizedPath)
	}
}

// normalizeSettingsKeyAsPath normalizes path in .spec.configuration.{users, profiles, quotas, settings, files} section
// Normalized path looks like 'a/b/c'
func normalizeSettingsKeyAsPath(path string) string {
	// Normalize multi-'/' values (like '//') to single-'/'
	re := regexp.MustCompile("//+")
	path = re.ReplaceAllString(path, "/")

	// Cut all leading and trailing '/', so the result would be 'a/b/c'
	return strings.Trim(path, "/")
}

func getSectionFromPath(path string) (SettingsSection, error) {
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		// We need to have path to be at least section/file.name
		return SectionEmpty, errorNoSectionSpecified
	}

	section := parts[0]
	if strings.EqualFold(section, string(SectionCommon)) || strings.EqualFold(section, CommonConfigDir) {
		return SectionCommon, nil
	}
	if strings.EqualFold(section, string(SectionUsers)) || strings.EqualFold(section, UsersConfigDir) {
		return SectionUsers, nil
	}
	if strings.EqualFold(section, string(SectionHost)) || strings.EqualFold(section, HostConfigDir) {
		return SectionHost, nil
	}

	log.V(1).Infof("unknown section specified %v", section)

	return SectionEmpty, fmt.Errorf("unknown section specified %v", section)
}
