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
)

const (
	// Float with fractional part less than ignoreThreshold is considered to be int and is casted to int
	ignoreThreshold = 0.001
)

// SettingsSection specifies settings section
type SettingsSection string

// Configuration sections
// Each section translates into separate ConfigMap mapped into Pod
var (
	SectionEmpty  SettingsSection = ""
	SectionCommon SettingsSection = "COMMON"
	SectionUsers  SettingsSection = "USERS"
	SectionHost   SettingsSection = "HOST"
)

// Specify returned errors for being re-used
var (
	errorNoSectionSpecified  = fmt.Errorf("no section specified")
	errorNoFilenameSpecified = fmt.Errorf("no filename specified")
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

// Setting represents one settings, which can be either a sting or a vector of strings
type Setting struct {
	isScalar   bool
	scalar     string
	vector     []string
	attributes map[string]string
}

// NewSettingScalar makes new scalar Setting
func NewSettingScalar(scalar string) *Setting {
	return &Setting{
		isScalar: true,
		scalar:   scalar,
	}
}

// NewSettingVector makes new vector Setting
func NewSettingVector(vector []string) *Setting {
	return &Setting{
		isScalar: false,
		vector:   vector,
	}
}

// IsScalar checks whether setting is a scalar value
func (s *Setting) IsScalar() bool {
	if s == nil {
		return false
	}
	return s.isScalar
}

// IsVector checks whether setting is a vector value
func (s *Setting) IsVector() bool {
	if s == nil {
		return false
	}
	return !s.isScalar
}

// Scalar gets scalar value of a setting
func (s *Setting) Scalar() string {
	if s == nil {
		return ""
	}
	return s.scalar
}

// Vector gets vector values of a setting
func (s *Setting) Vector() []string {
	if s == nil {
		return nil
	}
	return s.vector
}

// AsVector gets value of a setting as vector. Scalar value is casted to vector
func (s *Setting) AsVector() []string {
	if s == nil {
		return nil
	}
	if s.isScalar {
		return []string{
			s.scalar,
		}
	}
	return s.vector
}

// SetAttribute sets attribute of the setting
func (s *Setting) SetAttribute(name, value string) *Setting {
	if s == nil {
		return nil
	}
	if s.attributes == nil {
		s.attributes = make(map[string]string)
	}
	s.attributes[name] = value
	return s
}

// HasAttributes checks whether setting has attributes
func (s *Setting) HasAttributes() bool {
	if s == nil {
		return false
	}
	return len(s.attributes) > 0
}

// Attributes returns string form of attributes - used to config tag creation
func (s *Setting) Attributes() string {
	if s == nil {
		return ""
	}
	a := ""
	for name, value := range s.attributes {
		a += fmt.Sprintf(` %s="%s"`, name, value)
	}
	return a
}

// String gets string value of a setting. Vector is combined into one string
func (s *Setting) String() string {
	if s == nil {
		return ""
	}

	if s.isScalar {
		return s.scalar
	}

	return strings.Join(s.vector, ",")
}

// Settings specifies settings
type Settings struct {
	m map[string]*Setting
}

// NewSettings creates new settings
func NewSettings() *Settings {
	return &Settings{
		m: makeM(),
	}
}

func makeM() map[string]*Setting {
	return make(map[string]*Setting)
}

// Len gets length of the settings
func (settings *Settings) Len() int {
	if settings == nil {
		return 0
	}
	return len(settings.m)
}

// IsZero checks whether settings is zero
func (settings *Settings) IsZero() bool {
	if settings == nil {
		return true
	}
	return settings.Len() == 0
}

// Walk walks over settings
func (settings *Settings) Walk(f func(name string, setting *Setting)) {
	if settings == nil {
		return
	}
	if settings.Len() == 0 {
		return
	}
	for name := range settings.m {
		f(name, settings.Get(name))
	}
}

// Has checks whether named setting exists
func (settings *Settings) Has(name string) bool {
	if settings == nil {
		return false
	}
	if settings.Len() == 0 {
		return false
	}
	_, ok := settings.m[name]
	return ok
}

// Get gets named setting
func (settings *Settings) Get(name string) *Setting {
	if settings == nil {
		return nil
	}
	if settings.Len() == 0 {
		return nil
	}
	return settings.m[name]
}

// Set sets named setting
func (settings *Settings) Set(name string, setting *Setting) {
	if settings == nil {
		return
	}
	if settings == nil {
		return
	}
	// Lazy load
	if settings.m == nil {
		settings.m = makeM()
	}
	settings.m[name] = setting
}

// SetIfNotExists sets named setting
func (settings *Settings) SetIfNotExists(name string, setting *Setting) {
	if settings == nil {
		return
	}
	if !settings.Has(name) {
		settings.Set(name, setting)
	}
}

// Delete deletes named setting
func (settings *Settings) Delete(name string) {
	if settings == nil {
		return
	}
	if !settings.Has(name) {
		return
	}
	delete(settings.m, name)
}

// UnmarshalJSON unmarshal JSON
func (settings *Settings) UnmarshalJSON(data []byte) error {
	if settings == nil {
		return fmt.Errorf("unable to unmashal with nil")
	}
	type untypedMapType map[string]interface{}
	var untypedMap untypedMapType
	if err := json.Unmarshal(data, &untypedMap); err != nil {
		return err
	}

	if len(untypedMap) == 0 {
		return nil
	}

	for name, untyped := range untypedMap {
		if scalar, ok := unmarshalScalar(untyped); ok {
			settings.Set(name, NewSettingScalar(scalar))
		} else if vector, ok := unmarshalVector(untyped); ok {
			if len(vector) > 0 {
				settings.Set(name, NewSettingVector(vector))
			}
		}
	}

	return nil
}

// MarshalJSON marshals JSON
func (settings *Settings) MarshalJSON() ([]byte, error) {
	if settings == nil {
		return json.Marshal(nil)
	}

	raw := make(map[string]interface{})
	settings.Walk(func(name string, setting *Setting) {
		if setting.isScalar {
			raw[name] = setting.scalar
		} else {
			raw[name] = setting.vector
		}
	})

	return json.Marshal(raw)
}

// unmarshalScalar
func unmarshalScalar(untyped interface{}) (string, bool) {
	var res string
	var knownType bool

	typeOf := reflect.TypeOf(untyped)
	if typeOf == nil {
		// Unable to determine type of the value
		return "", false
	}

	switch untyped.(type) {
	case // scalar
		int, uint,
		int8, uint8,
		int16, uint16,
		int32, uint32,
		int64, uint64,
		bool,
		string:
		res = fmt.Sprintf("%v", untyped)
		knownType = true
	case // scalar
		float32:
		floatVal := untyped.(float32)
		_, frac := math.Modf(float64(floatVal))
		if frac > ignoreThreshold {
			// Consider it float
			res = fmt.Sprintf("%f", untyped)
		} else {
			// Consider it int
			intVal := int64(floatVal)
			res = fmt.Sprintf("%v", intVal)
		}
		knownType = true
	case // scalar
		float64:
		floatVal := untyped.(float64)
		_, frac := math.Modf(floatVal)
		if frac > ignoreThreshold {
			// Consider it float
			res = fmt.Sprintf("%f", untyped)
		} else {
			// Consider it int
			intVal := int64(floatVal)
			res = fmt.Sprintf("%v", intVal)
		}
		knownType = true
	}

	if knownType {
		return res, true
	}
	return "", false
}

// unmarshalVector
func unmarshalVector(untyped interface{}) ([]string, bool) {
	var res []string
	var knownType bool

	typeOf := reflect.TypeOf(untyped)
	if typeOf == nil {
		// Unable to determine type of the value
		return nil, false
	}

	switch untyped.(type) {
	case // vector
		[]interface{}:
		for _, _untyped := range untyped.([]interface{}) {
			if scalar, ok := unmarshalScalar(_untyped); ok {
				res = append(res, scalar)
			}
		}
		knownType = true
	}

	if knownType {
		return res, true
	}
	return nil, false
}

// getValueAsScalar
func (settings *Settings) getValueAsScalar(name string) (string, bool) {
	if !settings.Has(name) {
		return "", false
	}
	setting := settings.Get(name)
	if setting.IsScalar() {
		return setting.Scalar(), true
	}
	return "", false
}

// getValueAsVector
func (settings *Settings) getValueAsVector(name string) ([]string, bool) {
	if !settings.Has(name) {
		return nil, false
	}
	setting := settings.Get(name)
	if setting.IsScalar() {
		return nil, false
	}
	return setting.Vector(), true
}

// getValueAsInt
func (settings *Settings) getValueAsInt(name string) int {
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
func (settings *Settings) fetchPort(name string) int32 {
	return int32(settings.getValueAsInt(name))
}

// GetTCPPort gets TCP port from settings
func (settings *Settings) GetTCPPort() int32 {
	return settings.fetchPort("tcp_port")
}

// GetHTTPPort gets HTTP port from settings
func (settings *Settings) GetHTTPPort() int32 {
	return settings.fetchPort("http_port")
}

// GetInterserverHTTPPort gets interserver HTTP port from settings
func (settings *Settings) GetInterserverHTTPPort() int32 {
	return settings.fetchPort("interserver_http_port")
}

// MergeFrom merges into `dst` non-empty new-key-values from `src` in case no such `key` already in `src`
func (settings *Settings) MergeFrom(src *Settings) *Settings {
	if src.Len() == 0 {
		return settings
	}

	if settings == nil {
		settings = NewSettings()
	}

	src.Walk(func(key string, value *Setting) {
		settings.SetIfNotExists(key, value)
	})

	return settings
}

// MergeFromCB merges settings from src approved by filtering callback function
func (settings *Settings) MergeFromCB(src *Settings, filter func(path string, setting *Setting) bool) *Settings {
	if src.Len() == 0 {
		return settings
	}

	if settings == nil {
		settings = NewSettings()
	}

	src.Walk(func(key string, value *Setting) {
		if filter(key, value) {
			// Accept
			settings.Set(key, value)
		}
	})

	return settings
}

// GetSectionStringMap returns map of settings sections
func (settings *Settings) GetSectionStringMap(section SettingsSection, includeUnspecified bool) map[string]string {
	if settings == nil {
		return nil
	}

	m := make(map[string]string)

	settings.Walk(func(path string, _ *Setting) {
		_section, err := getSectionFromPath(path)
		if (err == nil) && (_section != section) {
			// This is not the section we are looking for, skip to next
			return
		}
		if (err != nil) && (err != errorNoSectionSpecified) {
			// We have a complex error, skip to next
			return
		}
		if (err == errorNoSectionSpecified) && !includeUnspecified {
			// We are not ready to include unspecified section, skip to next
			return
		}

		// We'd like to get this section

		filename, err := getFilenameFromPath(path)
		if err != nil {
			// We need to have filename specified
			return
		}

		if scalar, ok := settings.getValueAsScalar(path); ok {
			m[filename] = scalar
		} else {
			// Skip vector for now
		}
	})

	return m
}

// inArray checks whether needle is in haystack
func inArray(needle SettingsSection, haystack []SettingsSection) bool {
	for _, item := range haystack {
		if item == needle {
			return true
		}
	}
	return false
}

// Filter filters settings according to include and exclude lists
func (settings *Settings) Filter(
	includeSections []SettingsSection,
	excludeSections []SettingsSection,
	includeUnspecified bool,
) *Settings {
	res := NewSettings()

	if settings.Len() == 0 {
		return res
	}

	settings.Walk(func(path string, _ *Setting) {
		_section, err := getSectionFromPath(path)

		var include bool
		var exclude bool

		if err == nil {
			include = (includeSections == nil) || inArray(_section, includeSections)
			exclude = (excludeSections != nil) && inArray(_section, excludeSections)
		}

		include = include && !exclude

		if (err == nil) && !include {
			// This is not the section we are looking for, skip to next
			return
		}
		if (err != nil) && (err != errorNoSectionSpecified) {
			// We have a complex error, skip to next
			return
		}
		if (err == errorNoSectionSpecified) && !includeUnspecified {
			// We are not ready to include unspecified section, skip to next
			return
		}

		// We'd like to get this section
		res.Set(path, settings.Get(path))
	})

	return res
}

// AsSortedSliceOfStrings return settings as sorted strings
func (settings *Settings) AsSortedSliceOfStrings() []string {
	if settings == nil {
		return nil
	}

	// Sort keys
	var keys []string
	settings.Walk(func(key string, _ *Setting) {
		keys = append(keys, key)
	})
	sort.Strings(keys)

	var res []string

	// Walk over sorted keys
	for _, key := range keys {
		res = append(res, key)
		res = append(res, settings.Get(key).String())
	}

	return res
}

// Normalize normalizes settings
func (settings *Settings) Normalize() {
	settings.normalizePaths()
}

// normalizePaths normalizes paths in settings
func (settings *Settings) normalizePaths() {
	if settings.Len() == 0 {
		return
	}

	pathsToNormalize := make([]string, 0, 0)

	// Find entries with paths to normalize
	settings.Walk(func(unNormalizedPath string, _ *Setting) {
		normalizedPath := normalizeSettingsKeyAsPath(unNormalizedPath)
		if len(normalizedPath) != len(unNormalizedPath) {
			// Normalization changed something. This path has to be normalized
			pathsToNormalize = append(pathsToNormalize, unNormalizedPath)
		}
	})

	// Add entries with normalized paths
	for _, unNormalizedPath := range pathsToNormalize {
		normalizedPath := normalizeSettingsKeyAsPath(unNormalizedPath)
		settings.Set(normalizedPath, settings.Get(unNormalizedPath))
	}

	// Delete entries with un-normalized paths
	for _, unNormalizedPath := range pathsToNormalize {
		settings.Delete(unNormalizedPath)
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

// getSectionFromPath
func getSectionFromPath(path string) (SettingsSection, error) {
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		// We need to have path to be at least section/file.name
		return SectionEmpty, errorNoSectionSpecified
	}

	section := parts[0]
	return string2Section(section)
}

// string2Section
func string2Section(section string) (SettingsSection, error) {
	if strings.EqualFold(section, string(SectionCommon)) || strings.EqualFold(section, CommonConfigDir) {
		return SectionCommon, nil
	}
	if strings.EqualFold(section, string(SectionUsers)) || strings.EqualFold(section, UsersConfigDir) {
		return SectionUsers, nil
	}
	if strings.EqualFold(section, string(SectionHost)) || strings.EqualFold(section, HostConfigDir) {
		return SectionHost, nil
	}

	return SectionEmpty, fmt.Errorf("unknown section specified %v", section)
}

// getFilenameFromPath
func getFilenameFromPath(path string) (string, error) {
	parts := strings.Split(path, "/")
	if len(parts) < 1 {
		// We need to have path to be at least one entry - which will be 'filename'
		return "", errorNoFilenameSpecified
	}

	// Extract last component from path
	filename := parts[len(parts)-1]
	if filename == "" {
		// We need to have path to be at least one entry - which will be 'filename'
		return "", errorNoFilenameSpecified
	}

	return filename, nil
}
