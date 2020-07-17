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
	// Configuration sections
	// Each section translates into separate ConfigMap mapped into Pod
	SectionEmpty  SettingsSection = ""
	SectionCommon SettingsSection = "COMMON"
	SectionUsers  SettingsSection = "USERS"
	SectionHost   SettingsSection = "HOST"

	// Specify returned errors for being re-used
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
	isScalar bool
	scalar   string
	vector   []string
}

// NewScalarSetting makes new scalar Setting
func NewScalarSetting(scalar string) *Setting {
	return &Setting{
		isScalar: true,
		scalar:   scalar,
	}
}

// NewVectorSetting makles new vector Setting
func NewVectorSetting(vector []string) *Setting {
	return &Setting{
		isScalar: false,
		vector:   vector,
	}
}

// IsScalar checks whether setting is a scalar value
func (s *Setting) IsScalar() bool {
	return s.isScalar
}

// IsVector checks whether setting is a vector value
func (s *Setting) IsVector() bool {
	return !s.isScalar
}

// Scalar gets scalar value of a setting
func (s *Setting) Scalar() string {
	return s.scalar
}

// Vector gets vector values of a setting
func (s *Setting) Vector() []string {
	return s.vector
}

// AsVector gets value of a setting as vector. Scalar value is casted to vector
func (s *Setting) AsVector() []string {
	if s.isScalar {
		return []string{
			s.scalar,
		}
	}
	return s.vector
}

// String gets string value of a setting. Vector is combined into one string
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
	var res string
	var knownType bool

	typeOf := reflect.TypeOf(untyped)
	if typeOf == nil {
		// Unable to determine type of the value
		log.V(3).Infof("unmarshalScalar() typeOf==nil")
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

	str := typeOf.String()
	if knownType {
		log.V(3).Infof("unmarshalScalar() type=%v value=%s", str, res)
		return res, true
	} else {
		log.V(3).Infof("unmarshalScalar() type=%v - UNABLE to unmarshal", str)
		return "", false
	}
}

// unmarshalVector
func unmarshalVector(untyped interface{}) ([]string, bool) {
	var res []string
	var knownType bool

	typeOf := reflect.TypeOf(untyped)
	if typeOf == nil {
		// Unable to determine type of the value
		log.V(3).Infof("unmarshalVector() typeOf==nil")
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

	str := typeOf.String()
	if knownType {
		log.V(3).Infof("unmarshalVector() type=%v value=%s", str, res)
		return res, true
	} else {
		log.V(3).Infof("unmarshalVector type=%v - UNABLE to unmarshal", str)
		return nil, false
	}
}

// getValueAsScalar
func (settings Settings) getValueAsScalar(name string) (string, bool) {
	setting, ok := settings[name]
	if !ok {
		// Unknown setting
		return "", false
	}
	if setting.IsScalar() {
		return setting.Scalar(), true
	}
	return "", false
}

// getValueAsVector
func (settings Settings) getValueAsVector(name string) ([]string, bool) {
	setting, ok := settings[name]
	if !ok {
		// Unknown setting
		return nil, false
	}
	if setting.IsScalar() {
		return nil, false
	}
	return setting.Vector(), true
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

// MergeFromCB merges settings from src approved by filtering callback function
func (settings *Settings) MergeFromCB(src Settings, filter func(path string, setting *Setting) bool) {
	if src == nil {
		return
	}

	if *settings == nil {
		*settings = NewSettings()
	}

	for key, value := range src {
		if filter(key, value) {
			// Accept
			(*settings)[key] = value
		}
	}
}

// GetSectionStringMap
func (settings Settings) GetSectionStringMap(section SettingsSection, includeUnspecified bool) map[string]string {
	m := make(map[string]string)

	for path := range settings {
		_section, err := getSectionFromPath(path)
		if (err == nil) && (_section != section) {
			// This is not the section we are looking for, skip to next
			continue // for
		}
		if (err != nil) && (err != errorNoSectionSpecified) {
			// We have a complex error, skip to next
			continue // for
		}
		if (err == errorNoSectionSpecified) && !includeUnspecified {
			// We are not ready to include unspecified section, skip to next
			continue // for
		}

		// We'd like to get this section

		filename, err := getFilenameFromPath(path)
		if err != nil {
			// We need to have filename specified
			continue // for
		}

		if scalar, ok := settings.getValueAsScalar(path); ok {
			m[filename] = scalar
		} else {
			// Skip vector for now
		}
	}

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
func (settings Settings) Filter(
	includeSections []SettingsSection,
	excludeSections []SettingsSection,
	includeUnspecified bool,
) Settings {
	res := make(Settings)

	for path := range settings {
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
			continue // for
		}
		if (err != nil) && (err != errorNoSectionSpecified) {
			// We have a complex error, skip to next
			continue // for
		}
		if (err == errorNoSectionSpecified) && !includeUnspecified {
			// We are not ready to include unspecified section, skip to next
			continue // for
		}

		// We'd like to get this section

		res[path] = settings[path]
	}

	return res
}

// AsSortedSliceOfStrings
func (settings Settings) AsSortedSliceOfStrings() []string {
	// Sort keys
	var keys []string
	for key := range settings {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var res []string

	// Walk over sorted keys
	for _, key := range keys {
		res = append(res, key)
		res = append(res, settings[key].String())
	}

	return res
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
	return string2Section(section)
}

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

	log.V(1).Infof("unknown section specified %v", section)

	return SectionEmpty, fmt.Errorf("unknown section specified %v", section)
}

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
