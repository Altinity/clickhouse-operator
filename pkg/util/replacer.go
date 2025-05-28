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

import "strings"

type Replacer struct {
	macroToExpansionMap map[string]string
	stringReplacer      *strings.Replacer
	mapReplacer         *MapReplacer
}

// NewReplacer
func NewReplacer(macroToExpansionMap ...map[string]string) *Replacer {
	r := &Replacer{
		macroToExpansionMap: make(map[string]string),
	}

	if len(macroToExpansionMap) > 0 {
		r.macroToExpansionMap = macroToExpansionMap[0]
	}

	var replacements []string
	for macro, expansion := range r.macroToExpansionMap {
		replacements = append(replacements, macro, expansion)
	}

	r.stringReplacer = strings.NewReplacer(replacements...)
	r.mapReplacer = NewMapReplacer(r.stringReplacer)
	return r
}

// Line expands line with macros(es)
func (e *Replacer) Line(line string) string {
	if e == nil {
		// No replacement
		return line
	}
	return e.stringReplacer.Replace(line)
}

// LineEx expands line with macros(es)
func (e *Replacer) LineEx(line string) (string, bool) {
	res := e.Line(line)
	return res, res != line
}

// Map expands map with macros(es)
func (e *Replacer) Map(_map map[string]string) map[string]string {
	if e == nil {
		// No replacement
		return _map
	}
	return e.mapReplacer.Replace(_map)
}

// MapEx expands map with macros(es)
func (e *Replacer) MapEx(_map map[string]string) (map[string]string, bool) {
	return e.mapReplacer.ReplaceEx(_map)
}

// MapReplacer replaces a list of strings with replacements on a map.
type MapReplacer struct {
	*strings.Replacer
}

// NewMapReplacer creates new MapReplacer
func NewMapReplacer(r *strings.Replacer) *MapReplacer {
	return &MapReplacer{
		r,
	}
}

// Replace returns a copy of m with all replacements performed.
func (r *MapReplacer) Replace(m map[string]string) map[string]string {
	if r == nil {
		// No replacement
		return m
	}
	if len(m) == 0 {
		// Nothing to replace
		return m
	}
	result := make(map[string]string, len(m))
	for key := range m {
		result[r.Replacer.Replace(key)] = r.Replacer.Replace(m[key])
	}
	return result
}

// Replace returns a copy of m with all replacements performed.
func (r *MapReplacer) ReplaceEx(m map[string]string) (map[string]string, bool) {
	if r == nil {
		// No replacement
		return m, false
	}
	if len(m) == 0 {
		// Nothing to replace
		return m, false
	}
	result := make(map[string]string, len(m))
	modified := false
	for key, value := range m {
		resultKey := r.Replacer.Replace(key)
		resultValue := r.Replacer.Replace(value)
		result[resultKey] = resultValue
		modifiedKey := key != resultKey
		modifiedValue := value != resultValue
		modified = modified || modifiedKey || modifiedValue
	}
	return result, modified
}
