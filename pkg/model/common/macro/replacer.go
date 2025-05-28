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

package macro

import (
	"strings"

	"github.com/altinity/clickhouse-operator/pkg/util"
)

type Replacer struct {
	macroToExpansionMap map[string]string
	stringReplacer *strings.Replacer
	mapReplacer *util.MapReplacer
}

// New
func NewReplacer(macroToExpansionMap ...map[string]string) *Replacer {
	r :=  &Replacer{
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
	r.mapReplacer = util.NewMapReplacer(r.stringReplacer)
	return r
}

// Line expands line with macros(es)
func (e *Replacer) Line(line string) string {
	return e.stringReplacer.Replace(line)
}

// Map expands map with macros(es)
func (e *Replacer) Map(_map map[string]string) map[string]string {
	return e.mapReplacer.Replace(_map)
}
