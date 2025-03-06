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

package types

import (
	"encoding/json"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type Tag string
type Tags map[Tag]int

// NewTags creates new variable
func NewTags() map[Tag]int {
	return make(map[Tag]int)
}

func (t Tags) Set(tag Tag, value int) Tags {
	if t == nil {
		return nil
	}
	t[tag] = value

	return t
}

func (t Tags) UnSet(tag Tag) Tags {
	util.MapDeleteKeys(t, tag)
	return t
}

func (t Tags) Get(tag Tag)int {
	if t == nil {
		return 0
	}
	if value, ok := t[tag]; ok {
		return value
	}
	return 0
}


func (t Tags) Has(tag Tag) bool {
	return util.MapHasKeys(t, tag)
}

func (t Tags) Equal(b Tags) bool {
	return util.MapsAreTheSame(t, b)
}

func (t Tags) HaveIntersection(b Tags) bool {
	return util.MapsHaveKeyValuePairsIntersection(t, b)
}

// String casts to a string
func (t Tags) String() string {
	if t == nil {
		return ""
	}
	b, _ := json.Marshal(t)
	return string(b)
}
