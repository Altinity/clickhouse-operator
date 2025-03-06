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
	"fmt"
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
	if t == nil {
		return nil
	}
	delete(t, tag)
	return t
}


func (t Tags) Has(tag Tag) bool {
	if t == nil {
		return false
	}
	_, ok := t[tag]
	return ok
}

// String casts to a string
func (t Tags) String() string {
	if t == nil {
		return ""
	}
	return fmt.Sprintf("%v", t)
}
