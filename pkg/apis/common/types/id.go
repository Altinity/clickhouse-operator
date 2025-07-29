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
	"strings"

	"github.com/google/uuid"
)

// Id defines id representation with possibility to be optional
type Id String

const autoPrefix = "auto-"

// NewId creates new variable
func NewId() *Id {
	return (*Id)(NewString(uuid.New().String()))
}

func NewAutoId() *Id {
	return (*Id)(NewString(autoPrefix + uuid.New().String()))
}

// HasValue checks whether value is specified
func (id *Id) HasValue() bool {
	return len(id.Value()) > 0
}

// Value returns value
func (id *Id) Value() string {
	return (*String)(id).Value()
}

// String casts to a string
func (id *Id) String() string {
	return id.Value()
}

// IsAutoId checks whether id is an auto-id
func (id *Id) IsAutoId() bool {
	return strings.HasPrefix(id.Value(), autoPrefix)
}
