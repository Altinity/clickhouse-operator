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

// FoldEnum returns the canonical-cased member of canonical that case-insensitively
// matches value, so callers can accept both humped and all-lowercase enum input and
// then compare downstream with plain ==. If value matches no canonical member it is
// returned unchanged (the caller's default/validation handles unrecognized values).
func FoldEnum(value string, canonical ...string) string {
	for _, c := range canonical {
		if strings.EqualFold(value, c) {
			return c
		}
	}
	return value
}
