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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFoldEnum verifies that any accepted casing folds to the canonical member,
// while empty and unrecognized inputs pass through unchanged.
func TestFoldEnum(t *testing.T) {
	canonical := []string{"Abort", "Delete", "Ignore"}

	tests := []struct {
		name  string
		value string
		want  string
	}{
		{"exact canonical preserved", "Abort", "Abort"},
		{"all-lowercase folds up", "abort", "Abort"},
		{"all-uppercase folds", "DELETE", "Delete"},
		{"mixed case folds", "iGnOrE", "Ignore"},
		{"empty passes through", "", ""},
		{"unrecognized passes through unchanged", "bogus", "bogus"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, FoldEnum(tc.value, canonical...))
		})
	}
}

// TestFoldEnumNoCandidates verifies the value is returned unchanged when no
// canonical members are supplied (defensive — never panics on empty varargs).
func TestFoldEnumNoCandidates(t *testing.T) {
	require.Equal(t, "anything", FoldEnum("anything"))
}
