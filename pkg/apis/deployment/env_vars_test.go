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

package deployment

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDefaultCollectorsDisabled pins the opt-out semantics: only a value that
// strconv.ParseBool understands as true may remove the collectors, so an unset
// or malformed value leaves the long-standing default behaviour untouched.
func TestDefaultCollectorsDisabled(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected bool
	}{
		{name: "unset", value: "", expected: false},
		{name: "false", value: "false", expected: false},
		{name: "zero", value: "0", expected: false},
		{name: "true", value: "true", expected: true},
		{name: "one", value: "1", expected: true},
		{name: "malformed", value: "yes", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(OPERATOR_METRICS_DISABLE_DEFAULT_COLLECTORS, tt.value)
			require.Equal(t, tt.expected, DefaultCollectorsDisabled())
		})
	}
}
