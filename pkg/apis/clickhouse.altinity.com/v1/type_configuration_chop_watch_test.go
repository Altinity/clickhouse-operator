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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
)

// TestApplyEnvVarParamsWatchNamespaces verifies that WATCH_NAMESPACES (wired by the OLM CSV
// to the OperatorGroup's olm.targetNamespaces annotation) maps every advertised OLM install
// mode to the right watch set. The decisive case is AllNamespaces: OLM sets the var to an
// empty string, which must mean "watch all namespaces", not "watch own namespace".
func TestApplyEnvVarParamsWatchNamespaces(t *testing.T) {
	tests := []struct {
		name     string // OLM install mode under test
		value    string // WATCH_NAMESPACES as OLM sets it from olm.targetNamespaces
		expected []string
	}{
		{"OwnNamespace", "openshift-operators", []string{"openshift-operators"}},
		{"SingleNamespace", "team-a", []string{"team-a"}},
		{"MultiNamespace comma", "team-a,team-b", []string{"team-a", "team-b"}},
		{"MultiNamespace colon", "team-a:team-b", []string{"team-a", "team-b"}},
		{"AllNamespaces empty -> watch all", "", []string{".*"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(deployment.WATCH_NAMESPACES, tt.value)

			c := &OperatorConfig{}
			c.applyEnvVarParams()

			// ElementsMatch, not Equal: the include set is order-insensitive (NewStrings
			// dedups via a map), and watch.namespaces is consumed as a set downstream.
			require.ElementsMatch(t, tt.expected, c.Watch.Namespaces.Include.Value(),
				"WATCH_NAMESPACES=%q", tt.value)
		})
	}
}
