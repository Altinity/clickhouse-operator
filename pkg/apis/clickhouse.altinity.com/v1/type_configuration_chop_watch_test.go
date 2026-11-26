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

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
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

const testShardKey = "example.com/clickhouse-shard"

// An absent/empty selector must behave exactly like the pre-patch operator:
// every CR matches, regardless of its labels.
func TestWatchLabelSelectorBackwardCompat(t *testing.T) {
	labelStates := []map[string]string{
		nil,
		{},
		{testShardKey: "stg"},
		{testShardKey: "logs"},
		{"unrelated": "value"},
	}

	for _, selector := range []string{"", "   "} {
		config := &OperatorConfig{}
		config.Watch.LabelSelector = selector
		if err := config.ParseWatchLabelSelector(); err != nil {
			t.Fatalf("ParseWatchLabelSelector(%q) unexpected error: %v", selector, err)
		}
		if config.HasWatchLabelSelector() {
			t.Errorf("HasWatchLabelSelector() = true for empty selector %q, want false", selector)
		}
		for _, lbls := range labelStates {
			if !config.IsLabelSelectorWatched(lbls) {
				t.Errorf("IsLabelSelectorWatched(%v) = false with empty selector, want true (backward compat)", lbls)
			}
			if !config.IsCRWatched("any-namespace", lbls) {
				t.Errorf("IsCRWatched(any-namespace, %v) = false with empty selector, want true", lbls)
			}
		}
	}
}

func TestParseWatchLabelSelectorValid(t *testing.T) {
	tests := []struct {
		selector string
		match    map[string]string
		noMatch  map[string]string
	}{
		{
			selector: testShardKey + "=stg",
			match:    map[string]string{testShardKey: "stg"},
			noMatch:  map[string]string{testShardKey: "logs"},
		},
		{
			selector: "!" + testShardKey,
			match:    map[string]string{"unrelated": "value"},
			noMatch:  map[string]string{testShardKey: "stg"},
		},
		{
			selector: testShardKey + " in (logs,ads)",
			match:    map[string]string{testShardKey: "ads"},
			noMatch:  map[string]string{testShardKey: "stg"},
		},
		{
			selector: testShardKey + "=stg,env=prod",
			match:    map[string]string{testShardKey: "stg", "env": "prod"},
			noMatch:  map[string]string{testShardKey: "stg"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.selector, func(t *testing.T) {
			config := &OperatorConfig{}
			config.Watch.LabelSelector = tt.selector
			if err := config.ParseWatchLabelSelector(); err != nil {
				t.Fatalf("ParseWatchLabelSelector(%q) unexpected error: %v", tt.selector, err)
			}
			if !config.HasWatchLabelSelector() {
				t.Fatalf("HasWatchLabelSelector() = false after parsing %q, want true", tt.selector)
			}
			if !config.IsLabelSelectorWatched(tt.match) {
				t.Errorf("IsLabelSelectorWatched(%v) = false under %q, want true", tt.match, tt.selector)
			}
			if config.IsLabelSelectorWatched(tt.noMatch) {
				t.Errorf("IsLabelSelectorWatched(%v) = true under %q, want false", tt.noMatch, tt.selector)
			}
			// Unlabeled CRs must never match an equality/set selector
			if tt.selector != "!"+testShardKey {
				if config.IsLabelSelectorWatched(nil) {
					t.Errorf("IsLabelSelectorWatched(nil) = true under %q, want false", tt.selector)
				}
			}
		})
	}
}

// An invalid selector must surface as a parse error (startup turns this into log.Fatalf),
// never silently fall back to match-all or match-none.
func TestParseWatchLabelSelectorInvalid(t *testing.T) {
	invalid := []string{
		testShardKey + "===stg",
		testShardKey + " in (",
		"!!" + testShardKey,
		testShardKey + "=val;ue",
		"=stg",
	}
	for _, selector := range invalid {
		t.Run(selector, func(t *testing.T) {
			config := &OperatorConfig{}
			config.Watch.LabelSelector = selector
			if err := config.ParseWatchLabelSelector(); err == nil {
				t.Errorf("ParseWatchLabelSelector(%q) = nil error, want parse failure (fail-fast)", selector)
			}
			if config.HasWatchLabelSelector() {
				t.Errorf("HasWatchLabelSelector() = true after failed parse of %q, want false", selector)
			}
		})
	}
}

func TestWatchLabelSelectorEnvVarOverride(t *testing.T) {
	t.Setenv(deployment.WATCH_LABEL_SELECTOR, testShardKey+"=stg")

	config := &OperatorConfig{}
	config.Watch.LabelSelector = "!" + testShardKey // file-based value, env must win
	config.applyEnvVarParams()

	if config.Watch.LabelSelector != testShardKey+"=stg" {
		t.Fatalf("WATCH_LABEL_SELECTOR env var did not override file config: got %q", config.Watch.LabelSelector)
	}
	if err := config.ParseWatchLabelSelector(); err != nil {
		t.Fatalf("ParseWatchLabelSelector() unexpected error: %v", err)
	}
	if !config.IsLabelSelectorWatched(map[string]string{testShardKey: "stg"}) {
		t.Error("env-var-provided selector not effective")
	}
}

func TestWatchLabelSelectorEnvVarAbsentKeepsFileValue(t *testing.T) {
	t.Setenv(deployment.WATCH_LABEL_SELECTOR, "")

	config := &OperatorConfig{}
	config.Watch.LabelSelector = "!" + testShardKey
	config.applyEnvVarParams()

	if config.Watch.LabelSelector != "!"+testShardKey {
		t.Fatalf("absent WATCH_LABEL_SELECTOR must keep file config, got %q", config.Watch.LabelSelector)
	}
}

// For the production scheme (shards use `example.com/clickhouse-shard=<shard>`, legacy uses
// `!example.com/clickhouse-shard`) every possible label state matches exactly one operator.
func TestWatchLabelSelectorDisjointness(t *testing.T) {
	selectors := []string{
		testShardKey + "=stg",
		testShardKey + "=logs",
		testShardKey + "=ads",
		"!" + testShardKey,
	}
	labelStates := []map[string]string{
		nil,
		{},
		{testShardKey: "stg"},
		{testShardKey: "logs"},
		{testShardKey: "ads"},
		{"unrelated": "value"},
		{testShardKey: "stg", "unrelated": "value"},
	}

	countMatches := func(lbls map[string]string) int {
		matches := 0
		for _, selector := range selectors {
			config := &OperatorConfig{}
			config.Watch.LabelSelector = selector
			if err := config.ParseWatchLabelSelector(); err != nil {
				t.Fatalf("ParseWatchLabelSelector(%q) unexpected error: %v", selector, err)
			}
			if config.IsLabelSelectorWatched(lbls) {
				matches++
			}
		}
		return matches
	}

	for _, lbls := range labelStates {
		if matches := countMatches(lbls); matches != 1 {
			t.Errorf("label state %v matched %d selectors, want exactly 1 (disjointness violated)", lbls, matches)
		}
	}

	// Documented gap: an unknown shard value matches ZERO operators (orphaned CHI).
	// Prevented upstream by the charts CI allowlist, not by the operator.
	if matches := countMatches(map[string]string{testShardKey: "no-such-shard"}); matches != 0 {
		t.Errorf("unknown shard value matched %d selectors, want 0", matches)
	}
}

// requireLabelSelector converts a missing selector on a sharded operator into a startup error.
func TestValidateWatchLabelSelectorRequire(t *testing.T) {
	tests := []struct {
		name     string
		selector string
		require  bool
		wantErr  bool
	}{
		{"require + selector set", testShardKey + "=stg", true, false},
		{"require + selector empty", "", true, true},
		{"no require + selector empty", "", false, false},
		{"require + invalid selector", "=stg", true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &OperatorConfig{}
			config.Watch.LabelSelector = tt.selector
			config.Watch.RequireLabelSelector = tt.require
			if err := config.ValidateWatchLabelSelector(); (err != nil) != tt.wantErr {
				t.Errorf("ValidateWatchLabelSelector() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWatchLabelSelectorRequiredEnvVar(t *testing.T) {
	for _, val := range []string{"true", "1"} {
		t.Setenv(deployment.WATCH_LABEL_SELECTOR_REQUIRED, val)
		config := &OperatorConfig{}
		config.applyEnvVarParams()
		if !config.Watch.RequireLabelSelector {
			t.Errorf("WATCH_LABEL_SELECTOR_REQUIRED=%q did not set RequireLabelSelector", val)
		}
	}

	t.Setenv(deployment.WATCH_LABEL_SELECTOR_REQUIRED, "false")
	config := &OperatorConfig{}
	config.Watch.RequireLabelSelector = true
	config.applyEnvVarParams()
	if config.Watch.RequireLabelSelector {
		t.Error("WATCH_LABEL_SELECTOR_REQUIRED=false did not clear RequireLabelSelector")
	}
}

// A ClickHouseOperatorConfiguration CR is merged by every operator in the namespace, so it
// must never be able to set or change an instance's selector identity.
func TestMergeFromIgnoresCRLabelSelector(t *testing.T) {
	tests := []struct {
		name         string
		base         string
		fromSelector string
		want         string
	}{
		{"CR cannot override file selector", testShardKey + "=stg", testShardKey + "=logs", testShardKey + "=stg"},
		{"CR cannot set selector on catch-all", "", testShardKey + "=logs", ""},
		{"CR without selector leaves file selector", testShardKey + "=stg", "", testShardKey + "=stg"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := &OperatorConfig{}
			base.Watch.LabelSelector = tt.base
			base.Watch.RequireLabelSelector = true
			from := &OperatorConfig{}
			from.Watch.LabelSelector = tt.fromSelector
			if err := base.MergeFrom(from); err != nil {
				t.Fatalf("MergeFrom() unexpected error: %v", err)
			}
			if base.Watch.LabelSelector != tt.want {
				t.Errorf("LabelSelector after merge = %q, want %q", base.Watch.LabelSelector, tt.want)
			}
			if !base.Watch.RequireLabelSelector {
				t.Error("RequireLabelSelector lost across merge")
			}
		})
	}
}

// Every key referenced by watch.labelSelector must land in label.exclude so ownership
// labels never propagate to child objects (a propagated shard label would turn any
// re-shard label flip into a pod-template change and a rolling restart).
func TestWatchLabelSelectorKeysExcludedFromPropagation(t *testing.T) {
	tests := []struct {
		name            string
		selector        string
		existingExclude []string
		wantExclude     []string
	}{
		{"equality selector", testShardKey + "=stg", nil, []string{testShardKey}},
		{"negation selector", "!" + testShardKey, nil, []string{testShardKey}},
		{"set selector", testShardKey + " in (logs,ads)", nil, []string{testShardKey}},
		{"multi-key selector", testShardKey + "=stg,env=prod", nil, []string{testShardKey, "env"}},
		{"existing excludes preserved", testShardKey + "=stg", []string{"team"}, []string{"team", testShardKey}},
		{"no duplicate append", testShardKey + "=stg", []string{testShardKey}, []string{testShardKey}},
		{"empty selector leaves excludes alone", "", []string{"team"}, []string{"team"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &OperatorConfig{}
			config.Watch.LabelSelector = tt.selector
			config.Label.Exclude = tt.existingExclude
			if err := config.ParseWatchLabelSelector(); err != nil {
				t.Fatalf("ParseWatchLabelSelector(%q) unexpected error: %v", tt.selector, err)
			}
			config.excludeWatchLabelSelectorKeysFromPropagation()

			if len(config.Label.Exclude) != len(tt.wantExclude) {
				t.Fatalf("Label.Exclude = %v, want %v", config.Label.Exclude, tt.wantExclude)
			}
			for _, key := range tt.wantExclude {
				found := false
				for _, have := range config.Label.Exclude {
					if have == key {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Label.Exclude = %v, missing key %q", config.Label.Exclude, key)
				}
			}
		})
	}
}

// The exclusion must survive repeated normalization without growing the list.
func TestWatchLabelSelectorKeysExclusionIdempotent(t *testing.T) {
	config := &OperatorConfig{}
	config.Watch.LabelSelector = testShardKey + "=stg"
	if err := config.ParseWatchLabelSelector(); err != nil {
		t.Fatalf("ParseWatchLabelSelector() unexpected error: %v", err)
	}
	config.excludeWatchLabelSelectorKeysFromPropagation()
	config.excludeWatchLabelSelectorKeysFromPropagation()
	if len(config.Label.Exclude) != 1 || config.Label.Exclude[0] != testShardKey {
		t.Errorf("Label.Exclude after double apply = %v, want [%s]", config.Label.Exclude, testShardKey)
	}
}

// IsCRWatched must be the conjunction of the namespace filter and the label selector filter.
func TestIsCRWatched(t *testing.T) {
	config := &OperatorConfig{}
	config.Watch.LabelSelector = testShardKey + "=stg"
	if err := config.ParseWatchLabelSelector(); err != nil {
		t.Fatalf("ParseWatchLabelSelector() unexpected error: %v", err)
	}
	config.Watch.Namespaces.Exclude = types.NewStrings([]string{"denied-ns"})

	tests := []struct {
		namespace string
		lbls      map[string]string
		want      bool
	}{
		{"clickhouse", map[string]string{testShardKey: "stg"}, true},
		{"clickhouse", map[string]string{testShardKey: "logs"}, false},
		{"clickhouse", nil, false},
		{"denied-ns", map[string]string{testShardKey: "stg"}, false},
	}
	for _, tt := range tests {
		if got := config.IsCRWatched(tt.namespace, tt.lbls); got != tt.want {
			t.Errorf("IsCRWatched(%q, %v) = %v, want %v", tt.namespace, tt.lbls, got, tt.want)
		}
	}
}
