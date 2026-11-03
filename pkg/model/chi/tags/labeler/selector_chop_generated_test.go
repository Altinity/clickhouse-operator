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

package labeler

import (
	"testing"

	"github.com/altinity/clickhouse-operator/pkg/interfaces"

	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sLabels "k8s.io/apimachinery/pkg/labels"

	commonLabeler "github.com/altinity/clickhouse-operator/pkg/model/common/tags/labeler"
)

// TestCHOPGeneratedSelectorMatchesPredicate is the invariant the server-side informer filter rests
// on. CHOPGeneratedSelector narrows an informer's List and Watch at the API server;
// IsCHOPGeneratedObject decides client-side whether a delivered event is ours. If the two ever
// disagree, the informer silently stops delivering objects the operator still wants - no error, no
// log, just an operator that stops reacting to part of its own fleet.
//
// With a single-key selector the two are trivially the same, so this does not catch today's
// refactor - it catches the divergence a second key would introduce, which is exactly when the
// mistake stops being obvious.
func TestCHOPGeneratedSelectorMatchesPredicate(t *testing.T) {
	l := New(nil)
	selector := k8sLabels.SelectorFromSet(l.CHOPGeneratedSelector())

	appKey := l.Get(commonLabeler.LabelAppName)
	appValue := l.Get(commonLabeler.LabelAppValue)

	tests := []struct {
		name     string
		labels   map[string]string
		expected bool
	}{
		{name: "nil labels", labels: nil, expected: false},
		{name: "empty labels", labels: map[string]string{}, expected: false},
		{name: "exact match", labels: map[string]string{appKey: appValue}, expected: true},
		{
			name:     "match plus unrelated labels - extras must not disqualify",
			labels:   map[string]string{appKey: appValue, "team": "data", "env": "prod"},
			expected: true,
		},
		{name: "right key, wrong value", labels: map[string]string{appKey: "not-chop"}, expected: false},
		{
			name:     "bare app key without the API group prefix",
			labels:   map[string]string{"app": appValue},
			expected: false,
		},
		{
			name:     "the CHK key - the two flavours must not match each other",
			labels:   map[string]string{"clickhouse-keeper.altinity.com/app": appValue},
			expected: false,
		},
		{name: "empty value", labels: map[string]string{appKey: ""}, expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			object := &meta.ObjectMeta{Labels: tt.labels}

			selectorMatches := selector.Matches(k8sLabels.Set(tt.labels))
			predicateMatches := l.IsCHOPGeneratedObject(object)

			require.Equal(t, tt.expected, selectorMatches,
				"server-side selector disagrees - a narrowed List/Watch would drop objects the "+
					"client-side predicate accepts")
			require.Equal(t, tt.expected, predicateMatches, "client-side predicate")
		})
	}
}

// The selector must carry the app label and nothing else. A namespace or CR-name key would make it
// CR-specific, and it is used to select across every CR the operator manages - from a labeler built
// with no CR at all, at operator startup, before any CR is in hand.
func TestCHOPGeneratedSelectorIsCRIndependent(t *testing.T) {
	l := New(nil)
	set := l.CHOPGeneratedSelector()

	require.Len(t, set, 1, "selector must constrain exactly one label")
	require.Equal(t, l.Get(commonLabeler.LabelAppValue), set[l.Get(commonLabeler.LabelAppName)])
}

// The rendered string is what actually travels to the API server as ?labelSelector=. Pinning it
// catches a label-key change that the equivalence test above would happily accept, because that
// test moves both sides together.
func TestCHOPGeneratedSelectorRendersExpectedString(t *testing.T) {
	rendered := k8sLabels.SelectorFromSet(New(nil).CHOPGeneratedSelector()).String()

	require.Equal(t, "clickhouse.altinity.com/app=chop", rendered)
}

// TestCHOPGeneratedSelectorIsImpliedByScopedSelectors is the invariant behind the CHI server-side
// informer narrowing: those informers List and Watch with CHOPGeneratedSelector, so any scoped
// selector the operator applies to what they deliver must already constrain the same app label -
// otherwise a scope asks for objects the informer was never told to fetch, and the operator goes
// quiet on part of its own fleet with no error anywhere.
//
// Unlike CHK there is no controller-runtime cache here: CHI reads reach the API server through a
// clientset, so this is about event delivery rather than a Get answering IsNotFound.
//
// Only the CR scopes are covered: cluster, shard and host scopes take the object they scope to as
// an argument, which a CR-less labeler cannot supply. The host scope is pinned indirectly instead,
// as the set the StatefulSet's spec.selector is built from.
func TestCHOPGeneratedSelectorIsImpliedByScopedSelectors(t *testing.T) {
	l := New(nil)
	narrowing := k8sLabels.SelectorFromSet(l.CHOPGeneratedSelector())

	for _, scope := range []interfaces.SelectorType{
		interfaces.SelectorCRScope,
		interfaces.SelectorCRScopeReady,
	} {
		scoped := l.Selector(scope)
		require.True(t, narrowing.Matches(k8sLabels.Set(scoped)),
			"selector scope %v does not carry the app label, so informers narrowed to %q would "+
				"never deliver objects this scope asks for; scoped set was %v",
			scope, narrowing.String(), scoped)
	}
}
