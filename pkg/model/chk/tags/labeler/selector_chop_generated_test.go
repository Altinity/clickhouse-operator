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
	k8sLabels "k8s.io/apimachinery/pkg/labels"
)

// The Keeper manager narrows its StatefulSet cache with this selector. Getting the key wrong hides
// every Keeper StatefulSet from the cache that drives Owns(), so the rendered string is pinned here
// rather than left to the caller - and the CHI flavour must not satisfy it.
func TestCHOPGeneratedSelectorRendersKeeperKey(t *testing.T) {
	rendered := k8sLabels.SelectorFromSet(New(nil).CHOPGeneratedSelector()).String()

	require.Equal(t, "clickhouse-keeper.altinity.com/app=chop", rendered)
}

// TestCHOPGeneratedSelectorIsImpliedByScopedSelectors is the invariant that makes a label-narrowed
// cache safe for Lists: the scoped selector the operator issues must already constrain the app
// label, so the cache filter is a strict superset and can never shrink a List result. If a scope
// stopped carrying it, a narrowed cache would silently return fewer objects than the caller asked
// for - discovery would go quiet, and purge acts on what discovery found.
//
// Only the CR scopes are covered: the cluster, shard and host scopes take the object they scope to
// as an argument, which a CR-less labeler cannot supply, and every cached List in the CHK adapter
// is CR-scoped. The host scope is pinned indirectly instead - it is what the StatefulSet's
// spec.selector is built from, asserted in pkg/model/chk/creator.
func TestCHOPGeneratedSelectorIsImpliedByScopedSelectors(t *testing.T) {
	l := New(nil)
	narrowing := k8sLabels.SelectorFromSet(l.CHOPGeneratedSelector())

	for _, scope := range []interfaces.SelectorType{
		interfaces.SelectorCRScope,
		interfaces.SelectorCRScopeReady,
	} {
		scoped := l.Selector(scope)
		require.True(t, narrowing.Matches(k8sLabels.Set(scoped)),
			"selector scope %v does not carry the app label, so a cache narrowed to %q would "+
				"return fewer objects than this scope asks for; scoped set was %v",
			scope, narrowing.String(), scoped)
	}
}
