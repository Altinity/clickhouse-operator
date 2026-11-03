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
)

// sqlHook is a small helper for building a SQL HookAction with a single query
// and a given event list.
func sqlHook(query string, events ...HookEvent) *HookAction {
	return &HookAction{
		SQL:    &SQLHookAction{Queries: []string{query}},
		Events: events,
	}
}

// TestReconcileHooksMergeFromIsDedupd locks in the fix for the accumulation bug:
// repeated MergeFrom calls (which can happen during the operator's normalization
// pipeline — buildCR runs createTemplatedCR which calls InheritClusterReconcileFrom,
// sometimes twice) must not duplicate hook actions in the receiver. Without dedup
// the persisted NormalizedCRCompleted accumulates the inherited hooks across
// reconciles, causing each hook to fire N times.
func TestReconcileHooksMergeFromIsDedupd(t *testing.T) {
	t.Run("repeated MergeFrom does not duplicate parent's hooks", func(t *testing.T) {
		parent := &ReconcileHooks{
			Pre:  []*HookAction{sqlHook("SELECT 'A'", HookEventHostShutdown)},
			Post: []*HookAction{sqlHook("SELECT 'B'", HookEventHostUpdate)},
		}
		child := &ReconcileHooks{}

		child = child.MergeFrom(parent)
		child = child.MergeFrom(parent)
		child = child.MergeFrom(parent)

		require.Len(t, child.Pre, 1, "Pre should contain exactly 1 entry after 3 merges of the same parent")
		require.Len(t, child.Post, 1, "Post should contain exactly 1 entry after 3 merges")
	})

	t.Run("MergeFrom keeps child's own action plus parent's distinct action", func(t *testing.T) {
		parent := &ReconcileHooks{
			Pre: []*HookAction{sqlHook("SELECT 'parent'", HookEventClusterReconcile)},
		}
		child := &ReconcileHooks{
			Pre: []*HookAction{sqlHook("SELECT 'child'", HookEventClusterReconcile)},
		}

		child = child.MergeFrom(parent)

		require.Len(t, child.Pre, 2, "child's own action and parent's distinct action both kept")
	})

	t.Run("hooks differing only by On are kept as separate entries", func(t *testing.T) {
		parent := &ReconcileHooks{
			Pre: []*HookAction{sqlHook("SELECT 'X'", HookEventHostUpdate)},
		}
		child := &ReconcileHooks{
			Pre: []*HookAction{sqlHook("SELECT 'X'", HookEventHostShutdown)},
		}

		child = child.MergeFrom(parent)

		require.Len(t, child.Pre, 2, "same SQL with different events must NOT dedup")
	})

	t.Run("MergeFrom with nil parent is a no-op", func(t *testing.T) {
		child := &ReconcileHooks{
			Pre: []*HookAction{sqlHook("SELECT 'A'", HookEventHostUpdate)},
		}
		out := child.MergeFrom(nil)
		require.Same(t, child, out)
		require.Len(t, out.Pre, 1)
	})

	t.Run("MergeFrom into nil child returns parent's deep copy", func(t *testing.T) {
		parent := &ReconcileHooks{
			Pre: []*HookAction{sqlHook("SELECT 'P'", HookEventHostUpdate)},
		}
		var child *ReconcileHooks
		out := child.MergeFrom(parent)
		require.NotNil(t, out)
		require.Len(t, out.Pre, 1)
		// Mutating the result should NOT mutate the parent.
		out.Pre[0].SQL.Queries = append(out.Pre[0].SQL.Queries, "extra")
		require.Len(t, parent.Pre[0].SQL.Queries, 1, "parent must not be mutated by deepcopy")
	})

	t.Run("Events compared case-insensitively for dedup", func(t *testing.T) {
		// Parent and child describe the same hook semantically; only differ in case.
		// Runtime matching is case-insensitive, so dedup must also be — otherwise
		// the same hook fires twice after inheritance.
		parent := &ReconcileHooks{
			Pre: []*HookAction{sqlHook("SELECT 'X'", "HostShutdown")},
		}
		child := &ReconcileHooks{
			Pre: []*HookAction{sqlHook("SELECT 'X'", "hostshutdown")},
		}

		child = child.MergeFrom(parent)

		require.Len(t, child.Pre, 1, "case-only differences in Events must dedup")
	})

	t.Run("Target compared case-insensitively for dedup", func(t *testing.T) {
		// Target value normalized at runtime (FirstHost == firsthost), so a hook
		// inherited from parent with one case must dedup against a child hook
		// using the other.
		parent := &ReconcileHooks{
			Pre: []*HookAction{{
				SQL:    &SQLHookAction{Queries: []string{"SELECT 1"}},
				Target: types.NewString("FirstHost"),
				Events: []HookEvent{HookEventClusterReconcile},
			}},
		}
		child := &ReconcileHooks{
			Pre: []*HookAction{{
				SQL:    &SQLHookAction{Queries: []string{"SELECT 1"}},
				Target: types.NewString("firsthost"),
				Events: []HookEvent{HookEventClusterReconcile},
			}},
		}

		child = child.MergeFrom(parent)

		require.Len(t, child.Pre, 1, "case-only differences in Target must dedup")
	})

	t.Run("FailurePolicy compared case-insensitively for dedup", func(t *testing.T) {
		// Same as Target — runtime normalizes Fail/fail/FAIL, so dedup must too.
		parent := &ReconcileHooks{
			Pre: []*HookAction{{
				SQL:           &SQLHookAction{Queries: []string{"SELECT 1"}},
				FailurePolicy: types.NewString("Ignore"),
				Events:        []HookEvent{HookEventHostShutdown},
			}},
		}
		child := &ReconcileHooks{
			Pre: []*HookAction{{
				SQL:           &SQLHookAction{Queries: []string{"SELECT 1"}},
				FailurePolicy: types.NewString("ignore"),
				Events:        []HookEvent{HookEventHostShutdown},
			}},
		}

		child = child.MergeFrom(parent)

		require.Len(t, child.Pre, 1, "case-only differences in FailurePolicy must dedup")
	})

	t.Run("Distinct Target values do NOT dedup", func(t *testing.T) {
		// Sanity check that EqualFold isn't masking real differences.
		parent := &ReconcileHooks{
			Pre: []*HookAction{{
				SQL:    &SQLHookAction{Queries: []string{"SELECT 1"}},
				Target: types.NewString("FirstHost"),
				Events: []HookEvent{HookEventClusterReconcile},
			}},
		}
		child := &ReconcileHooks{
			Pre: []*HookAction{{
				SQL:    &SQLHookAction{Queries: []string{"SELECT 1"}},
				Target: types.NewString("AllHosts"),
				Events: []HookEvent{HookEventClusterReconcile},
			}},
		}

		child = child.MergeFrom(parent)

		require.Len(t, child.Pre, 2, "FirstHost vs AllHosts are distinct hooks")
	})
}
