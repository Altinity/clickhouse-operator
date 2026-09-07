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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// chiWithReplicas builds a CHI whose diff against another has SEVERAL modified entries.
// Cardinality is the point: the rendering ranges a map, so a one-entry diff serialises
// identically every time and cannot show whether the result was cached or re-rendered.
func chiWithReplicas(n int) *ClickHouseInstallation {
	cr := &ClickHouseInstallation{}
	cr.EnsureRuntime()
	cr.Spec.Configuration = NewConfiguration()
	for i := 0; i < 4; i++ {
		cr.Spec.Configuration.Clusters = append(cr.Spec.Configuration.Clusters, &Cluster{
			Name:   fmt.Sprintf("c%d", i),
			Layout: &ChiClusterLayout{ShardsCount: n + i, ReplicasCount: n},
		})
	}
	return cr
}

// modifiedSpecItems reports how many spec entries the plan actually diffed, so a test can
// assert the fixture is rich enough for what it claims to prove.
func modifiedSpecItems(t *testing.T, plan IActionPlan) int {
	t.Helper()
	ap, ok := plan.(*ActionPlan)
	require.True(t, ok)
	require.NotNil(t, ap.specDiff)
	return len(ap.specDiff.Modified)
}

// TestActionPlanStringIsStableAcrossCalls pins the whole point of caching the rendering.
//
// The underlying diff is a map, and rendering ranges it, so an uncached String() returned
// different bytes on every call for the same plan. The status path calls String() once per
// update attempt - up to the retry limit - which both burned CPU reflection-dumping every
// diffed object and rewrote the stored text each time with identical meaning.
func TestActionPlanStringIsStableAcrossCalls(t *testing.T) {
	plan := MakeActionPlan(chiWithReplicas(1), chiWithReplicas(9))
	require.True(t, plan.HasActionsToDo(), "fixture must produce a non-empty plan")
	require.Greater(t, modifiedSpecItems(t, plan), 1,
		"fixture needs >1 modified entry: a single-entry diff renders stably even uncached, "+
			"so this test would pass against the very bug it exists to catch")

	first := plan.String()
	require.NotEmpty(t, first)
	for i := 0; i < 20; i++ {
		require.Equal(t, first, plan.String(), "String() must return identical bytes on every call")
	}
}

// TestActionPlanRendersOnlyOnDemand pins that construction does NOT render.
//
// prepareCHIUpdate builds a plan for every informer update event purely to test
// HasActionsToDo(), then discards it unread. Rendering in the constructor would pay a full
// reflection dump on the operator's busiest path - during the very scale-up this caching
// exists to speed up, where the diff is at its largest.
func TestActionPlanRendersOnlyOnDemand(t *testing.T) {
	plan := MakeActionPlan(chiWithReplicas(1), chiWithReplicas(9))

	ap, ok := plan.(*ActionPlan)
	require.True(t, ok)
	require.False(t, ap.rendered, "construction must not render; HasActionsToDo-only callers pay nothing")
	require.Empty(t, ap.str)

	require.NotEmpty(t, plan.String())
	require.True(t, ap.rendered, "first String() must populate the cache")
}

// TestActionPlanStringSurvivesNilOld pins that rendering a plan built from a nil old does not
// panic. Only the both-non-nil branch of MakeActionPlan assigns specDiffReverse, and render()
// dereferences it; that was survivable while String() was rarely called, but it is on the
// status path now.
func TestActionPlanStringSurvivesNilOld(t *testing.T) {
	plan := MakeActionPlan(nil, chiWithReplicas(3))
	require.NotPanics(t, func() { _ = plan.String() },
		"a plan with no old CR has no reverse diff and must still render")
}

// TestActionPlanEmptyPlanRendersEmpty pins that a plan with nothing to do renders "" and that
// the empty result is itself cached, rather than being re-rendered as a cache miss forever.
func TestActionPlanEmptyPlanRendersEmpty(t *testing.T) {
	same := chiWithReplicas(3)
	plan := MakeActionPlan(same, same)
	require.False(t, plan.HasActionsToDo())

	require.Empty(t, plan.String())
	ap := plan.(*ActionPlan)
	require.True(t, ap.rendered, "an empty rendering must still count as rendered, not retried each call")
}

// TestActionPlanStringIsRaceFree pins that concurrent String() is safe.
//
// One plan is shared by every shard worker in a fan-out: each worker triggers a status
// update, and building that status pointer-copies this same plan. Memoizing makes String()
// write to the receiver, so without a guard that is a data race on a string header - torn
// reads corrupt the stored status text rather than failing loudly. Production currently
// escapes it only because the reconciler renders the plan for a log line before the fan-out
// starts; this test does not rely on that accident. Run with -race to mean anything.
func TestActionPlanStringIsRaceFree(t *testing.T) {
	plan := MakeActionPlan(chiWithReplicas(1), chiWithReplicas(9))
	require.Greater(t, modifiedSpecItems(t, plan), 1, "need a multi-entry diff to race on")

	const goroutines = 8
	var wg sync.WaitGroup
	results := make([]string, goroutines)
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // release together, so the first render is genuinely contended
			results[i] = plan.String()
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 1; i < goroutines; i++ {
		require.Equal(t, results[0], results[i],
			"every goroutine must observe the same rendering")
	}
	require.NotEmpty(t, results[0])
}

// TestActionPlanZeroValueRendersEmpty pins the two guards that protect a plan which never
// went through MakeActionPlan: it has no mutex to lock and no diffs to render. Such a plan
// reports HasActionsToDo() true - every *Equal flag defaults false - so rendering it used to
// dereference a nil diff. String() is on the status path, where a panic kills the reconcile
// worker, so it answers "" instead.
func TestActionPlanZeroValueRendersEmpty(t *testing.T) {
	var ap ActionPlan
	require.Nil(t, ap.mu, "fixture must model a plan built outside MakeActionPlan")
	require.True(t, ap.HasActionsToDo(),
		"a never-diffed plan claims work to do, which is what makes rendering it dangerous")

	require.NotPanics(t, func() { _ = ap.String() })
	require.Empty(t, ap.String())
}

// TestActionPlanDeepCopyIsRaceFree pins that copying takes the same lock String() writes
// under. DeepCopyInto copies str/rendered by value, so an unguarded copy can capture
// rendered=true beside a half-written string header and serve that text for the rest of the
// plan's life. Generated CR deep-copies reach this whenever a status carries a plan.
func TestActionPlanDeepCopyIsRaceFree(t *testing.T) {
	plan := MakeActionPlan(chiWithReplicas(1), chiWithReplicas(9)).(*ActionPlan)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			if i%2 == 0 {
				_ = plan.String()
				return
			}
			var out ActionPlan
			plan.DeepCopyInto(&out)
		}(i)
	}
	close(start)
	wg.Wait()
}
