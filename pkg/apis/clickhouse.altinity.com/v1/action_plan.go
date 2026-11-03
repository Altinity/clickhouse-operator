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

	"gopkg.in/d4l3k/messagediff.v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/altinity/clickhouse-operator/pkg/util"
)

// +k8s:deepcopy-gen=false

// ActionPlan is an action plan with list of differences between two CHIs
type ActionPlan struct {
	old ICustomResource
	new ICustomResource

	specDiff        *messagediff.Diff
	specDiffReverse *messagediff.Diff
	specEqual       bool

	labelsDiff  *messagediff.Diff
	labelsEqual bool

	deletionTimestampDiff  *messagediff.Diff
	deletionTimestampEqual bool

	finalizersDiff  *messagediff.Diff
	finalizersEqual bool

	attributesDiff  *messagediff.Diff
	attributesEqual bool

	skipTaskID bool

	// str caches the rendering. render() reflection-dumps every diffed object and the
	// status path calls String() once per update attempt, so re-rendering dominated
	// operator CPU on a large scale-up. Rendered on first use, not at construction:
	// most plans are built only to test HasActionsToDo() and are discarded unread.
	//
	// mu guards that first render, because String() is genuinely called from several
	// goroutines at once: a shard fan-out gives every shard worker a status update, and
	// each one pointer-copies this same plan into the status it builds. Today the race is
	// masked - the reconciler renders the plan for a log line before the fan-out starts -
	// but that is an accident of statement order, not a guarantee, and it would vanish the
	// moment that log line became level-gated. A pointer keeps DeepCopyInto's *out = *ap
	// free of the copylocks vet error.
	str      string
	rendered bool
	mu       *sync.Mutex
}

func NewActionPlan() *ActionPlan {
	return &ActionPlan{}
}

// MakeActionPlan makes new ActionPlan out of two CHIs
func MakeActionPlan(old, new ICustomResource) IActionPlan {
	ap := &ActionPlan{
		old: old,
		new: new,
		mu:  &sync.Mutex{},
	}

	if (old != nil) && (new != nil) {
		ap.specDiff, ap.specEqual = messagediff.DeepDiff(ap.old.GetSpecA(), ap.new.GetSpecA())
		ap.specDiffReverse, _ = messagediff.DeepDiff(ap.new.GetSpecA(), ap.old.GetSpecA())
		ap.labelsDiff, ap.labelsEqual = messagediff.DeepDiff(ap.old.GetLabels(), ap.new.GetLabels())
		ap.deletionTimestampDiff, _ = messagediff.DeepDiff(ap.old.GetDeletionTimestamp(), ap.new.GetDeletionTimestamp())
		ap.deletionTimestampEqual = ap.timestampEqual(ap.old.GetDeletionTimestamp(), ap.new.GetDeletionTimestamp())
		ap.finalizersDiff, ap.finalizersEqual = messagediff.DeepDiff(ap.old.GetFinalizers(), ap.new.GetFinalizers())
		ap.attributesDiff, ap.attributesEqual = messagediff.DeepDiff(ap.old.GetRuntime().GetAttributes(), ap.new.GetRuntime().GetAttributes())
	} else if old == nil {
		ap.specDiff, ap.specEqual = messagediff.DeepDiff(nil, ap.new.GetSpecA())
		ap.labelsDiff, ap.labelsEqual = messagediff.DeepDiff(nil, ap.new.GetLabels())
		ap.deletionTimestampEqual = ap.timestampEqual(nil, ap.new.GetDeletionTimestamp())
		ap.deletionTimestampDiff, _ = messagediff.DeepDiff(nil, ap.new.GetDeletionTimestamp())
		ap.finalizersDiff, ap.finalizersEqual = messagediff.DeepDiff(nil, ap.new.GetFinalizers())
		ap.attributesDiff, ap.attributesEqual = messagediff.DeepDiff(nil, ap.new.GetRuntime().GetAttributes())
	} else if new == nil {
		ap.specDiff, ap.specEqual = messagediff.DeepDiff(ap.old.GetSpecA(), nil)
		ap.labelsDiff, ap.labelsEqual = messagediff.DeepDiff(ap.old.GetLabels(), nil)
		ap.deletionTimestampEqual = ap.timestampEqual(ap.old.GetDeletionTimestamp(), nil)
		ap.deletionTimestampDiff, _ = messagediff.DeepDiff(ap.old.GetDeletionTimestamp(), nil)
		ap.finalizersDiff, ap.finalizersEqual = messagediff.DeepDiff(ap.old.GetFinalizers(), nil)
		ap.attributesDiff, ap.attributesEqual = messagediff.DeepDiff(ap.old.GetRuntime().GetAttributes(), nil)
	} else {
		// Both are nil
		ap.specDiff = nil
		ap.specEqual = true

		ap.labelsDiff = nil
		ap.labelsEqual = true

		ap.deletionTimestampDiff = nil
		ap.deletionTimestampEqual = true

		ap.finalizersDiff = nil
		ap.finalizersEqual = true

		ap.attributesDiff = nil
		ap.attributesEqual = true
	}

	if (new != nil) && new.GetSpec().GetTaskID().IsAutoId() {
		ap.skipTaskID = true
	}

	ap.excludePaths()

	return ap
}

func (ap *ActionPlan) timestampEqual(old, new *meta.Time) bool {
	switch {
	case (old == nil) && (new == nil):
		// Both are useless - consider equal
		return true
	case (old == nil) && (new != nil):
		// Timestamp is assigned - unequal
		return false
	case (old != nil) && (new == nil):
		// Timestamp unassigned - unequal
		return false
	case (old != nil) && (new != nil):
		// Both have value - need to compare
		return old.Equal(new)
	default:
		// WTF?
		return false
	}
}

// excludePaths - sanitize diff - do not pay attention to changes in some paths, such as ObjectMeta.ResourceVersion
func (ap *ActionPlan) excludePaths() {
	// Sanity check
	if ap.specDiff == nil {
		return
	}

	// List of paths to be excluded
	var excludePaths []*messagediff.Path

	// Walk over all .diff.Modified paths and find all paths that are to be excluded from modified
	for path := range ap.specDiff.Modified {
		if ap.isExcludedPath(path) {
			// This path should be excluded from Modified
			excludePaths = append(excludePaths, path)
		}
	}

	// Exclude paths from diff.Modified according to the list of paths to be excluded
	for _, path := range excludePaths {
		delete(ap.specDiff.Modified, path)
	}
}

// isExcludedPath checks whether path is excluded
func (ap *ActionPlan) isExcludedPath(path *messagediff.Path) bool {
	// Walk over all path's segments and check whether any segment is excluded
	for i := range *path {
		pathNodeCurr := (*path)[i]
		pathNodePrev := (*path)[i]
		if i > 0 {
			// We have prev node
			pathNodePrev = (*path)[i-1]
		}

		if ap.isExcludedPathSegment(pathNodePrev.String(), pathNodeCurr.String()) {
			// Path has segment which specifies to exclude this path
			return true
		}
	}

	return false
}

// isExcludedPathSegment checks whether path segment is excluded
func (ap *ActionPlan) isExcludedPathSegment(prev, cur string) bool {
	if ((prev == "ObjectMeta") && (cur == ".ResourceVersion")) ||
		((prev == ".ObjectMeta") && (cur == ".ResourceVersion")) {
		return true
	}

	if ((prev == "Status") && (cur == "Status")) ||
		((prev == ".Status") && (cur == ".Status")) {
		return true
	}

	if ((prev == "Runtime") && (cur == "Version")) ||
		((prev == ".Runtime") && (cur == ".Version")) {
		return true
	}

	if (((prev == "TaskID") && (cur == ".TaskID")) ||
		((prev == ".TaskID") && (cur == ".TaskID"))) && ap.skipTaskID {
		return true
	}

	return false
}

// HasActionsToDo checks whether there are any actions to do - meaning changes between states to reconcile
func (ap *ActionPlan) HasActionsToDo() bool {
	if ap.specEqual && ap.labelsEqual && ap.deletionTimestampEqual && ap.finalizersEqual && ap.attributesEqual {
		// All is equal - no actions to do
		return false
	}

	// Something is not equal

	if ap.specDiff != nil {
		if len(ap.specDiff.Added)+len(ap.specDiff.Removed)+len(ap.specDiff.Modified) > 0 {
			// Spec section has some modifications
			return true
		}
	}

	if ap.labelsDiff != nil {
		if len(ap.labelsDiff.Added)+len(ap.labelsDiff.Removed)+len(ap.labelsDiff.Modified) > 0 {
			// Labels section has some modifications
			return true
		}
	}

	return !ap.deletionTimestampEqual || !ap.finalizersEqual || !ap.attributesEqual
}

func (ap *ActionPlan) Log(tag string) string {
	return fmt.Sprintf(
		"\nActionPlan start %s ---------------------------------------------:\n%s\nActionPlan end %s ---------------------------------------------",
		tag,
		ap,
		tag,
	)
}

// String stringifies ActionPlan, rendering at most once per plan.
func (ap *ActionPlan) String() string {
	if ap.mu == nil {
		// Not built by MakeActionPlan, so there is no shared plan to protect and no diffs
		// to cache. Render straight through rather than write unguarded fields.
		return ap.render()
	}
	ap.mu.Lock()
	defer ap.mu.Unlock()
	if !ap.rendered {
		ap.str = ap.render()
		ap.rendered = true
	}
	return ap.str
}

// render stringifies the ActionPlan. Call it through String(), which memoizes.
func (ap *ActionPlan) render() string {
	if !ap.HasActionsToDo() {
		return ""
	}
	if ap.specDiff == nil {
		// A plan that was never diffed reports actions-to-do (every *Equal flag is false)
		// but has nothing to render, so say so rather than dereference a nil diff.
		return ""
	}

	str := ""

	if len(ap.specDiff.Added) > 0 {
		// Something added
		str += util.MessageDiffItemString("added spec items", "none", "", ap.specDiff.Added)
	}

	if len(ap.specDiff.Removed) > 0 {
		// Something removed
		str += util.MessageDiffItemString("removed spec items", "none", "", ap.specDiff.Removed)
	}

	if len(ap.specDiff.Modified) > 0 {
		// Something modified
		str += util.MessageDiffItemString("modified spec items", "none", "", ap.specDiff.Modified)
	}

	// Only the both-non-nil construction branch assigns specDiffReverse, so a plan
	// built from a nil old has none. render() used to be reached rarely enough to
	// hide that; it must not panic now that String() is on the status path.
	if (ap.specDiffReverse != nil) && (len(ap.specDiffReverse.Modified) > 0) {
		// Something modified
		str += util.MessageDiffItemString("prev spec items", "none", "", ap.specDiffReverse.Modified)
	}

	if len(ap.labelsDiff.Added) > 0 {
		// Something added
		str += "added labels\n"
	}

	if len(ap.labelsDiff.Removed) > 0 {
		// Something removed
		str += "removed labels\n"
	}

	if len(ap.labelsDiff.Modified) > 0 {
		// Something modified
		str += "modified labels\n"
	}

	if !ap.deletionTimestampEqual {
		str += "modified deletion timestamp:\n"
		str += util.MessageDiffItemString("modified deletion timestamp", "none", ".metadata.deletionTimestamp", ap.deletionTimestampDiff.Modified)
	}

	if !ap.finalizersEqual {
		str += "modified finalizer:\n"
		str += util.MessageDiffItemString("modified finalizers", "none", ".metadata.finalizers", ap.finalizersDiff.Modified)
	}

	return str
}

// GetRemovedHostsNum - how many hosts would be removed
func (ap *ActionPlan) GetRemovedHostsNum() int {
	var count int
	ap.WalkRemoved(
		func(cluster ICluster) {
			count += cluster.HostsCount()
		},
		func(shard IShard) {
			count += shard.HostsCount()
		},
		func(host *Host) {
			count++
		},
	)
	return count
}

// WalkRemoved walk removed cluster items
func (ap *ActionPlan) WalkRemoved(
	clusterFunc func(cluster ICluster),
	shardFunc func(shard IShard),
	hostFunc func(host *Host),
) {
	if ap == nil {
		return
	}
	// TODO refactor to map[string]object handling, instead of slice
	for path := range ap.specDiff.Removed {
		switch ap.specDiff.Removed[path].(type) {
		//case ChiCluster:
		//	cluster := ap.specDiff.Removed[path].(api.ChiCluster)
		//	clusterFunc(&cluster)
		//case api.ChiShard:
		//	shard := ap.specDiff.Removed[path].(api.ChiShard)
		//	shardFunc(&shard)
		//case api.Host:
		//	host := ap.specDiff.Removed[path].(api.Host)
		//	hostFunc(&host)
		//case *api.ChiCluster:
		//	cluster := ap.specDiff.Removed[path].(*api.ChiCluster)
		//	clusterFunc(cluster)
		case ICluster:
			cluster := ap.specDiff.Removed[path].(ICluster)
			clusterFunc(cluster)
		//case *api.ChiShard:
		//	shard := ap.specDiff.Removed[path].(*api.ChiShard)
		//	shardFunc(shard)
		case IShard:
			shard := ap.specDiff.Removed[path].(IShard)
			shardFunc(shard)
		case *Host:
			host := ap.specDiff.Removed[path].(*Host)
			hostFunc(host)
		}
	}
}

// WalkAdded walk added cluster items
func (ap *ActionPlan) WalkAdded(
	clusterFunc func(cluster ICluster),
	shardFunc func(shard IShard),
	hostFunc func(host *Host),
) {
	if ap == nil {
		return
	}
	// TODO refactor to map[string]object handling, instead of slice
	for path := range ap.specDiff.Added {
		switch ap.specDiff.Added[path].(type) {
		//case api.ChiCluster:
		//	cluster := ap.specDiff.Added[path].(api.ChiCluster)
		//	clusterFunc(&cluster)
		//case api.ChiShard:
		//	shard := ap.specDiff.Added[path].(api.ChiShard)
		//	shardFunc(&shard)
		//case api.Host:
		//	host := ap.specDiff.Added[path].(api.Host)
		//	hostFunc(&host)
		//case *api.ChiCluster:
		//	cluster := ap.specDiff.Added[path].(*api.ChiCluster)
		//	clusterFunc(cluster)
		case ICluster:
			cluster := ap.specDiff.Added[path].(ICluster)
			clusterFunc(cluster)
		//case *api.ChiShard:
		//	shard := ap.specDiff.Added[path].(*api.ChiShard)
		//	shardFunc(shard)
		case IShard:
			shard := ap.specDiff.Added[path].(IShard)
			shardFunc(shard)
		case *Host:
			host := ap.specDiff.Added[path].(*Host)
			hostFunc(host)
		}
	}
}

// WalkModified walk modified cluster items
func (ap *ActionPlan) WalkModified(
	clusterFunc func(cluster ICluster),
	shardFunc func(shard IShard),
	hostFunc func(host *Host),
) {
	if ap == nil {
		return
	}
	// TODO refactor to map[string]object handling, instead of slice
	for path := range ap.specDiff.Modified {
		switch ap.specDiff.Modified[path].(type) {
		//case api.ChiCluster:
		//	cluster := ap.specDiff.Modified[path].(api.ChiCluster)
		//	clusterFunc(&cluster)
		//case api.ChiShard:
		//	shard := ap.specDiff.Modified[path].(api.ChiShard)
		//	shardFunc(&shard)
		//case api.Host:
		//	host := ap.specDiff.Modified[path].(api.Host)
		//	hostFunc(&host)
		//case *api.ChiCluster:
		//	cluster := ap.specDiff.Modified[path].(*api.ChiCluster)
		//	clusterFunc(cluster)
		case ICluster:
			cluster := ap.specDiff.Modified[path].(ICluster)
			clusterFunc(cluster)
		//case *ChiShard:
		//	shard := ap.specDiff.Modified[path].(*ChiShard)
		//	shardFunc(shard)
		case IShard:
			shard := ap.specDiff.Modified[path].(IShard)
			shardFunc(shard)
		case *Host:
			host := ap.specDiff.Modified[path].(*Host)
			hostFunc(host)
		}
	}
}

// DeepCopyInto is deliberately shallow: the diffs are fixed once MakeActionPlan returns,
// and its messagediff.Diff pointers are not safe to deep-copy. Sharing pointers between
// source and copy is acceptable because nobody mutates the diffs.
//
// The plan is not strictly immutable, though: String() memoizes on first call, so it
// mutates str/rendered under mu - and this copies those two fields, so it has to take the
// same lock. Without it a copy can land with rendered=true beside a torn str header and
// then serve that text forever. The copy shares mu with the source, which is harmless:
// each guards its own first render.
func (ap *ActionPlan) DeepCopyInto(out *ActionPlan) {
	if ap.mu != nil {
		ap.mu.Lock()
		defer ap.mu.Unlock()
	}
	*out = *ap
}

func (ap *ActionPlan) DeepCopy() *ActionPlan {
	if ap == nil {
		return nil
	}
	out := new(ActionPlan)
	ap.DeepCopyInto(out)
	return out
}

func (ap *ActionPlan) DeepCopyIActionPlan() IActionPlan {
	if ap == nil {
		return nil
	}
	return ap.DeepCopy()
}
