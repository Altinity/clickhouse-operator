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

package action_plan

import (
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
	"gopkg.in/d4l3k/messagediff.v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ActionPlan is an action plan with list of differences between two CHIs
type ActionPlan struct {
	old api.ICustomResource
	new api.ICustomResource

	specDiff  *messagediff.Diff
	specEqual bool

	labelsDiff  *messagediff.Diff
	labelsEqual bool

	deletionTimestampDiff  *messagediff.Diff
	deletionTimestampEqual bool

	finalizersDiff  *messagediff.Diff
	finalizersEqual bool

	attributesDiff  *messagediff.Diff
	attributesEqual bool

	skipTaskID bool
}

// NewActionPlan makes new ActionPlan out of two CHIs
func NewActionPlan(old, new api.ICustomResource) *ActionPlan {
	ap := &ActionPlan{
		old: old,
		new: new,
	}

	if (old != nil) && (new != nil) {
		ap.specDiff, ap.specEqual = messagediff.DeepDiff(ap.old.GetSpecA(), ap.new.GetSpecA())
		ap.labelsDiff, ap.labelsEqual = messagediff.DeepDiff(ap.old.GetLabels(), ap.new.GetLabels())
		ap.deletionTimestampEqual = ap.timestampEqual(ap.old.GetDeletionTimestamp(), ap.new.GetDeletionTimestamp())
		ap.deletionTimestampDiff, _ = messagediff.DeepDiff(ap.old.GetDeletionTimestamp(), ap.new.GetDeletionTimestamp())
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
		// Both are useless
		return true
	case (old == nil) && (new != nil):
		// Timestamp assigned
		return false
	case (old != nil) && (new == nil):
		// Timestamp unassigned
		return false
	}
	return old.Equal(new)
}

// excludePaths - sanitize diff - do not pay attention to changes in some paths, such as
// ObjectMeta.ResourceVersion
func (ap *ActionPlan) excludePaths() {
	if ap.specDiff == nil {
		return
	}

	excludePaths := make([]*messagediff.Path, 0)
	// Walk over all .diff.Modified paths and find .ObjectMeta.ResourceVersion path
	for ptrPath := range ap.specDiff.Modified {
		for i := range *ptrPath {
			pathNodeCurr := (*ptrPath)[i]
			pathNodePrev := (*ptrPath)[i]
			if i > 0 {
				// We have prev node
				pathNodePrev = (*ptrPath)[i-1]
			}

			if ap.isExcludedPath(pathNodePrev.String(), pathNodeCurr.String()) {
				// This path should be excluded from Modified
				excludePaths = append(excludePaths, ptrPath)
				break
			}
		}
	}

	// Exclude paths from diff.Modified
	for _, ptrPath := range excludePaths {
		delete(ap.specDiff.Modified, ptrPath)
	}
}

// isExcludedPath checks whether path is excluded
func (ap *ActionPlan) isExcludedPath(prev, cur string) bool {
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

// String stringifies ActionPlan
func (ap *ActionPlan) String() string {
	if !ap.HasActionsToDo() {
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
		func(cluster api.ICluster) {
			count += cluster.HostsCount()
		},
		func(shard api.IShard) {
			count += shard.HostsCount()
		},
		func(host *api.Host) {
			count++
		},
	)
	return count
}

// WalkRemoved walk removed cluster items
func (ap *ActionPlan) WalkRemoved(
	clusterFunc func(cluster api.ICluster),
	shardFunc func(shard api.IShard),
	hostFunc func(host *api.Host),
) {
	// TODO refactor to map[string]object handling, instead of slice
	for path := range ap.specDiff.Removed {
		switch ap.specDiff.Removed[path].(type) {
		//case api.ChiCluster:
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
		case api.ICluster:
			cluster := ap.specDiff.Removed[path].(api.ICluster)
			clusterFunc(cluster)
		//case *api.ChiShard:
		//	shard := ap.specDiff.Removed[path].(*api.ChiShard)
		//	shardFunc(shard)
		case api.IShard:
			shard := ap.specDiff.Removed[path].(api.IShard)
			shardFunc(shard)
		case *api.Host:
			host := ap.specDiff.Removed[path].(*api.Host)
			hostFunc(host)
		}
	}
}

// WalkAdded walk added cluster items
func (ap *ActionPlan) WalkAdded(
	clusterFunc func(cluster api.ICluster),
	shardFunc func(shard api.IShard),
	hostFunc func(host *api.Host),
) {
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
		case api.ICluster:
			cluster := ap.specDiff.Added[path].(api.ICluster)
			clusterFunc(cluster)
		//case *api.ChiShard:
		//	shard := ap.specDiff.Added[path].(*api.ChiShard)
		//	shardFunc(shard)
		case api.IShard:
			shard := ap.specDiff.Added[path].(api.IShard)
			shardFunc(shard)
		case *api.Host:
			host := ap.specDiff.Added[path].(*api.Host)
			hostFunc(host)
		}
	}
}

// WalkModified walk modified cluster items
func (ap *ActionPlan) WalkModified(
	clusterFunc func(cluster api.ICluster),
	shardFunc func(shard api.IShard),
	hostFunc func(host *api.Host),
) {
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
		case api.ICluster:
			cluster := ap.specDiff.Modified[path].(api.ICluster)
			clusterFunc(cluster)
		//case *api.ChiShard:
		//	shard := ap.specDiff.Modified[path].(*api.ChiShard)
		//	shardFunc(shard)
		case api.IShard:
			shard := ap.specDiff.Modified[path].(api.IShard)
			shardFunc(shard)
		case *api.Host:
			host := ap.specDiff.Modified[path].(*api.Host)
			hostFunc(host)
		}
	}
}
