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

package chi

import (
	"fmt"

	"gopkg.in/d4l3k/messagediff.v1"

	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// ActionPlan is an action plan with list of differences between two CHIs
type ActionPlan struct {
	old, new  *v1.ClickHouseInstallation
	specDiff  *messagediff.Diff
	specEqual bool

	labelsDiff  *messagediff.Diff
	labelsEqual bool
}

// NewActionPlan makes new ActionPlan out of two CHIs
func NewActionPlan(old, new *v1.ClickHouseInstallation) *ActionPlan {
	ap := &ActionPlan{
		old: old,
		new: new,
	}

	if (old != nil) && (new != nil) {
		ap.specDiff, ap.specEqual = messagediff.DeepDiff(ap.old.Spec, ap.new.Spec)
		ap.labelsDiff, ap.labelsEqual = messagediff.DeepDiff(ap.old.Labels, ap.new.Labels)
	} else if old == nil {
		ap.specDiff, ap.specEqual = messagediff.DeepDiff(nil, ap.new.Spec)
		ap.labelsDiff, ap.labelsEqual = messagediff.DeepDiff(nil, ap.new.Labels)
	} else if new == nil {
		ap.specDiff, ap.specEqual = messagediff.DeepDiff(ap.old.Spec, nil)
		ap.labelsDiff, ap.labelsEqual = messagediff.DeepDiff(ap.old.Labels, nil)
	} else {
		// Both are nil
		ap.specDiff = nil
		ap.specEqual = true

		ap.labelsDiff = nil
		ap.labelsEqual = true
	}

	ap.excludePaths()

	return ap
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

	return false
}

// HasActionsToDo checks whether there are any actions to do - meaning changes between states to reconcile
func (ap *ActionPlan) HasActionsToDo() bool {

	if ap.specEqual && ap.labelsEqual {
		// Already checked - equal - no actions to do
		return false
	}

	if (ap.specDiff == nil) && (ap.labelsDiff == nil) {
		// No diff to check with - no actions to do
		return false
	}

	// Looks like have some changes

	if len(ap.specDiff.Added) > 0 {
		// Something added
		return true
	}

	if len(ap.specDiff.Removed) > 0 {
		// Something removed
		return true
	}

	if len(ap.specDiff.Modified) > 0 {
		// Something modified
		return true
	}

	if len(ap.labelsDiff.Added) > 0 {
		// Something added
		return true
	}

	if len(ap.labelsDiff.Removed) > 0 {
		// Something removed
		return true
	}

	if len(ap.labelsDiff.Modified) > 0 {
		// Something modified
		return true
	}

	// We should not be here, actually, because this means that there are some changes (diff is not empty),
	// but we were unable to find out what exactly changed
	return false
}

// String stringifies ActionPlan
func (ap *ActionPlan) String() string {
	if !ap.HasActionsToDo() {
		return ""
	}

	str := ""

	if len(ap.specDiff.Added) > 0 {
		// Something added
		str += ap.stringItem("added spec items", ap.specDiff.Added)
	}

	if len(ap.specDiff.Removed) > 0 {
		// Something removed
		str += ap.stringItem("removed spec items", ap.specDiff.Removed)
	}

	if len(ap.specDiff.Modified) > 0 {
		// Something modified
		str += ap.stringItem("modified spec items", ap.specDiff.Modified)
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

	return str
}

// stringItem stringifies one map[*messagediff.Path]interface{} item
func (ap *ActionPlan) stringItem(banner string, items map[*messagediff.Path]interface{}) string {
	var str string
	str += fmt.Sprintf("%s: %d\n", banner, len(items))
	str += fmt.Sprintf("----------\n")
	for pathPtr := range items {
		str += fmt.Sprintf("----- path:\n")
		for _, pathNode := range *pathPtr {
			str += fmt.Sprintf("%s\n", pathNode.String())
		}
		str += fmt.Sprintf("----- value:\n")
		str += fmt.Sprintf("%s\n", items[pathPtr])
		str += fmt.Sprintf("----------\n")
	}

	return str
}

// GetNewHostsNum - total number of hosts to be achieved
func (ap *ActionPlan) GetNewHostsNum() int {
	return ap.new.HostsCount()
}

// GetRemovedHostsNum - how many hosts would be removed
func (ap *ActionPlan) GetRemovedHostsNum() int {
	var count int
	ap.WalkRemoved(
		func(cluster *v1.ChiCluster) {
			count += cluster.HostsCount()
		},
		func(shard *v1.ChiShard) {
			count += shard.HostsCount()
		},
		func(host *v1.ChiHost) {
			count++
		},
	)
	return count
}

// WalkRemoved walk removed cluster items
func (ap *ActionPlan) WalkRemoved(
	clusterFunc func(cluster *v1.ChiCluster),
	shardFunc func(shard *v1.ChiShard),
	hostFunc func(host *v1.ChiHost),
) {
	// TODO refactor to map[string]object handling, instead of slice
	for path := range ap.specDiff.Removed {
		switch ap.specDiff.Removed[path].(type) {
		case v1.ChiCluster:
			cluster := ap.specDiff.Removed[path].(v1.ChiCluster)
			clusterFunc(&cluster)
		case v1.ChiShard:
			shard := ap.specDiff.Removed[path].(v1.ChiShard)
			shardFunc(&shard)
		case v1.ChiHost:
			host := ap.specDiff.Removed[path].(v1.ChiHost)
			hostFunc(&host)
		case *v1.ChiCluster:
			cluster := ap.specDiff.Removed[path].(*v1.ChiCluster)
			clusterFunc(cluster)
		case *v1.ChiShard:
			shard := ap.specDiff.Removed[path].(*v1.ChiShard)
			shardFunc(shard)
		case *v1.ChiHost:
			host := ap.specDiff.Removed[path].(*v1.ChiHost)
			hostFunc(host)
		}
	}
}

// WalkAdded walk added cluster items
func (ap *ActionPlan) WalkAdded(
	clusterFunc func(cluster *v1.ChiCluster),
	shardFunc func(shard *v1.ChiShard),
	hostFunc func(host *v1.ChiHost),
) {
	// TODO refactor to map[string]object handling, instead of slice
	for path := range ap.specDiff.Added {
		switch ap.specDiff.Added[path].(type) {
		case v1.ChiCluster:
			cluster := ap.specDiff.Added[path].(v1.ChiCluster)
			clusterFunc(&cluster)
		case v1.ChiShard:
			shard := ap.specDiff.Added[path].(v1.ChiShard)
			shardFunc(&shard)
		case v1.ChiHost:
			host := ap.specDiff.Added[path].(v1.ChiHost)
			hostFunc(&host)
		case *v1.ChiCluster:
			cluster := ap.specDiff.Added[path].(*v1.ChiCluster)
			clusterFunc(cluster)
		case *v1.ChiShard:
			shard := ap.specDiff.Added[path].(*v1.ChiShard)
			shardFunc(shard)
		case *v1.ChiHost:
			host := ap.specDiff.Added[path].(*v1.ChiHost)
			hostFunc(host)
		}
	}
}
