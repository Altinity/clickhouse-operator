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
	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"gopkg.in/d4l3k/messagediff.v1"
)

type ActionPlan struct {
	old, new *v1.ClickHouseInstallation
	diff     *messagediff.Diff
	equal    bool
}

func NewActionPlan(old, new *v1.ClickHouseInstallation) *ActionPlan {
	ap := &ActionPlan{
		old: old,
		new: new,
	}
	if (old != nil) && (new != nil) {
		ap.diff, ap.equal = messagediff.DeepDiff(ap.old.Spec, ap.new.Spec)
	} else if old == nil {
		ap.diff, ap.equal = messagediff.DeepDiff(nil, ap.new.Spec)
	} else if new == nil {
		ap.diff, ap.equal = messagediff.DeepDiff(ap.old.Spec, nil)
	} else {
		// Both are nil
		ap.diff = nil
		ap.equal = true
	}

	// Sanitize diff - do not pay attention to ObjectMeta.ResourceVersion changes
	if ap.diff != nil {
		exclude := make([]*messagediff.Path, 0)
		for ptrPath := range ap.diff.Modified {
			for i := range *ptrPath {
				pathNodeCurr := (*ptrPath)[i]
				pathNodePrev := (*ptrPath)[i]
				if i > 0 {
					// We have prev node
					pathNodePrev = (*ptrPath)[i-1]
				}

				prev := pathNodePrev.String()
				curr := pathNodeCurr.String()
				if (prev == "ObjectMeta") && (curr == ".ResourceVersion") {
					// Exclude this path from Modified
					exclude = append(exclude, ptrPath)
				} else if (prev == ".ObjectMeta") && (curr == ".ResourceVersion") {
					// Exclude this path from Modified
					exclude = append(exclude, ptrPath)
				}
			}
		}
		for _, ptrPath := range exclude {
			delete(ap.diff.Modified, ptrPath)
		}
	}

	return ap
}

func (ap *ActionPlan) HasNoChanges() bool {
	return ap.equal || ((ap.diff != nil) && (len(ap.diff.Added) == 0) && (len(ap.diff.Removed) == 0) && (len(ap.diff.Modified) == 0))
}

func (ap *ActionPlan) GetNewHostsNum() int {
	return ap.new.HostsCount()
}

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

func (ap *ActionPlan) WalkRemoved(
	clusterFunc func(cluster *v1.ChiCluster),
	shardFunc func(shard *v1.ChiShard),
	hostFunc func(host *v1.ChiHost),
) {
	// TODO refactor to map[string]object handling, instead of slice
	for path := range ap.diff.Removed {
		switch ap.diff.Removed[path].(type) {
		case v1.ChiCluster:
			cluster := ap.diff.Removed[path].(v1.ChiCluster)
			clusterFunc(&cluster)
		case v1.ChiShard:
			shard := ap.diff.Removed[path].(v1.ChiShard)
			shardFunc(&shard)
		case v1.ChiHost:
			host := ap.diff.Removed[path].(v1.ChiHost)
			hostFunc(&host)
		case *v1.ChiCluster:
			cluster := ap.diff.Removed[path].(*v1.ChiCluster)
			clusterFunc(cluster)
		case *v1.ChiShard:
			shard := ap.diff.Removed[path].(*v1.ChiShard)
			shardFunc(shard)
		case *v1.ChiHost:
			host := ap.diff.Removed[path].(*v1.ChiHost)
			hostFunc(host)
		}
	}
}

func (ap *ActionPlan) WalkAdded(
	clusterFunc func(cluster *v1.ChiCluster),
	shardFunc func(shard *v1.ChiShard),
	hostFunc func(host *v1.ChiHost),
) {
	// TODO refactor to map[string]object handling, instead of slice
	for path := range ap.diff.Added {
		switch ap.diff.Added[path].(type) {
		case v1.ChiCluster:
			cluster := ap.diff.Added[path].(v1.ChiCluster)
			clusterFunc(&cluster)
		case v1.ChiShard:
			shard := ap.diff.Added[path].(v1.ChiShard)
			shardFunc(&shard)
		case v1.ChiHost:
			host := ap.diff.Added[path].(v1.ChiHost)
			hostFunc(&host)
		case *v1.ChiCluster:
			cluster := ap.diff.Added[path].(*v1.ChiCluster)
			clusterFunc(cluster)
		case *v1.ChiShard:
			shard := ap.diff.Added[path].(*v1.ChiShard)
			shardFunc(shard)
		case *v1.ChiHost:
			host := ap.diff.Added[path].(*v1.ChiHost)
			hostFunc(host)
		}
	}
}
