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

// NewChiZookeeperConfig creates new ChiZookeeperConfig object
func NewChiZookeeperConfig() *ChiZookeeperConfig {
	return new(ChiZookeeperConfig)
}

// IsEmpty checks whether config is empty
func (zkc *ChiZookeeperConfig) IsEmpty() bool {
	if zkc == nil {
		return true
	}

	return len(zkc.Nodes) == 0
}

// MergeFrom merges from provided object
func (zkc *ChiZookeeperConfig) MergeFrom(from *ChiZookeeperConfig, _type MergeType) *ChiZookeeperConfig {
	if from == nil {
		return zkc
	}

	if zkc == nil {
		zkc = NewChiZookeeperConfig()
	}

	if !from.IsEmpty() {
		// Append Nodes from `from`
		if zkc.Nodes == nil {
			zkc.Nodes = make([]ChiZookeeperNode, 0)
		}
		for fromIndex := range from.Nodes {
			fromNode := &from.Nodes[fromIndex]

			// Try to find equal entry
			equalFound := false
			for toIndex := range zkc.Nodes {
				toNode := &zkc.Nodes[toIndex]
				if toNode.Equal(fromNode) {
					// Received already have such a node
					equalFound = true
					break
				}
			}

			if !equalFound {
				// Append Node from `from`
				zkc.Nodes = append(zkc.Nodes, *fromNode.DeepCopy())
			}
		}
	}

	if from.SessionTimeoutMs > 0 {
		zkc.SessionTimeoutMs = from.SessionTimeoutMs
	}
	if from.OperationTimeoutMs > 0 {
		zkc.OperationTimeoutMs = from.OperationTimeoutMs
	}
	if from.Root != "" {
		zkc.Root = from.Root
	}
	if from.Identity != "" {
		zkc.Identity = from.Identity
	}

	return zkc
}
