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

func (zoo *ChiZookeeperConfig) MergeFrom(from *ChiZookeeperConfig) {
	if from == nil {
		return
	}

	if len(from.Nodes) > 0 {
		// Append Nodes from `from`
		if zoo.Nodes == nil {
			zoo.Nodes = make([]ChiZookeeperNode, 0)
		}
		for fromIndex := range from.Nodes {
			fromNode := &from.Nodes[fromIndex]

			// Try to find equal entry
			equalFound := false
			for toIndex := range zoo.Nodes {
				toNode := &zoo.Nodes[toIndex]
				if toNode.Equal(fromNode) {
					// Received already have such a node
					equalFound = true
					break
				}
			}

			if !equalFound {
				// Append Node from `from`
				zoo.Nodes = append(zoo.Nodes, *fromNode.DeepCopy())
			}
		}
	}
}
