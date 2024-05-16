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

package namer

type Target string

type namer struct {
	target Target
}

// NewNamer creates new namer with specified context
func NewNamer(target Target) *namer {
	return &namer{
		target: target,
	}
}

func (n *namer) lenCHI() int {
	if n.target == TargetLabels {
		return namePartChiMaxLenLabelsCtx
	} else {
		return namePartChiMaxLenNamesCtx
	}
}

func (n *namer) lenCluster() int {
	if n.target == TargetLabels {
		return namePartClusterMaxLenLabelsCtx
	} else {
		return namePartClusterMaxLenNamesCtx
	}
}

func (n *namer) lenShard() int {
	if n.target == TargetLabels {
		return namePartShardMaxLenLabelsCtx
	} else {
		return namePartShardMaxLenNamesCtx
	}

}

func (n *namer) lenReplica() int {
	if n.target == TargetLabels {
		return namePartReplicaMaxLenLabelsCtx
	} else {
		return namePartReplicaMaxLenNamesCtx
	}
}
