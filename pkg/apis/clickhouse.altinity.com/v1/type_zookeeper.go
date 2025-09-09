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
	"strings"

	"gopkg.in/d4l3k/messagediff.v1"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// ZookeeperConfig defines zookeeper section of .spec.configuration
// Refers to
// https://clickhouse.com/docs/operations/server-configuration-parameters/settings#zookeeper
type ZookeeperConfig struct {
	Nodes              ZookeeperNodes    `json:"nodes,omitempty"                yaml:"nodes,omitempty"`
	SessionTimeoutMs   int               `json:"session_timeout_ms,omitempty"   yaml:"session_timeout_ms,omitempty"`
	OperationTimeoutMs int               `json:"operation_timeout_ms,omitempty" yaml:"operation_timeout_ms,omitempty"`
	Root               string            `json:"root,omitempty"                 yaml:"root,omitempty"`
	Identity           string            `json:"identity,omitempty"             yaml:"identity,omitempty"`
	UseCompression     *types.StringBool `json:"use_compression,omitempty"      yaml:"use_compression,omitempty"`
}

type ZookeeperNodes []ZookeeperNode

func (n ZookeeperNodes) Len() int {
	return len(n)
}

func (n ZookeeperNodes) First() ZookeeperNode {
	return n[0]
}

func (n ZookeeperNodes) Servers() []string {
	var servers []string
	for _, node := range n {
		servers = append(servers, node.String())
	}
	return servers
}

func (n ZookeeperNodes) String() string {
	return strings.Join(n.Servers(), ",")
}

// NewZookeeperConfig creates new ZookeeperConfig object
func NewZookeeperConfig() *ZookeeperConfig {
	return new(ZookeeperConfig)
}

// IsEmpty checks whether config is empty
func (zkc *ZookeeperConfig) IsEmpty() bool {
	if zkc == nil {
		return true
	}

	return len(zkc.Nodes) == 0
}

// MergeFrom merges from provided object
func (zkc *ZookeeperConfig) MergeFrom(from *ZookeeperConfig, _type MergeType) *ZookeeperConfig {
	if from == nil {
		return zkc
	}

	if zkc == nil {
		zkc = NewZookeeperConfig()
	}

	if !from.IsEmpty() {
		// Append Nodes from `from`
		if zkc.Nodes == nil {
			zkc.Nodes = make([]ZookeeperNode, 0)
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
	zkc.UseCompression = zkc.UseCompression.MergeFrom(from.UseCompression)

	return zkc
}

// Equals checks whether config is equal to another one
func (zkc *ZookeeperConfig) Equals(b *ZookeeperConfig) bool {
	_, equals := messagediff.DeepDiff(zkc, b)
	return equals
}
