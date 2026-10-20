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
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// ZookeeperConfig defines zookeeper section of .spec.configuration.
// Maps to ClickHouse server's <zookeeper> XML configuration section.
// Ref: https://clickhouse.com/docs/operations/server-configuration-parameters/settings#zookeeper
type ZookeeperConfig struct {
	// Nodes is a list of ZooKeeper/Keeper endpoints (host:port pairs).
	// Rendered as <zookeeper><node><host>...</host><port>...</port></node></zookeeper> in ClickHouse config.
	Nodes ZookeeperNodes `json:"nodes,omitempty" yaml:"nodes,omitempty"`
	// Keeper is a reference to a ClickHouseKeeperInstallation (CHK) custom resource.
	// When specified, the operator resolves CHK service endpoints into Nodes automatically before reconcile.
	// Either Nodes or Keeper may be specified. If Keeper is set, resolved nodes are appended to Nodes.
	Keeper *KeeperRef `json:"keeper,omitempty" yaml:"keeper,omitempty"`
	// SessionTimeoutMs is the ZooKeeper session timeout in milliseconds.
	// Rendered as <zookeeper><session_timeout_ms>...</session_timeout_ms></zookeeper>.
	SessionTimeoutMs int `json:"session_timeout_ms,omitempty" yaml:"session_timeout_ms,omitempty"`
	// OperationTimeoutMs is the timeout for a single ZooKeeper operation in milliseconds.
	// Rendered as <zookeeper><operation_timeout_ms>...</operation_timeout_ms></zookeeper>.
	OperationTimeoutMs int `json:"operation_timeout_ms,omitempty" yaml:"operation_timeout_ms,omitempty"`
	// Root is an optional ZooKeeper root znode path prefix for all ClickHouse znodes.
	// Rendered as <zookeeper><root>...</root></zookeeper>.
	Root string `json:"root,omitempty" yaml:"root,omitempty"`
	// Identity is the ZooKeeper digest authentication credentials in "user:password" format.
	// Rendered as <zookeeper><identity>...</identity></zookeeper>.
	Identity string `json:"identity,omitempty" yaml:"identity,omitempty"`
	// UseCompression enables the Keeper protocol compression for client-server communication.
	// Rendered as <zookeeper><use_compression>...</use_compression></zookeeper>.
	UseCompression *types.StringBool `json:"use_compression,omitempty" yaml:"use_compression,omitempty"`
}

// ZookeeperNodes is a typed slice of ZookeeperNode providing convenience accessors.
type ZookeeperNodes []ZookeeperNode

// NewZookeeperNodes creates a ZookeeperNodes list from the given nodes.
func NewZookeeperNodes(nodes ...ZookeeperNode) ZookeeperNodes {
	return ZookeeperNodes(nodes)
}

// Append adds one or more nodes to the list and returns the updated list.
func (n ZookeeperNodes) Append(nodes ...ZookeeperNode) ZookeeperNodes {
	return append(n, nodes...)
}

// Len returns the number of nodes in the list.
func (n ZookeeperNodes) Len() int {
	return len(n)
}

// First returns the first node in the list. Panics if the list is empty.
func (n ZookeeperNodes) First() ZookeeperNode {
	return n[0]
}

// Servers returns a string slice of all nodes in "host:port" format.
func (n ZookeeperNodes) Servers() []string {
	var servers []string
	for _, node := range n {
		servers = append(servers, node.String())
	}
	return servers
}

// String returns a comma-separated list of all nodes in "host:port" format.
func (n ZookeeperNodes) String() string {
	return strings.Join(n.Servers(), ",")
}

// Equals reports whether two node lists contain the same set of nodes, ignoring order.
// Uses ZookeeperNode.Equal() as the element equality. A nil list is treated as an empty set,
// so nil equals an empty non-nil list.
func (n ZookeeperNodes) Equals(other ZookeeperNodes) bool {
	return util.SlicesEqualAsSetFunc(n, other, func(x, y ZookeeperNode) bool {
		return x.Equal(&y)
	})
}

// NewZookeeperConfig creates a new empty ZookeeperConfig.
func NewZookeeperConfig() *ZookeeperConfig {
	return new(ZookeeperConfig)
}

// IsEmpty returns true if no ZooKeeper connectivity is configured (no nodes and no keeper reference).
func (zkc *ZookeeperConfig) IsEmpty() bool {
	if zkc == nil {
		return true
	}

	return len(zkc.Nodes) == 0 && !zkc.HasKeeper()
}

// HasKeeper returns true if a keeper reference with a non-empty name is specified.
func (zkc *ZookeeperConfig) HasKeeper() bool {
	return zkc != nil && zkc.Keeper.HasName()
}

// GetKeeper returns the keeper reference, nil-safe when ZookeeperConfig itself is nil.
func (zkc *ZookeeperConfig) GetKeeper() *KeeperRef {
	if zkc == nil {
		return nil
	}
	return zkc.Keeper
}

// GetNodes returns the explicit ZooKeeper nodes list, nil-safe when ZookeeperConfig itself is nil.
func (zkc *ZookeeperConfig) GetNodes() ZookeeperNodes {
	if zkc == nil {
		return nil
	}
	return zkc.Nodes
}

// InheritFrom fills a cluster's ZooKeeper configuration from the installation-level one, for a
// cluster that names no nodes of its own. Unlike a layer merge, the installation's root, identity
// and timeouts replace the cluster's own: such a cluster has always been configured with them, so
// its replicated tables live under that root and authenticate with those credentials, and honouring
// the cluster's own values would move them. The cluster's own use_compression is kept, as always.
func (zkc *ZookeeperConfig) InheritFrom(from *ZookeeperConfig) *ZookeeperConfig {
	if from == nil {
		return zkc
	}
	var compression *types.StringBool
	if zkc != nil {
		compression = zkc.UseCompression
	}
	zkc = zkc.MergeFrom(from, MergeTypeOverrideByNonEmptyValues)
	zkc.UseCompression = MergeScalar(zkc.UseCompression, compression, MergeTypeOverrideByNonEmptyValues)
	return zkc
}

// MergeFrom merges ZooKeeper configuration from the provided source.
// Nodes are appended (duplicates skipped by equality check); the keeper reference and the scalar
// fields (timeouts, root, identity, compression) follow the merge direction.
func (zkc *ZookeeperConfig) MergeFrom(from *ZookeeperConfig, _type MergeType) *ZookeeperConfig {
	if from == nil {
		return zkc
	}

	if zkc == nil {
		zkc = NewZookeeperConfig()
	}

	if !from.IsEmpty() {
		// Append unique nodes from source
		if zkc.Nodes == nil {
			zkc.Nodes = make([]ZookeeperNode, 0)
		}
		for fromIndex := range from.Nodes {
			fromNode := &from.Nodes[fromIndex]

			equalFound := false
			for toIndex := range zkc.Nodes {
				toNode := &zkc.Nodes[toIndex]
				if toNode.Equal(fromNode) {
					equalFound = true
					break
				}
			}

			if !equalFound {
				zkc.Nodes = append(zkc.Nodes, *fromNode.DeepCopy())
			}
		}
	}

	// Keeper reference is resolved as a unit
	keeperFromWins := (_type == MergeTypeOverrideByNonEmptyValues) || zkc.Keeper.IsEmpty()
	if keeperFromWins && !from.Keeper.IsEmpty() {
		zkc.Keeper = from.Keeper.DeepCopy()
	}

	zkc.SessionTimeoutMs = MergeValue(zkc.SessionTimeoutMs, from.SessionTimeoutMs, _type)
	zkc.OperationTimeoutMs = MergeValue(zkc.OperationTimeoutMs, from.OperationTimeoutMs, _type)
	zkc.Root = MergeValue(zkc.Root, from.Root, _type)
	zkc.Identity = MergeValue(zkc.Identity, from.Identity, _type)
	zkc.UseCompression = MergeScalar(zkc.UseCompression, from.UseCompression, _type)

	return zkc
}

// Equals returns true if both ZookeeperConfig objects are structurally identical.
func (zkc *ZookeeperConfig) Equals(b *ZookeeperConfig) bool {
	_, equals := messagediff.DeepDiff(zkc, b)
	return equals
}
