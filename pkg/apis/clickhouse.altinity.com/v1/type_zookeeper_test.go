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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// zkNode is a small helper for building a ZookeeperNode in tests.
func zkNode(host string, port int32, secure bool) ZookeeperNode {
	return ZookeeperNode{
		Host:   host,
		Port:   types.NewInt32(port),
		Secure: types.NewStringBool(secure),
	}
}

// TestZookeeperNodesEquals verifies order-insensitive set equality on endpoint lists.
// Used by the CHK→CHI reconcile gateway to decide whether a CHK change altered the endpoint set.
func TestZookeeperNodesEquals(t *testing.T) {
	a := zkNode("a", 2181, false)
	b := zkNode("b", 2181, false)
	c := zkNode("c", 2181, false)
	aTLS := zkNode("a", 2281, true)

	tests := []struct {
		name     string
		x, y     ZookeeperNodes
		expected bool
	}{
		{"both nil", nil, nil, true},
		{"both empty", ZookeeperNodes{}, ZookeeperNodes{}, true},
		{"nil vs empty", nil, ZookeeperNodes{}, true},
		{"identical order", ZookeeperNodes{a, b, c}, ZookeeperNodes{a, b, c}, true},
		{"different order (set equal)", ZookeeperNodes{a, b, c}, ZookeeperNodes{c, a, b}, true},
		{"one element differs (new host)", ZookeeperNodes{a, b, c}, ZookeeperNodes{a, b, zkNode("d", 2181, false)}, false},
		{"one element differs (port changed)", ZookeeperNodes{a, b}, ZookeeperNodes{a, zkNode("b", 9999, false)}, false},
		{"one element differs (secure flipped)", ZookeeperNodes{a}, ZookeeperNodes{aTLS}, false},
		{"extra element in x", ZookeeperNodes{a, b, c}, ZookeeperNodes{a, b}, false},
		{"extra element in y", ZookeeperNodes{a, b}, ZookeeperNodes{a, b, c}, false},
		{"single node match", ZookeeperNodes{a}, ZookeeperNodes{a}, true},
		{"single node mismatch", ZookeeperNodes{a}, ZookeeperNodes{b}, false},
		// Duplicate guard — if one side has duplicates and the other doesn't, they should differ
		{"duplicate in x only", ZookeeperNodes{a, a, b}, ZookeeperNodes{a, b, c}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.x.Equals(tc.y))
			// Symmetry
			require.Equal(t, tc.expected, tc.y.Equals(tc.x))
		})
	}
}
