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
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// ZookeeperNode defines item of nodes section of .spec.configuration.zookeeper
type ZookeeperNode struct {
	Host             string            `json:"host,omitempty"             yaml:"host,omitempty"`
	Port             *types.Int32      `json:"port,omitempty"             yaml:"port,omitempty"`
	Secure           *types.StringBool `json:"secure,omitempty"           yaml:"secure,omitempty"`
	AvailabilityZone *types.String     `json:"availabilityZone,omitempty" yaml:"availabilityZone,omitempty"`
}

func (zkNode *ZookeeperNode) String() string {
	if zkNode == nil {
		return ""
	}
	str := zkNode.Host
	if zkNode.Port.HasValue() {
		str += ":" + zkNode.Port.String()
	}
	return str
}

// Equal checks whether zookeeper node is equal to another
func (zkNode *ZookeeperNode) Equal(to *ZookeeperNode) bool {
	if to == nil {
		return false
	}

	return zkNode.hostEqual(to) && zkNode.portEqual(to) && zkNode.secureEqual(to) && zkNode.availabilityZoneEqual(to)
}

func (zkNode *ZookeeperNode) hostEqual(to *ZookeeperNode) bool {
	return zkNode.Host == to.Host
}

func (zkNode *ZookeeperNode) portEqual(to *ZookeeperNode) bool {
	return zkNode.Port.Equal(to.Port)
}

func (zkNode *ZookeeperNode) secureEqual(to *ZookeeperNode) bool {
	return zkNode.Secure.Value() == to.Secure.Value()
}

func (zkNode *ZookeeperNode) availabilityZoneEqual(to *ZookeeperNode) bool {
	return zkNode.AvailabilityZone.Value() == to.AvailabilityZone.Value()
}

// IsSecure checks whether zookeeper node is secure
func (zkNode *ZookeeperNode) IsSecure() bool {
	if zkNode == nil {
		return false
	}

	return zkNode.Secure.Value()
}
