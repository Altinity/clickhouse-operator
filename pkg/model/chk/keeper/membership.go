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

package keeper

import (
	"context"
	"fmt"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/model/zookeeper"
)

// GetCommittedMembership reads the committed Raft configuration from the
// Keeper node at host:port via the special znode `/keeper/config`.
//
// The read is served from that node's local committed state: it can lag behind
// the leader but can never show a configuration that has not been committed,
// so a positive match against the expected set is safe to use as a
// convergence barrier. A node without a live leader refuses the session
// entirely (ZCONNECTIONLOSS) — that surfaces as an error, which callers treat
// as "barrier not passed".
func GetCommittedMembership(ctx context.Context, host string, port int32) ([]Member, error) {
	conn := zookeeper.NewConnection(
		api.NewZookeeperNodes(api.ZookeeperNode{Host: host, Port: types.NewInt32(port)}),
		&zookeeper.ConnectionParams{MaxRetriesNum: 1},
	)
	defer conn.Close()
	data, _, err := conn.Get(ctx, "/keeper/config")
	if err != nil {
		return nil, fmt.Errorf("get /keeper/config from %s:%d: %w", host, port, err)
	}
	return ParseClusterConfig(string(data))
}
