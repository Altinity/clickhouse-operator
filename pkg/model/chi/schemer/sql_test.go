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

package schemer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSqlDropReplica(t *testing.T) {
	s := &ClusterSchemer{}

	t.Run("replica of a surviving shard", func(t *testing.T) {
		require.Equal(t, []string{
			"SYSTEM DROP REPLICA 'chi-a-c-0-0'",
			"SYSTEM DROP DATABASE REPLICA 'shard-a|chi-a-c-0-0'",
		}, s.sqlDropReplica("shard-a", "chi-a-c-0-0", nil))
	})

	t.Run("replica of a removed shard is dropped by (Zoo)Keeper path as well", func(t *testing.T) {
		require.Equal(t, []string{
			"SYSTEM DROP REPLICA 'chi-a-c-1-0'",
			"SYSTEM DROP DATABASE REPLICA '1|chi-a-c-1-0'",
			"SYSTEM DROP REPLICA 'chi-a-c-1-0' FROM ZKPATH '/clickhouse/c/tables/1/default/t'",
		}, s.sqlDropReplica("1", "chi-a-c-1-0", []ZKReplica{{Path: "/clickhouse/c/tables/1/default/t", Name: "chi-a-c-1-0"}}))
	})
}

func TestQuoteString(t *testing.T) {
	require.Equal(t, `'a\'b\\c'`, quoteString(`a'b\c`))
}

func TestForeignZKReplicas(t *testing.T) {
	replicas := []ZKReplica{
		{Path: "/tables/1/t", Name: "r1"},
		{Path: "/tables/shared/t", Name: "r1"},
	}
	local := []ZKReplica{
		{Path: "/tables/shared/t", Name: "r2"},
		{Path: "/tables/2/t", Name: "r2"},
	}
	require.Equal(t, []ZKReplica{{Path: "/tables/1/t", Name: "r1"}}, foreignZKReplicas(replicas, local))
	require.Equal(t, replicas, foreignZKReplicas(replicas, nil))
}
