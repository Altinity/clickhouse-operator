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

// TestClusterGetSecureNilSafe pins that GetSecure/GetInsecure are nil-safe
// on nil cluster receivers and round-trip the underlying field. These are
// the load-bearing getters that host.IsSecure() / IsInsecure() chain into
// when the host-level flag is unset.
func TestClusterGetSecureNilSafe(t *testing.T) {
	t.Run("nil cluster receiver", func(t *testing.T) {
		var c *Cluster
		require.Nil(t, c.GetSecure())
		require.Nil(t, c.GetInsecure())
	})
	t.Run("empty cluster", func(t *testing.T) {
		c := &Cluster{}
		require.Nil(t, c.GetSecure())
		require.Nil(t, c.GetInsecure())
	})
	t.Run("cluster with explicit secure=yes", func(t *testing.T) {
		yes := types.NewStringBool(true)
		no := types.NewStringBool(false)
		c := &Cluster{Secure: yes, Insecure: no}
		require.True(t, c.GetSecure().Value())
		require.False(t, c.GetInsecure().Value())
	})
}
