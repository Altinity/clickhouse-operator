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

package chi

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
)

// newShardWithHosts builds a bare shard holding the named hosts. No CR wiring is needed
// because shardHasHealthyPeer takes the shard directly.
func newShardWithHosts(t *testing.T, names ...string) *api.ChiShard {
	t.Helper()
	shard := &api.ChiShard{Name: "shard"}
	for _, name := range names {
		shard.Hosts = append(shard.Hosts, &api.Host{Name: name})
	}
	return shard
}

// TestShardHasHealthyPeer covers the rule that decides whether a host may be disrupted:
// a host is protected only while it is the shard's last healthy replica (#1704).
func TestShardHasHealthyPeer(t *testing.T) {
	allHealthy := func(*api.Host) bool { return true }
	noneHealthy := func(*api.Host) bool { return false }

	t.Run("nil host is safe - nothing to protect", func(t *testing.T) {
		require.True(t, shardHasHealthyPeer(newShardWithHosts(t, "h0", "h1"), nil, noneHealthy))
	})

	t.Run("nil shard is safe", func(t *testing.T) {
		require.True(t, shardHasHealthyPeer(nil, &api.Host{Name: "orphan"}, noneHealthy))
	})

	t.Run("single-replica shard is safe - no replication to preserve", func(t *testing.T) {
		shard := newShardWithHosts(t, "h0")
		// A one-host shard has no peer by definition, so disruption is always allowed:
		// blocking it would mean never being able to reconcile a non-replicated shard.
		require.True(t, shardHasHealthyPeer(shard, shard.Hosts[0], noneHealthy))
	})

	t.Run("peer healthy - safe to disrupt", func(t *testing.T) {
		shard := newShardWithHosts(t, "h0", "h1")
		require.True(t, shardHasHealthyPeer(shard, shard.Hosts[0], allHealthy))
	})

	t.Run("only peer unhealthy - NOT safe, this is the #1704 case", func(t *testing.T) {
		shard := newShardWithHosts(t, "h0", "h1")
		require.False(t, shardHasHealthyPeer(shard, shard.Hosts[0], noneHealthy))
	})

	t.Run("one healthy peer among several is enough", func(t *testing.T) {
		shard := newShardWithHosts(t, "h0", "h1", "h2")
		healthy := func(host *api.Host) bool { return host.GetName() == "h2" }
		require.True(t, shardHasHealthyPeer(shard, shard.Hosts[0], healthy))
	})

	t.Run("the host itself is never counted as its own peer", func(t *testing.T) {
		shard := newShardWithHosts(t, "h0", "h1")
		// Only h0 is healthy. Asked about h0, the answer must be "no healthy peer" - if self
		// counted, a lone survivor would look safe to take down.
		healthy := func(host *api.Host) bool { return host.GetName() == "h0" }
		require.False(t, shardHasHealthyPeer(shard, shard.Hosts[0], healthy))
	})

	t.Run("self-exclusion is by pointer, not by name", func(t *testing.T) {
		// Host names are user-overridable, so two replicas can share a name. A name-based
		// self-check would treat the duplicate as self and report no healthy peer, deferring
		// the host forever.
		shard := newShardWithHosts(t, "same", "same")
		require.True(t, shardHasHealthyPeer(shard, shard.Hosts[0], allHealthy))
	})
}

// TestErrCRUDDeferredIsDistinctFromAbort locks in the property the shard/cluster walks rely
// on: a deferral must be discriminable from a real abort, or a deferred host would once again
// stop every sibling shard.
func TestErrCRUDDeferredIsDistinctFromAbort(t *testing.T) {
	require.False(t, errors.Is(common.ErrCRUDDeferred, common.ErrCRUDAbort),
		"deferral must not satisfy errors.Is(..., ErrCRUDAbort)")
	require.False(t, errors.Is(common.ErrCRUDAbort, common.ErrCRUDDeferred))
	require.True(t, errors.Is(common.ErrCRUDDeferred, common.ErrCRUDDeferred))
}
