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

// migrateTables wraps a failed HostCreateTables in common.ErrCRUDDeferred. That wrap is
// load-bearing, not decoration: reconcileHostMain propagates only the two CRUD sentinels and logs
// anything else as a Warning before continuing, so a bare error would leave the reconcile marching
// on to includeHost() and a Completed CHI - exactly the bug.
//
// It must be Deferred and NOT Abort: Abort unwinds the shard walk, so one host with one
// un-creatable object would halt every remaining shard mid-upgrade. Deferred lets the siblings
// reconcile and still ends the CR non-Completed.
func TestMigrateTablesFailureIsDeferredSentinel(t *testing.T) {
	underlying := errors.New("clickhouse: upstream connect error or disconnect/reset before headers")

	// Call the production wrapper, not a hand-rolled copy of it. An earlier version of this test
	// rebuilt the fmt.Errorf itself and so asserted on stdlib %w rather than on our code - it stayed
	// green with the sentinel removed from migrateTables.
	host := &api.Host{Name: "0-0"}
	err := migrateTablesFailure(host, underlying)

	require.Error(t, err)
	require.ErrorIs(t, err, common.ErrCRUDDeferred,
		"reconcileHostMain only propagates the CRUD sentinels - an unwrapped error is silently tolerated")
	require.False(t, errors.Is(err, common.ErrCRUDAbort),
		"must NOT be Abort: that would unwind the shard walk and starve the sibling shards")
	require.Contains(t, err.Error(), "0-0", "the host must be identifiable from the message")
	require.Contains(t, err.Error(), "upstream connect error", "the underlying cause must survive")
}

// A host recorded in hostsWithTablesCreated is skipped by shouldMigrateTables forever after
// (HasData() -> "no need to migrate again"). That is why a FAILED migration must not record the
// host: doing so does not merely hide the failure, it makes it permanent by preventing any retry.
func TestShouldMigrateTablesSkipsHostThatHasData(t *testing.T) {
	w := &worker{}

	// shouldMigrateTables consults IsStopped/IsTroubleshoot/IsInNewCluster, all of which reach
	// through GetCR(), so the host needs a CR attached.
	hostWithData := newTestHost()
	hostWithData.SetHasData(true)
	require.False(t, w.shouldMigrateTables(hostWithData),
		"a host marked tables-created is never migrated again - so never mark one whose migration failed")

	hostWithoutData := newTestHost()
	hostWithoutData.SetHasData(false)
	require.True(t, w.shouldMigrateTables(hostWithoutData),
		"a host with no tables recorded must remain eligible for migration retry")
}

// ForceMigrate outranks HasData, which is what lets the data-loss recovery path replay the schema
// onto a host that is already listed as tables-created.
func TestShouldMigrateTablesForceOutranksHasData(t *testing.T) {
	w := &worker{}
	host := newTestHost()
	host.SetHasData(true)

	require.True(t, w.shouldMigrateTables(host, NewMigrateTableOptions().SetForceMigrate()),
		"forceMigrate must override the tables-created short-circuit")
}

// newTestHost builds a Host wired to a CR, which shouldMigrateTables needs in order to evaluate its
// stopped/troubleshoot/new-cluster guards.
func newTestHost() *api.Host {
	cr := &api.ClickHouseInstallation{}
	// Differing counts keep IsInNewCluster false, so the HasData branch is the one under test.
	cr.EnsureStatus().HostsCount = 2
	cr.EnsureStatus().HostsAddedCount = 1
	host := &api.Host{}
	host.SetCR(cr)
	return host
}
