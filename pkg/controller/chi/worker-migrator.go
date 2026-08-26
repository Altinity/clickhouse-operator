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
	"context"
	"fmt"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/schemer"
	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"
)

type migrateTableOptions struct {
	forceMigrate                    bool
	forceDropReplicaUponStorageLoss bool
}

func NewMigrateTableOptions() *migrateTableOptions {
	return &migrateTableOptions{}
}

func (o *migrateTableOptions) SetForceMigrate() *migrateTableOptions {
	if o == nil {
		return o
	}
	o.forceMigrate = true
	return o
}

func (o *migrateTableOptions) ForceMigrate() bool {
	if o == nil {
		return false
	}
	return o.forceMigrate
}

func (o *migrateTableOptions) SetForceDropReplicaUponStorageLoss() *migrateTableOptions {
	if o == nil {
		return o
	}
	o.forceDropReplicaUponStorageLoss = true
	return o
}

func (o *migrateTableOptions) ForceDropReplicaUponStorageLoss() bool {
	if o == nil {
		return false
	}
	return o.forceDropReplicaUponStorageLoss
}

type migrateTableOptionsArr []*migrateTableOptions

// NewMigrateTableOptionsArr creates new migrateTableOptions array
func NewMigrateTableOptionsArr(opts ...*migrateTableOptions) (res migrateTableOptionsArr) {
	return append(res, opts...)
}

// First gets first option
func (a migrateTableOptionsArr) First() *migrateTableOptions {
	if len(a) > 0 {
		return a[0]
	}
	return nil
}

// migrateTables
func (w *worker) migrateTables(ctx context.Context, host *api.Host, opts *migrateTableOptions) error {
	if opts.ForceDropReplicaUponStorageLoss() {
		w.a.V(1).
			M(host).F().
			Info(
				"Need to drop replica on host %d to shard %d in cluster %s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		w.dropZKReplica(ctx, host, NewDropReplicaOptions().SetForceDropUponStorageLoss())
	}

	w.a.V(1).
		WithEvent(host.GetCR(), a.EventActionCreate, a.EventReasonCreateStarted).
		WithAction(host.GetCR()).
		M(host).F().
		Info(
			"Adding tables on shard/host:%d/%d cluster:%s",
			host.Runtime.Address.ShardIndex, host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ClusterName)

	if err := w.ensureClusterSchemer(host).HostCreateTables(ctx, host); err != nil {
		w.a.V(1).
			WithEvent(host.GetCR(), a.EventActionCreate, a.EventReasonCreateFailed).
			WithAction(host.GetCR()).
			M(host).F().
			Error("ERROR add tables failed on shard/host:%d/%d cluster:%s err:%v",
				host.Runtime.Address.ShardIndex, host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ClusterName, err)
		// Return, and do NOT fall through to the success announcement or PushHostTablesCreated.
		//
		// Pushing here was the worse half of the bug: hostsWithTablesCreated feeds HasData(), and
		// shouldMigrateTables() skips migration outright for a host that HasData - so a host recorded
		// as tables-created after a FAILED migration is never retried. It also fed the false
		// "Tables added successfully" event and let includeHost() put a schema-less replica back into
		// remote_servers with the CHI reporting Completed.
		//
		// See migrateTablesFailure for why the returned error carries a CRUD sentinel, and why it is
		// Deferred rather than Abort. Status is deliberately NOT written here: the post-restart
		// re-migration caller is best-effort and swallows this error, so writing status would poison
		// the CR as a side effect of a path that is allowed to fail.
		return migrateTablesFailure(host, err)
	}

	w.a.V(1).
		WithEvent(host.GetCR(), a.EventActionCreate, a.EventReasonCreateCompleted).
		WithAction(host.GetCR()).
		M(host).F().
		Info("Tables added successfully on shard/host:%d/%d cluster:%s",
			host.Runtime.Address.ShardIndex, host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ClusterName)

	host.GetCR().IEnsureStatus().PushHostTablesCreated(w.c.namer.Name(interfaces.NameFQDN, host))

	return nil
}

// migrateTablesFailure wraps a table-creation failure in the sentinel reconcileHostMain propagates.
// Extracted so the wrap is reachable from a test: everything else about the failure path needs a
// live schemer, but losing the sentinel is the silent regression - a bare error is logged as a
// Warning and the reconcile carries on to includeHost() with a Completed CHI.
//
// Deferred rather than Abort on purpose. Both end the CR non-Completed (reconcile() coerces any
// error to ErrCRUDAbort before markReconcileCompletedUnsuccessfully), but Abort unwinds the shard
// walk on the first bad host, so one un-creatable object - a Dictionary with a missing source, an
// MV over a dropped table - would halt every remaining shard mid-upgrade. Deferred is the signal
// the shard/cluster walks already understand: runConcurrently keeps going, records the deferral,
// and surfaces it once at the end of the pass, so the siblings still reconcile.
func migrateTablesFailure(host *api.Host, err error) error {
	return fmt.Errorf("%w: add tables failed on host %s: %v", common.ErrCRUDDeferred, host.GetName(), err)
}

func (w *worker) setHasData(host *api.Host) {
	host.SetHasData(host.HasListedTablesCreated(w.c.namer.Name(interfaces.NameFQDN, host)))
}

// shouldMigrateTables
func (w *worker) shouldMigrateTables(host *api.Host, opts ...*migrateTableOptions) bool {
	o := NewMigrateTableOptionsArr(opts...).First()

	// Deal with special cases in order of priority
	switch {
	case host.IsStopped():
		// Stopped host is not able to receive any data, migration is inapplicable
		return false

	case host.IsTroubleshoot():
		// Troubleshooted host is not able to receive any data, migration is inapplicable
		return false

	case o.ForceMigrate():
		// Force migration requested
		return true

	case host.HasData():
		// This host is listed as having tables created already, no need to migrate again
		return false

	case host.IsInNewCluster():
		// CHI is new, all hosts were added
		return false
	}

	// In all the rest cases - perform migration
	return true
}

func (w *worker) ensureClusterSchemer(host *api.Host) *schemer.ClusterSchemer {
	if w == nil {
		return nil
	}
	// Make base cluster connection params from CHOP-config defaults, then
	// overlay the per-cluster security.clickhouse.tls fields populated by the
	// normalizer (3-level inheritance: CHOP-config → CHI → cluster). Without
	// the overlay the cluster-level Verify/MinVersion/ServerName/RootCA
	// wouldn't reach the dial — CHOP-config values would silently win.
	clusterConnectionParams := clickhouse.NewClusterConnectionParamsFromCHOpConfig(chop.Config())
	clusterConnectionParams.OverlayClusterSecurityTLS(host.GetCluster().GetSecurity().GetClickHouse().GetTLS())
	// Adjust base cluster connection params with per-host props
	switch clusterConnectionParams.Scheme {
	case api.ChSchemeAuto:
		switch {
		case host.HTTPPort.HasValue():
			clusterConnectionParams.Scheme = api.ChSchemeHTTP
			clusterConnectionParams.Port = host.HTTPPort.IntValue()
		case host.HTTPSPort.HasValue():
			clusterConnectionParams.Scheme = api.ChSchemeHTTPS
			clusterConnectionParams.Port = host.HTTPSPort.IntValue()
		}
	case api.ChSchemeHTTP:
		clusterConnectionParams.Port = host.HTTPPort.IntValue()
	case api.ChSchemeHTTPS:
		clusterConnectionParams.Port = host.HTTPSPort.IntValue()
	}
	w.schemer = schemer.NewClusterSchemer(clusterConnectionParams, host.Runtime.Version)

	return w.schemer
}
