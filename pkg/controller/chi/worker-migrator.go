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

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/schemer"
	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type migrateTableOptions struct {
	forceMigrate bool
	dropReplica  bool
}

func (o *migrateTableOptions) ForceMigrate() bool {
	if o == nil {
		return false
	}
	return o.forceMigrate
}

func (o *migrateTableOptions) DropReplica() bool {
	if o == nil {
		return false
	}
	return o.dropReplica
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
func (w *worker) migrateTables(ctx context.Context, host *api.Host, opts ...*migrateTableOptions) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	if !w.shouldMigrateTables(host, opts...) {
		w.a.V(1).
			M(host).F().
			Info(
				"No need to add tables on host %d to shard %d in cluster %s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		return nil
	}

	// Need to migrate tables

	if w.shouldDropReplica(host, opts...) {
		w.a.V(1).
			M(host).F().
			Info(
				"Need to drop replica on host %d to shard %d in cluster %s",
				host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ShardIndex, host.Runtime.Address.ClusterName)
		w.dropReplica(ctx, host, &dropReplicaOptions{forceDrop: true})
	}

	w.a.V(1).
		WithEvent(host.GetCR(), common.EventActionCreate, common.EventReasonCreateStarted).
		WithStatusAction(host.GetCR()).
		M(host).F().
		Info(
			"Adding tables on shard/host:%d/%d cluster:%s",
			host.Runtime.Address.ShardIndex, host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ClusterName)

	err := w.ensureClusterSchemer(host).HostCreateTables(ctx, host)
	if err == nil {
		w.a.V(1).
			WithEvent(host.GetCR(), common.EventActionCreate, common.EventReasonCreateCompleted).
			WithStatusAction(host.GetCR()).
			M(host).F().
			Info("Tables added successfully on shard/host:%d/%d cluster:%s",
				host.Runtime.Address.ShardIndex, host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ClusterName)
		host.GetCR().EnsureStatus().PushHostTablesCreated(w.c.namer.Name(interfaces.NameFQDN, host))
	} else {
		w.a.V(1).
			WithEvent(host.GetCR(), common.EventActionCreate, common.EventReasonCreateFailed).
			WithStatusAction(host.GetCR()).
			M(host).F().
			Error("ERROR add tables added successfully on shard/host:%d/%d cluster:%s err:%v",
				host.Runtime.Address.ShardIndex, host.Runtime.Address.ReplicaIndex, host.Runtime.Address.ClusterName, err)
	}
	return err
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

	case o.ForceMigrate():
		// Force migration requested
		return true

	case host.HasData():
		// This host is listed as having tables created already, no need to migrate again
		return false

	case host.IsNewOne():
		// CHI is new, all hosts were added
		return false
	}

	// In all the rest cases - perform migration
	return true
}

// shouldDropTables
func (w *worker) shouldDropReplica(host *api.Host, opts ...*migrateTableOptions) bool {
	o := NewMigrateTableOptionsArr(opts...).First()

	// Deal with special cases
	switch {
	case o.DropReplica():
		return true

	}

	return false
}

func (w *worker) ensureClusterSchemer(host *api.Host) *schemer.ClusterSchemer {
	if w == nil {
		return nil
	}
	// Make base cluster connection params
	clusterConnectionParams := clickhouse.NewClusterConnectionParamsFromCHOpConfig(chop.Config())
	// Adjust base cluster connection params with per-host props
	switch clusterConnectionParams.Scheme {
	case api.ChSchemeAuto:
		switch {
		case host.HTTPPort.HasValue():
			clusterConnectionParams.Scheme = "http"
			clusterConnectionParams.Port = host.HTTPPort.IntValue()
		case host.HTTPSPort.HasValue():
			clusterConnectionParams.Scheme = "https"
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
