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
	"errors"
	"math"
	"sync"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/swversion"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
)

func (w *worker) getHostSoftwareVersion(ctx context.Context, host *api.Host) *swversion.SoftWareVersion {
	opts := &VersionOptions{
		Skip{
			New:             true,
			StoppedAncestor: true,
		},
	}

	// Try to report tag-based version
	if tagBasedVersion := w.getTagBasedVersion(host); tagBasedVersion.IsKnown() {
		// Able to report version from the tag
		return tagBasedVersion.SetDescription("parsed from the tag: '%s'", tagBasedVersion.GetOriginal())
	} else {
		w.a.V(1).M(host).F().Info("Unable to report version from the tag. Tag: '%s' Host: %s ", tagBasedVersion.GetOriginal(), host.GetName())
		if tagBasedOnly, description := opts.tagBasedOnly(host); tagBasedOnly {
			return swversion.MinVersion().SetDescription("set min version cause unable to parse from the tag: '%s' via '%s'", tagBasedVersion.GetOriginal(), description)
		}
		w.a.V(1).M(host).F().Info("Fallback to app-based version. Tag: '%s' Host: %s ", tagBasedVersion.GetOriginal(), host.GetName())
	}

	// Try to report version from the app
	if appBasedVersion := w.getHostClickHouseVersion(ctx, host); appBasedVersion.IsKnown() {
		// Able to fetch version from the app - report version
		return appBasedVersion.SetDescription("fetched from the host")
	}

	// Unable to acquire any version - report min one
	return swversion.MinVersion().SetDescription("min - unable to acquire neither from the tag nor from the app")
}

func (w *worker) isHostSoftwareAbleToRespond(ctx context.Context, host *api.Host) error {
	// Check whether the software is able to respond its version
	version := w.getHostClickHouseVersion(ctx, host)
	if version.IsKnown() {
		w.a.V(1).M(host).F().Info("Host software is alive - version detected. Host: %s version: %s", host.GetName(), version)
	} else {
		w.a.V(1).M(host).F().Info("Host software is not alive - version NOT detected. Host: %s ", host.GetName())
	}

	return nil
}

// getReconcileShardsWorkersNum calculates how many workers are allowed to be used for concurrent shards reconcile
func (w *worker) getReconcileShardsWorkersNum(cluster *api.Cluster, opts *common.ReconcileShardsAndHostsOptions) int {
	availableWorkers := float64(cluster.GetReconcile().Runtime.ReconcileShardsThreadsNumber)
	maxConcurrencyPercent := float64(cluster.GetReconcile().Runtime.ReconcileShardsMaxConcurrencyPercent)
	_100Percent := float64(100)
	shardsNum := float64(len(cluster.Layout.Shards))

	if opts.FullFanOut {
		// For full fan-out scenarios use all available workers.
		// Always allow at least 1 worker.
		return int(math.Max(availableWorkers, 1))
	}

	// For non-full fan-out scenarios respect .Reconcile.Runtime.ReconcileShardsMaxConcurrencyPercent.
	// Always allow at least 1 worker.
	maxAllowedWorkers := math.Max(math.Round((maxConcurrencyPercent/_100Percent)*shardsNum), 1)
	return int(math.Min(availableWorkers, maxAllowedWorkers))
}

func (w *worker) reconcileShardsAndHostsFetchOpts(ctx context.Context) *common.ReconcileShardsAndHostsOptions {
	// Try to fetch options
	if opts, ok := ctx.Value(common.ReconcileShardsAndHostsOptionsCtxKey).(*common.ReconcileShardsAndHostsOptions); ok {
		w.a.V(1).Info("found ReconcileShardsAndHostsOptionsCtxKey")
		return opts
	} else {
		w.a.V(1).Info("not found ReconcileShardsAndHostsOptionsCtxKey, use empty opts")
		return &common.ReconcileShardsAndHostsOptions{}
	}
}

func (w *worker) runConcurrently(ctx context.Context, workersNum int, startShardIndex int, shards []*api.ChiShard) error {
	if len(shards) == 0 {
		return nil
	}

	type shardReconcile struct {
		shard *api.ChiShard
		index int
	}

	ch := make(chan *shardReconcile)
	wg := sync.WaitGroup{}

	// Launch tasks feeder
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(ch)
		for i, shard := range shards {
			ch <- &shardReconcile{
				shard,
				startShardIndex + i,
			}
		}
	}()

	// Launch workers
	var err error
	// Tracked apart from err: a deferral is a routine outcome, and last-writer-wins would let
	// one silently overwrite a real shard failure.
	var deferred bool
	var errLock sync.Mutex
	for i := 0; i < workersNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rq := range ch {
				w.a.V(1).Info("Starting shard index: %d on worker", rq.index)
				if e := w.reconcileShardWithHosts(ctx, rq.shard); e != nil {
					errLock.Lock()
					if errors.Is(e, common.ErrCRUDDeferred) {
						deferred = true
					} else {
						err = e
					}
					errLock.Unlock()
				}
			}
		}()
	}

	w.a.V(1).Info("Starting to wait shards from index: %d on workers.", startShardIndex)
	wg.Wait()
	w.a.V(1).Info("Finished to wait shards from index: %d on workers.", startShardIndex)
	if err != nil {
		return err
	}
	if deferred {
		return common.ErrCRUDDeferred
	}
	return nil
}

func (w *worker) hostPVCsDataLossDetectedOptions(host *api.Host) (*statefulset.ReconcileOptions, *migrateTableOptions) {
	w.a.V(1).
		M(host).F().
		Info("Data loss detected for host: %s. Will do force data recovery", host.GetName())

	// In case of data loss detection on existing volumes, we need to:
	// 1. recreate StatefulSet
	// 2. run tables migration again

	stsReconcileOpts := statefulset.NewReconcileStatefulSetOptions().SetForceRecreate()
	migrateTableOpts := NewMigrateTableOptions().SetForceMigrate().SetForceDropReplicaUponStorageLoss()
	return stsReconcileOpts, migrateTableOpts
}

// hostPVCsDataVolumeAddedDetectedOptions is the response to a volume being ADDED to a host that
// already has data.
//
// Two things are needed, and only two:
//  1. Re-create the StatefulSet so the new mount takes effect.
//  2. Force a table migration afterwards. Re-creating the pod wipes every non-persistent object the
//     operator had migrated onto it - notably Engine=Memory databases and views, which live in RAM,
//     are not ZK-replicated, and have nothing to restore them. HostCreateTables issues
//     CREATE ... IF NOT EXISTS, so persistent objects are untouched. Dropping this step is what
//     broke test_010036 ("checking view in Memory engine exists"): the view never came back.
//
// What must NOT happen is SetForceDropReplicaUponStorageLoss. Nothing was lost, so tearing the ZK
// replica down and rebuilding it is destructive busywork - that is the whole point of splitting this
// path away from hostPVCsDataLossDetectedOptions.
func (w *worker) hostPVCsDataVolumeAddedDetectedOptions(host *api.Host) (*statefulset.ReconcileOptions, *migrateTableOptions) {
	w.a.V(1).
		M(host).F().
		Info("Volume added to host: %s. Will recreate StatefulSet and re-migrate tables, without replica drop", host.GetName())

	stsReconcileOpts := statefulset.NewReconcileStatefulSetOptions().SetForceRecreate()
	migrateTableOpts := NewMigrateTableOptions().SetForceMigrate()
	return stsReconcileOpts, migrateTableOpts
}

func (w *worker) hostPVCsDataVolumeMissedDetectedOptions(host *api.Host) (*statefulset.ReconcileOptions, *migrateTableOptions) {
	w.a.V(1).
		M(host).F().
		Info("Data volume missed detected for host: %s. Will do force volume creation", host.GetName())

	// In case of data volume missed detection, we need to:
	// 1. recreate StatefulSet
	// NB Do not run tables migration again

	stsReconcileOpts := statefulset.NewReconcileStatefulSetOptions().SetForceRecreate()
	return stsReconcileOpts, nil
}
