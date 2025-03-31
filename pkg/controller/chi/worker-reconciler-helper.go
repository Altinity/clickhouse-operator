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
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/swversion"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
	"github.com/altinity/clickhouse-operator/pkg/util"
	"k8s.io/apimachinery/pkg/version"
	"math"
	"sync"
)

const (
	knownVersion = "ok to query CH"
	unknownVersion = "failed to query CH"
)

func (w *worker) getHostSoftwareVersion(ctx context.Context, host *api.Host) *swversion.SoftWareVersion {
	opts := versionOptions{
		Skip{
			New:             true,
			StoppedAncestor: true,
		},
	}

	// Fetch tag from the image
	tag, tagOk := w.task.Creator().GetAppImageTag(host)

	if skip, description := opts.shouldSkip(host); skip {
		// Need to report version from the tag
		if tagOk {
			if version := swversion.NewSoftWareVersionFromTag(tag); version != nil {
				return version.SetDescription(description)
			}
		}

		// Unable to report version from the tag - report min one
		return swversion.MinVersion().SetDescription(description)
	}

	// Try to report version from the app

	if version, err := w.getHostClickHouseVersion(ctx, host); err == nil {
		// Able to fetch version from the host - report version
		return version.SetDescription(knownVersion)
	}

	// Unable to fetch version fom the host - report min one
	return swversion.MinVersion().SetDescription(unknownVersion)
}

func (w *worker) isHostSoftwareAbleToRespond(ctx context.Context, host *api.Host) error {
	// Check whether the software is able to respond its version
	version, err := w.getHostClickHouseVersion(ctx, host)
	if err != nil {
		w.a.V(1).M(host).F().Info("Host software is not alive - version NOT detected. Host: %s Err: %v", host.GetName(), err)
	}

	w.a.V(1).M(host).F().Info("Host software is alive - version detected. Host: %s version: %s", host.GetName(), version)
	return nil
}

// getReconcileShardsWorkersNum calculates how many workers are allowed to be used for concurrent shard reconcile
func (w *worker) getReconcileShardsWorkersNum(shards []*api.ChiShard, opts *common.ReconcileShardsAndHostsOptions) int {
	availableWorkers := float64(chop.Config().Reconcile.Runtime.ReconcileShardsThreadsNumber)
	maxConcurrencyPercent := float64(chop.Config().Reconcile.Runtime.ReconcileShardsMaxConcurrencyPercent)
	_100Percent := float64(100)
	shardsNum := float64(len(shards))

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
	var errLock sync.Mutex
	for i := 0; i < workersNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rq := range ch {
				w.a.V(1).Info("Starting shard index: %d on worker", rq.index)
				if e := w.reconcileShardWithHosts(ctx, rq.shard); e != nil {
					errLock.Lock()
					err = e
					errLock.Unlock()
				}
			}
		}()
	}

	w.a.V(1).Info("Starting to wait shards from index: %d on workers.", startShardIndex)
	wg.Wait()
	w.a.V(1).Info("Finished to wait shards from index: %d on workers.", startShardIndex)
	return err
}

func (w *worker) runConcurrentlyInBatches(ctx context.Context, workersNum int, start int, shards []*api.ChiShard) error {
	for startShardIndex := 0; startShardIndex < len(shards); startShardIndex += workersNum {
		endShardIndex := util.IncTopped(startShardIndex, workersNum, len(shards))
		concurrentlyProcessedShards := shards[startShardIndex:endShardIndex]
		w.a.V(1).Info("Starting shards from index: %d on workers. Shards indexes [%d:%d)", start+startShardIndex, start+startShardIndex, start+endShardIndex)

		// Processing error protected with mutex
		var err error
		var errLock sync.Mutex

		wg := sync.WaitGroup{}
		wg.Add(len(concurrentlyProcessedShards))
		// Launch shard concurrent processing
		for j := range concurrentlyProcessedShards {
			shard := concurrentlyProcessedShards[j]
			w.a.V(1).Info("Starting shard on worker. Shard index: %d", start+startShardIndex+j)
			go func() {
				defer wg.Done()
				w.a.V(1).Info("Starting shard on goroutine. Shard index: %d", start+startShardIndex+j)
				if e := w.reconcileShardWithHosts(ctx, shard); e != nil {
					errLock.Lock()
					err = e
					errLock.Unlock()
				}
				w.a.V(1).Info("Finished shard on goroutine. Shard index: %d", start+startShardIndex+j)
			}()
		}
		w.a.V(1).Info("Starting to wait shards from index: %d on workers. Shards indexes [%d:%d)", start+startShardIndex, start+startShardIndex, start+endShardIndex)
		wg.Wait()
		w.a.V(1).Info("Finished to wait shards from index: %d on workers. Shards indexes [%d:%d)", start+startShardIndex, start+startShardIndex, start+endShardIndex)
		if err != nil {
			w.a.V(1).Warning("Skipping rest of shards due to an error: %v", err)
			return err
		}
	}
	return nil
}

func (w *worker) hostPVCsDataLossDetected(host *api.Host) (*statefulset.ReconcileOptions, *migrateTableOptions) {
	w.a.V(1).
		M(host).F().
		Info("Data loss detected for host: %s. Will do force data recovery", host.GetName())

	// In case of data loss detection on existing volumes, we need to:
	// 1. recreate StatefulSet
	// 2. run tables migration again
	return statefulset.NewReconcileStatefulSetOptions().SetForceRecreate(), &migrateTableOptions{
		forceMigrate: true,
		dropReplica:  true,
	}
}
