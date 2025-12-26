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

package chk

import (
	"context"
	"sync"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/swversion"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
)

func (w *worker) getHostSoftwareVersion(ctx context.Context, host *api.Host) *swversion.SoftWareVersion {
	// Unable to acquire any version - report min one
	return swversion.MaxVersion().SetDescription("so far so")
}

// getReconcileShardsWorkersNum calculates how many workers are allowed to be used for concurrent shard reconcile
func (w *worker) getReconcileShardsWorkersNum(cluster *apiChk.Cluster, opts *common.ReconcileShardsAndHostsOptions) int {
	return 1
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

func (w *worker) runConcurrently(ctx context.Context, workersNum int, startShardIndex int, shards []*apiChk.ChkShard) error {
	if len(shards) == 0 {
		return nil
	}

	type shardReconcile struct {
		shard *apiChk.ChkShard
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

func (w *worker) hostPVCsDataLossDetectedOptions(host *api.Host) *statefulset.ReconcileOptions {
	w.a.V(1).
		M(host).F().
		Info("Data loss detected for host: %s. Will do force data recovery", host.GetName())

	// In case of data loss detection on existing volumes, we need to:
	// 1. recreate StatefulSet
	// 2. run tables migration again

	stsReconcileOpts := statefulset.NewReconcileStatefulSetOptions().SetForceRecreate()
	return stsReconcileOpts
}
