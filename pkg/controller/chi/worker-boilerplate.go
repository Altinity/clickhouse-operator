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

	utilRuntime "k8s.io/apimachinery/pkg/util/runtime"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi/cmd_queue"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi/metrics"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// run is an endless work loop, expected to be run in a thread
func (w *worker) run() {
	w.a.V(2).S().P()
	defer w.a.V(2).E().P()

	// For system thread let's wait its 'official start time', thus giving it time to bootstrap
	util.WaitContextDoneUntil(context.Background(), w.start)

	// Events loop
	for {
		// Get() blocks until it can return an item
		item, ctx, ok := w.queue.Get()
		if !ok {
			w.a.Info("shutdown request")
			return
		}

		//item, shut := w.queue.Get()
		//task := context.Background()
		//if shut {
		//	w.a.Info("shutdown request")
		//	return
		//}

		if err := w.processItem(ctx, item); err != nil {
			// Item not processed
			// this code cannot return an error and needs to indicate error has been ignored
			utilRuntime.HandleError(err)
		}

		// Forget indicates that an item is finished being retried.  Doesn't matter whether its for perm failing
		// or for success, we'll stop the rate limiter from tracking it.  This only clears the `rateLimiter`, you
		// still have to call `Done` on the queue.
		//w.queue.Forget(item)

		// Remove item from processing set when processing completed
		w.queue.Done(item)
	}
}

func (w *worker) processReconcileCHI(ctx context.Context, cmd *cmd_queue.ReconcileCHI) error {
	switch cmd.Cmd {
	case cmd_queue.ReconcileAdd:
		return w.updateCHI(ctx, nil, cmd.New)
	case cmd_queue.ReconcileUpdate:
		return w.updateCHI(ctx, cmd.Old, cmd.New)
	case cmd_queue.ReconcileDelete:
		return w.discoveryAndDeleteCR(ctx, cmd.Old)
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilRuntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", cmd))
	return nil
}

func (w *worker) processReconcileCHIT(cmd *cmd_queue.ReconcileCHIT) error {
	switch cmd.Cmd {
	case cmd_queue.ReconcileAdd:
		return w.addChit(cmd.New)
	case cmd_queue.ReconcileUpdate:
		return w.updateChit(cmd.Old, cmd.New)
	case cmd_queue.ReconcileDelete:
		return w.deleteChit(cmd.Old)
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilRuntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", cmd))
	return nil
}

func (w *worker) processReconcileChopConfig(cmd *cmd_queue.ReconcileChopConfig) error {
	switch cmd.Cmd {
	case cmd_queue.ReconcileAdd:
		return w.c.addChopConfig(cmd.New)
	case cmd_queue.ReconcileUpdate:
		return w.c.updateChopConfig(cmd.Old, cmd.New)
	case cmd_queue.ReconcileDelete:
		return w.c.deleteChopConfig(cmd.Old)
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilRuntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", cmd))
	return nil
}

func (w *worker) processReconcileEndpoints(ctx context.Context, cmd *cmd_queue.ReconcileEndpoints) error {
	switch cmd.Cmd {
	case cmd_queue.ReconcileUpdate:
		w.a.V(1).M(cmd.New).F().Info("Reconcile Endpoints. %s/%s", cmd.New.Namespace, cmd.New.Name)
		return w.updateEndpoints(ctx, cmd.New.GetObjectMeta())
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilRuntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", cmd))
	return nil
}

func (w *worker) processReconcileEndpointSlice(ctx context.Context, cmd *cmd_queue.ReconcileEndpointSlice) error {
	switch cmd.Cmd {
	case cmd_queue.ReconcileUpdate:
		w.a.V(1).M(cmd.New).F().Info("Reconcile EndpointSlice. %s/%s Transition: '%s'=>'%s'", cmd.New.Namespace, cmd.New.Name, buildComparableEndpointAddresses(cmd.Old), buildComparableEndpointAddresses(cmd.New))
		return w.updateEndpoints(ctx, cmd.New.GetObjectMeta())
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilRuntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", cmd))
	return nil
}

func (w *worker) processReconcilePod(ctx context.Context, cmd *cmd_queue.ReconcilePod) error {
	switch cmd.Cmd {
	case cmd_queue.ReconcileAdd:
		w.a.V(1).M(cmd.New).F().Info("Add Pod. %s/%s", cmd.New.Namespace, cmd.New.Name)
		metrics.PodAdd(ctx)
		return nil
	case cmd_queue.ReconcileUpdate:
		//ignore
		//w.a.V(1).M(cmd.new).F().Info("Update Pod. %s/%s", cmd.new.Namespace, cmd.new.Name)
		//metricsPodUpdate(ctx)
		return nil
	case cmd_queue.ReconcileDelete:
		w.a.V(1).M(cmd.Old).F().Info("Delete Pod. %s/%s", cmd.Old.Namespace, cmd.Old.Name)
		metrics.PodDelete(ctx)
		return nil
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilRuntime.HandleError(fmt.Errorf("unexpected reconcile - %#v", cmd))
	return nil
}

// processItem processes one work item according to its type
func (w *worker) processItem(ctx context.Context, item interface{}) error {
	if util.IsContextDone(ctx) {
		log.V(1).Info("Reconcile is aborted")
		return nil
	}

	w.a.V(3).S().P()
	defer w.a.V(3).E().P()

	switch cmd := item.(type) {
	case *cmd_queue.ReconcileCHI:
		return w.processReconcileCHI(ctx, cmd)
	case *cmd_queue.ReconcileCHIT:
		return w.processReconcileCHIT(cmd)
	case *cmd_queue.ReconcileChopConfig:
		return w.processReconcileChopConfig(cmd)
	case *cmd_queue.ReconcileEndpoints:
		return w.processReconcileEndpoints(ctx, cmd)
	case *cmd_queue.ReconcileEndpointSlice:
		return w.processReconcileEndpointSlice(ctx, cmd)
	case *cmd_queue.ReconcilePod:
		return w.processReconcilePod(ctx, cmd)
	}

	// Unknown item type, don't know what to do with it
	// Just skip it and behave like it never existed
	utilRuntime.HandleError(fmt.Errorf("unexpected item in the queue - %#v", item))
	return nil
}
