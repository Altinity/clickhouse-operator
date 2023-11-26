// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
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
	"time"

	apps "k8s.io/api/apps/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	model "github.com/altinity/clickhouse-operator/pkg/model/chi"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// waitHostNotReady polls host's StatefulSet for not exists or not ready
func (c *Controller) waitHostNotReady(ctx context.Context, host *api.ChiHost) error {
	err := c.pollHostStatefulSet(
		ctx,
		host,
		controller.NewPollerOptions().
			FromConfig(chop.Config()).
			SetGetErrorTimeout(0),
		func(_ context.Context, sts *apps.StatefulSet) bool {
			return model.IsStatefulSetNotReady(sts)
		},
		nil,
	)
	if apiErrors.IsNotFound(err) {
		err = nil
	}

	return err
}

// waitHostReady polls host's StatefulSet until it is ready
func (c *Controller) waitHostReady(ctx context.Context, host *api.ChiHost) error {
	// Wait for StatefulSet to reach generation
	err := c.pollHostStatefulSet(
		ctx,
		host,
		nil,
		func(_ctx context.Context, sts *apps.StatefulSet) bool {
			if sts == nil {
				return false
			}
			_ = c.deleteLabelReadyPod(_ctx, host)
			_ = c.deleteAnnotationReadyService(_ctx, host)
			return model.IsStatefulSetGeneration(sts, sts.Generation)
		},
		func(_ctx context.Context) {
			_ = c.deleteLabelReadyPod(_ctx, host)
			_ = c.deleteAnnotationReadyService(_ctx, host)
		},
	)
	if err != nil {
		return err
	}

	// Wait StatefulSet to reach ready status
	return c.pollHostStatefulSet(
		ctx,
		host,
		nil,
		func(_ctx context.Context, sts *apps.StatefulSet) bool {
			_ = c.deleteLabelReadyPod(_ctx, host)
			_ = c.deleteAnnotationReadyService(_ctx, host)
			return model.IsStatefulSetReady(sts)
		},
		func(_ctx context.Context) {
			_ = c.deleteLabelReadyPod(_ctx, host)
			_ = c.deleteAnnotationReadyService(_ctx, host)
		},
	)
}

// waitHostDeleted polls host's StatefulSet until it is not available
func (c *Controller) waitHostDeleted(host *api.ChiHost) {
	for {
		// TODO
		// Probably there would be better way to wait until k8s reported StatefulSet deleted
		if _, err := c.getStatefulSet(host); err == nil {
			log.V(2).Info("cache NOT yet synced")
			time.Sleep(15 * time.Second)
		} else {
			log.V(1).Info("cache synced")
			return
		}
	}
}

// pollHost polls host
func (c *Controller) pollHost(
	ctx context.Context,
	host *api.ChiHost,
	opts *controller.PollerOptions,
	isDoneFn func(ctx context.Context, host *api.ChiHost) bool,
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	opts = opts.Ensure().FromConfig(chop.Config())
	namespace := host.Address.Namespace
	name := host.Address.HostName

	return controller.Poll(
		ctx,
		namespace, name,
		opts,
		&controller.PollerFunctions{
			IsDone: func(_ctx context.Context, _ any) bool {
				return isDoneFn(_ctx, host)
			},
		},
		nil,
	)
}

// pollHostStatefulSet polls host's StatefulSet
func (c *Controller) pollHostStatefulSet(
	ctx context.Context,
	host *api.ChiHost,
	opts *controller.PollerOptions,
	isDoneFn func(context.Context, *apps.StatefulSet) bool,
	backFn func(context.Context),
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	namespace := host.Address.Namespace
	name := host.Address.StatefulSet

	return controller.Poll(
		ctx,
		namespace, name,
		opts,
		&controller.PollerFunctions{
			Get: func(_ctx context.Context) (any, error) {
				return c.getStatefulSet(host)
			},
			IsDone: func(_ctx context.Context, a any) bool {
				return isDoneFn(_ctx, a.(*apps.StatefulSet))
			},
			ShouldContinue: func(_ctx context.Context, _ any, e error) bool {
				return apiErrors.IsNotFound(e)
			},
		},
		&controller.PollerBackgroundFunctions{
			F: backFn,
		},
	)
}
