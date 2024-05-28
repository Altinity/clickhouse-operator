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
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/model/common/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
)

type HostPoller struct {
	*common.StatefulSetPoller
	*Labeler
	interfaces.IKubeSTS
}

func NewHostPoller(poller *common.StatefulSetPoller, labeler *Labeler, kube interfaces.IKube) *HostPoller {
	return &HostPoller{
		StatefulSetPoller: poller,
		Labeler:           labeler,
		IKubeSTS:          kube.STS(),
	}
}

// waitHostReady polls host's StatefulSet until it is ready
func (c *HostPoller) WaitHostReady(ctx context.Context, host *api.Host) error {
	// Wait for StatefulSet to reach generation
	err := c.PollHostStatefulSet(
		ctx,
		host,
		nil, // rely on default options
		func(_ctx context.Context, sts *apps.StatefulSet) bool {
			if sts == nil {
				return false
			}
			_ = c.deleteLabelReadyOnPod(_ctx, host)
			_ = c.deleteAnnotationReadyOnService(_ctx, host)
			return k8s.IsStatefulSetGeneration(sts, sts.Generation)
		},
		func(_ctx context.Context) {
			_ = c.deleteLabelReadyOnPod(_ctx, host)
			_ = c.deleteAnnotationReadyOnService(_ctx, host)
		},
	)
	if err != nil {
		return err
	}

	// Wait StatefulSet to reach ready status
	err = c.PollHostStatefulSet(
		ctx,
		host,
		nil, // rely on default options
		func(_ctx context.Context, sts *apps.StatefulSet) bool {
			_ = c.deleteLabelReadyOnPod(_ctx, host)
			_ = c.deleteAnnotationReadyOnService(_ctx, host)
			return k8s.IsStatefulSetReady(sts)
		},
		func(_ctx context.Context) {
			_ = c.deleteLabelReadyOnPod(_ctx, host)
			_ = c.deleteAnnotationReadyOnService(_ctx, host)
		},
	)

	return err
}

// waitHostNotReady polls host's StatefulSet for not exists or not ready
func (c *HostPoller) WaitHostNotReady(ctx context.Context, host *api.Host) error {
	err := c.PollHostStatefulSet(
		ctx,
		host,
		// Since we are waiting for host to be nopt readylet's assyme that it should exist already
		// and thus let's set GetErrorTimeout to zero, since we are not expecting getter function
		// to return any errors
		common.NewPollerOptions().
			FromConfig(chop.Config()).
			SetGetErrorTimeout(0),
		func(_ context.Context, sts *apps.StatefulSet) bool {
			return k8s.IsStatefulSetNotReady(sts)
		},
		nil,
	)
	if apiErrors.IsNotFound(err) {
		err = nil
	}

	return err
}

// waitHostDeleted polls host's StatefulSet until it is not available
func (c *HostPoller) WaitHostDeleted(host *api.Host) {
	for {
		// TODO
		// Probably there would be better way to wait until k8s reported StatefulSet deleted
		if _, err := c.IKubeSTS.Get(host); err == nil {
			log.V(2).Info("cache NOT yet synced")
			time.Sleep(15 * time.Second)
		} else {
			log.V(1).Info("cache synced")
			return
		}
	}
}
