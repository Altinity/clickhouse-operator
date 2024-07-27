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

package poller

import (
	"context"

	apps "k8s.io/api/apps/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type StatefulSetPoller struct {
	kubeSTS interfaces.IKubeSTS
}

func NewStatefulSetPoller(kube interfaces.IKube) *StatefulSetPoller {
	return &StatefulSetPoller{
		kubeSTS: kube.STS(),
	}
}

// pollHostStatefulSet polls host's StatefulSet
func (poller *StatefulSetPoller) PollHostStatefulSet(
	ctx context.Context,
	host *api.Host,
	opts *Options,
	isDoneFn func(context.Context, *apps.StatefulSet) bool,
	backFn func(context.Context),
) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	if opts == nil {
		opts = NewPollerOptions().FromConfig(chop.Config())
	}

	namespace := host.Runtime.Address.Namespace
	name := host.Runtime.Address.StatefulSet

	return Poll(
		ctx,
		namespace, name,
		opts,
		&Functions{
			Get: func(_ctx context.Context) (any, error) {
				return poller.kubeSTS.Get(host)
			},
			IsDone: func(_ctx context.Context, a any) bool {
				return isDoneFn(_ctx, a.(*apps.StatefulSet))
			},
			ShouldContinue: func(_ctx context.Context, _ any, e error) bool {
				return apiErrors.IsNotFound(e)
			},
		},
		&BackgroundFunctions{
			F: backFn,
		},
	)
}
