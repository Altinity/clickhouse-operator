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

package domain

import (
	"context"

	apps "k8s.io/api/apps/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
)

type readyMarkDeleter interface {
	DeleteReadyMarkOnPodAndService(ctx context.Context, host *api.Host) error
}

// HostStatefulSetPoller enriches StatefulSet poller with host capabilities
type HostStatefulSetPoller struct {
	*HostK8SObjectPoller
	readyMarkDeleter
}

// NewHostStatefulSetPoller creates new HostStatefulSetPoller from StatefulSet poller
func NewHostStatefulSetPoller(poller *HostK8SObjectPoller, readyMarkDeleter readyMarkDeleter) *HostStatefulSetPoller {
	return &HostStatefulSetPoller{
		HostK8SObjectPoller: poller,
		readyMarkDeleter:    readyMarkDeleter,
	}
}

// WaitHostStatefulSetReady polls host's StatefulSet until it is ready
func (p *HostStatefulSetPoller) WaitHostStatefulSetReady(ctx context.Context, host *api.Host) error {
	log.V(2).F().Info("Wait for StatefulSet to reach target generation")
	err := p.HostK8SObjectPoller.Poll(
		ctx,
		host,
		func(_ctx context.Context, sts *apps.StatefulSet) bool {
			if sts == nil {
				return false
			}
			_ = p.readyMarkDeleter.DeleteReadyMarkOnPodAndService(_ctx, host)
			return k8s.IsStatefulSetReconcileCompleted(sts)
		},
	)
	if err != nil {
		log.V(1).F().Warning("FAILED wait for StatefulSet to reach generation")
		return err
	}

	log.V(2).F().Info("Wait StatefulSet to reach ready status")
	err = p.HostK8SObjectPoller.Poll(
		ctx,
		host,
		func(_ctx context.Context, sts *apps.StatefulSet) bool {
			_ = p.readyMarkDeleter.DeleteReadyMarkOnPodAndService(_ctx, host)
			return k8s.IsStatefulSetReady(sts)
		},
	)
	if err != nil {
		log.V(1).F().Warning("FAILED wait StatefulSet to reach ready status")
		return err
	}

	return nil
}
