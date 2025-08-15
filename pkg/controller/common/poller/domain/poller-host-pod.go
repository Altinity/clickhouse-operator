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
	core "k8s.io/api/core/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
)

type readyMarkDeleter interface {
	DeleteReadyMarkOnPodAndService(ctx context.Context, host *api.Host) error
}

type HostPodPoller struct {
	stsPoller        *HostObjectPoller[apps.StatefulSet]
	podPoller        *HostObjectPoller[core.Pod]
	readyMarkDeleter readyMarkDeleter
}

// NewHostPodPoller creates new HostPodPoller
func NewHostPodPoller(
	stsPoller *HostObjectPoller[apps.StatefulSet],
	podPoller *HostObjectPoller[core.Pod],
	readyMarkDeleter readyMarkDeleter,
) *HostPodPoller {
	return &HostPodPoller{
		stsPoller:        stsPoller,
		podPoller:        podPoller,
		readyMarkDeleter: readyMarkDeleter,
	}
}

// WaitHostStatefulSetReady polls host's StatefulSet until it is ready
func (p *HostPodPoller) WaitHostPodStarted(ctx context.Context, host *api.Host) error {
	log.V(2).F().Info("Wait for StatefulSet to reach target generation")
	err := p.stsPoller.Poll(
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

	log.V(2).F().Info("Wait Pod to reach started status")
	err = p.podPoller.Poll(
		ctx,
		host,
		func(_ctx context.Context, pod *core.Pod) bool {
			_ = p.readyMarkDeleter.DeleteReadyMarkOnPodAndService(_ctx, host)
			return k8s.PodHasAllContainersStarted(pod)
		},
	)
	if err != nil {
		log.V(1).F().Warning("FAILED wait Pod to reach started status")
		return err
	}

	return nil
}

// WaitHostStatefulSetReady polls host's StatefulSet until it is ready
func (p *HostPodPoller) WaitHostStatefulSetReady(ctx context.Context, host *api.Host) error {
	log.V(2).F().Info("Wait for StatefulSet to reach target generation")
	err := p.stsPoller.Poll(
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
	err = p.stsPoller.Poll(
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
