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
	"k8s.io/api/core/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopmodel "github.com/altinity/clickhouse-operator/pkg/model"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func (c *Controller) appendLabelReady(ctx context.Context, host *chop.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	pod, err := c.getPod(host)
	if err != nil {
		log.M(host).F().Error("FAIL get pod for host %s err:%v", host.Address.NamespaceNameString(), err)
		return err
	}

	chopmodel.AppendLabelReady(&pod.ObjectMeta)
	_, err = c.kubeClient.CoreV1().Pods(pod.Namespace).Update(ctx, pod, newUpdateOptions())
	if err != nil {
		log.M(host).F().Error("FAIL setting 'ready' label for host %s err:%v", host.Address.NamespaceNameString(), err)
		return err
	}
	return err
}

func (c *Controller) deleteLabelReady(ctx context.Context, host *chop.ChiHost) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	if host == nil {
		return nil
	}
	if host.StatefulSet.Spec.Replicas != nil {
		if *host.StatefulSet.Spec.Replicas == 0 {
			return nil
		}
	}

	pod, err := c.getPod(host)
	if err != nil {
		log.V(1).M(host).F().Info("FAIL get pod for host %s err:%v", host.Address.NamespaceNameString(), err)
		return err
	}

	chopmodel.DeleteLabelReady(&pod.ObjectMeta)
	_, err = c.kubeClient.CoreV1().Pods(pod.Namespace).Update(ctx, pod, newUpdateOptions())
	return err
}

func (c *Controller) walkContainers(host *chop.ChiHost, f func(container *v1.Container)) {
	pod, err := c.getPod(host)
	if err != nil {
		log.M(host).F().Error("FAIL get pod for host %s err:%v", host.Address.NamespaceNameString(), err)
		return
	}

	for i := range pod.Spec.Containers {
		container := &pod.Spec.Containers[i]
		f(container)
	}
}

func (c *Controller) walkContainerStatuses(host *chop.ChiHost, f func(status *v1.ContainerStatus)) {
	pod, err := c.getPod(host)
	if err != nil {
		log.M(host).F().Error("FAIL get pod for host %s err:%v", host.Address.NamespaceNameString(), err)
		return
	}

	for i := range pod.Status.ContainerStatuses {
		status := &pod.Status.ContainerStatuses[i]
		f(status)
	}
}

func (c *Controller) isHostRunning(host *chop.ChiHost) bool {
	all := true
	c.walkContainerStatuses(host, func(status *v1.ContainerStatus) {
		if status.State.Running == nil {
			all = false
		}
	})
	return all
}
