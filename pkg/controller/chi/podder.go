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
	"k8s.io/api/core/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// walkContainers walks with specified func over all containers of the specified host
func (c *Controller) walkContainers(host *api.Host, f func(container *v1.Container)) {
	pod, err := c.getPod(host)
	if err != nil {
		log.M(host).F().Error("FAIL get pod for host '%s' err: %v", host.Runtime.Address.NamespaceNameString(), err)
		return
	}

	for i := range pod.Spec.Containers {
		container := &pod.Spec.Containers[i]
		f(container)
	}
}

// walkContainerStatuses walks with specified func over all statuses of the specified host
func (c *Controller) walkContainerStatuses(host *api.Host, f func(status *v1.ContainerStatus)) {
	pod, err := c.getPod(host)
	if err != nil {
		log.M(host).F().Error("FAIL get pod for host %s err:%v", host.Runtime.Address.NamespaceNameString(), err)
		return
	}

	for i := range pod.Status.ContainerStatuses {
		status := &pod.Status.ContainerStatuses[i]
		f(status)
	}
}

// isHostRunning checks whether ALL containers of the specified host are running
func (c *Controller) isHostRunning(host *api.Host) bool {
	all := true
	c.walkContainerStatuses(host, func(status *v1.ContainerStatus) {
		if status.State.Running == nil {
			all = false
		}
	})
	return all
}
