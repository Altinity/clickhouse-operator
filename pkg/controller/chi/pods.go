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

	log "github.com/golang/glog"
	// log "k8s.io/klog"

	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopmodel "github.com/altinity/clickhouse-operator/pkg/model"
)

func (c *Controller) walkContainers(host *chop.ChiHost, f func(container *v1.Container)) {
	namespace := host.Address.Namespace
	name := chopmodel.CreatePodName(host)
	pod, err := c.kubeClient.CoreV1().Pods(namespace).Get(name, newGetOptions())
	if err != nil {
		log.Errorf("FAIL get pod for host %s/%s err:%v", namespace, host.Name, err)
		return
	}

	for i := range pod.Spec.Containers {
		container := &pod.Spec.Containers[i]
		f(container)
	}
}

func (c *Controller) walkContainerStatuses(host *chop.ChiHost, f func(status *v1.ContainerStatus)) {
	namespace := host.Address.Namespace
	name := chopmodel.CreatePodName(host)
	pod, err := c.kubeClient.CoreV1().Pods(namespace).Get(name, newGetOptions())
	if err != nil {
		log.Errorf("FAIL get pod for host %s/%s err:%v", namespace, host.Name, err)
		return
	}

	for i := range pod.Status.ContainerStatuses {
		status := &pod.Status.ContainerStatuses[i]
		f(status)
	}
}
