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
	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopmodel "github.com/altinity/clickhouse-operator/pkg/model"
)

func (c *Controller) walkPVCs(host *chop.ChiHost, f func(pvc *v1.PersistentVolumeClaim)) {
	namespace := host.Address.Namespace
	name := chopmodel.CreatePodName(host)
	pod, err := c.kubeClient.CoreV1().Pods(namespace).Get(newContext(), name, newGetOptions())
	if err != nil {
		log.M(host).F().Error("FAIL get pod for host %s/%s err:%v", namespace, host.Name, err)
		return
	}

	for i := range pod.Spec.Volumes {
		volume := &pod.Spec.Volumes[i]
		if volume.PersistentVolumeClaim == nil {
			continue
		}

		pvcName := volume.PersistentVolumeClaim.ClaimName
		pvc, err := c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(newContext(), pvcName, newGetOptions())
		if err != nil {
			log.M(host).F().Error("FAIL get PVC %s/%s err:%v", namespace, pvcName, err)
			continue
		}

		f(pvc)
	}
}

func (c *Controller) walkDiscoveredPVCs(host *chop.ChiHost, f func(pvc *v1.PersistentVolumeClaim)) {
	namespace := host.Address.Namespace

	pvcList, err := c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).List(newContext(), newListOptions(chopmodel.GetSelectorHostScope(host)))
	if err != nil {
		log.M(host).F().Error("FAIL get list of PVC for host %s/%s err:%v", namespace, host.Name, err)
		return
	}

	for i := range pvcList.Items {
		// Convenience wrapper
		pvc := &pvcList.Items[i]

		f(pvc)
	}
}

func (c *Controller) walkPVs(host *chop.ChiHost, f func(pv *v1.PersistentVolume)) {
	c.walkPVCs(host, func(pvc *v1.PersistentVolumeClaim) {
		pv, err := c.kubeClient.CoreV1().PersistentVolumes().Get(newContext(), pvc.Spec.VolumeName, newGetOptions())
		if err != nil {
			log.M(host).F().Error("FAIL get PV %s err:%v", pvc.Spec.VolumeName, err)
			return
		}
		f(pv)
	})
}
