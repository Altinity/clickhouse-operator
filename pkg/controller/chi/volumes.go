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
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer"
	core "k8s.io/api/core/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller"
)

func (c *Controller) walkPVCs(host *api.Host, f func(pvc *core.PersistentVolumeClaim)) {
	namespace := host.Runtime.Address.Namespace
	name := namer.Name(namer.NamePod, host)
	pod, err := c.kubeClient.CoreV1().Pods(namespace).Get(controller.NewContext(), name, controller.NewGetOptions())
	if err != nil {
		log.M(host).F().Error("FAIL get pod for host %s/%s err:%v", namespace, host.GetName(), err)
		return
	}

	for i := range pod.Spec.Volumes {
		volume := &pod.Spec.Volumes[i]
		if volume.PersistentVolumeClaim == nil {
			continue
		}

		pvcName := volume.PersistentVolumeClaim.ClaimName
		pvc, err := c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(controller.NewContext(), pvcName, controller.NewGetOptions())
		if err != nil {
			log.M(host).F().Error("FAIL get PVC %s/%s for the host %s/%s with err:%v", namespace, pvcName, namespace, host.GetName(), err)
			continue
		}

		f(pvc)
	}
}

func (c *Controller) walkDiscoveredPVCs(host *api.Host, f func(pvc *core.PersistentVolumeClaim)) {
	namespace := host.Runtime.Address.Namespace

	pvcList, err := c.kubeClient.
		CoreV1().
		PersistentVolumeClaims(namespace).
		List(controller.NewContext(), controller.NewListOptions(c.labeler(host.GetCR()).GetSelectorHostScope(host)))
	if err != nil {
		log.M(host).F().Error("FAIL get list of PVCs for the host %s/%s err:%v", namespace, host.GetName(), err)
		return
	}

	for i := range pvcList.Items {
		// Convenience wrapper
		pvc := &pvcList.Items[i]

		f(pvc)
	}
}

// Comment out PV
//func (c *Controller) walkPVs(host *api.Host, f func(pv *core.PersistentVolume)) {
//	c.walkPVCs(host, func(pvc *core.PersistentVolumeClaim) {
//		pv, err := c.kubeClient.CoreV1().PersistentVolumes().Get(newContext(), pvc.Spec.VolumeName, newGetOptions())
//		if err != nil {
//			log.M(host).F().Error("FAIL get PV %s err:%v", pvc.Spec.VolumeName, err)
//			return
//		}
//		f(pv)
//	})
//}
