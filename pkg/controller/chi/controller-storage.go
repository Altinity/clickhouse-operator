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
	"fmt"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/model/common/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func (c *Controller) kubePVCGet(ctx context.Context, namespace, name string) (*core.PersistentVolumeClaim, error) {
	return c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, controller.NewGetOptions())
}

func (c *Controller) kubePVCDelete(ctx context.Context, namespace, name string) error {
	return c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, name, controller.NewDeleteOptions())
}

// updateOrCreatePVC
func (c *Controller) updateOrCreatePVC(ctx context.Context, pvc *core.PersistentVolumeClaim) (*core.PersistentVolumeClaim, error) {
	log.V(2).M(pvc).F().P()
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil, fmt.Errorf("task is done")
	}

	_, err := c.kubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, controller.NewGetOptions())
	if err != nil {
		if apiErrors.IsNotFound(err) {
			// This is not an error per se, means PVC is not created (yet)?
			_, err = c.kubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Create(ctx, pvc, controller.NewCreateOptions())
			if err != nil {
				log.V(1).M(pvc).F().Error("unable to Create PVC err: %v", err)
			}
			return pvc, err
		}
		// In case of any non-NotFound API error - unable to proceed
		log.V(1).M(pvc).F().Error("ERROR unable to get PVC(%s/%s) err: %v", pvc.Namespace, pvc.Name, err)
		return nil, err
	}

	pvcUpdated, err := c.kubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, controller.NewUpdateOptions())
	if err == nil {
		return pvcUpdated, err
	}

	// Update failed
	// May want to suppress special case of an error
	//if strings.Contains(err.Error(), "field can not be less than previous value") {
	//	return pvc, nil
	//}
	log.V(1).M(pvc).F().Error("unable to Update PVC err: %v", err)
	return nil, err
}

// deletePVC deletes PersistentVolumeClaim
func (c *Controller) deletePVC(ctx context.Context, host *api.Host) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	log.V(2).M(host).S().P()
	defer log.V(2).M(host).E().P()

	namespace := host.Runtime.Address.Namespace
	c.walkDiscoveredPVCs(host, func(pvc *core.PersistentVolumeClaim) {
		if util.IsContextDone(ctx) {
			log.V(2).Info("task is done")
			return
		}

		// Check whether PVC can be deleted
		if c.pvcDeleter.HostCanDeletePVC(host, pvc.Name) {
			log.V(1).M(host).Info("PVC %s/%s would be deleted", namespace, pvc.Name)
		} else {
			log.V(1).M(host).Info("PVC %s/%s should not be deleted, leave it intact", namespace, pvc.Name)
			// Move to the next PVC
			return
		}

		// Delete PVC
		if err := c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, controller.NewDeleteOptions()); err == nil {
			log.V(1).M(host).Info("OK delete PVC %s/%s", namespace, pvc.Name)
		} else if apiErrors.IsNotFound(err) {
			log.V(1).M(host).Info("NEUTRAL not found PVC %s/%s", namespace, pvc.Name)
		} else {
			log.M(host).F().Error("FAIL to delete PVC %s/%s err:%v", namespace, pvc.Name, err)
		}
	})

	return nil
}

// Comment out PV
// updatePersistentVolume
//func (c *Controller) updatePersistentVolume(ctx context.Context, pv *core.PersistentVolume) (*core.PersistentVolume, error) {
//	log.V(2).M(pv).F().P()
//	if util.IsContextDone(ctx) {
//		log.V(2).Info("task is done")
//		return nil, fmt.Errorf("task is done")
//	}
//
//	var err error
//	pv, err = c.kubeClient.CoreV1().PersistentVolumes().Update(ctx, pv, newUpdateOptions())
//	if err != nil {
//		// Update failed
//		log.V(1).M(pv).F().Error("%v", err)
//		return nil, err
//	}
//
//	return pv, err
//}

func (c *Controller) walkPVCs(host *api.Host, f func(pvc *core.PersistentVolumeClaim)) {
	namespace := host.Runtime.Address.Namespace
	name := c.namer.Name(interfaces.NamePod, host)
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
		List(controller.NewContext(), controller.NewListOptions(c.labeler(host.GetCR()).Selector(interfaces.SelectorHostScope, host)))
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
