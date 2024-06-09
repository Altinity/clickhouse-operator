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

package storage

import (
	"context"
	"fmt"

	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/common/volume"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type PVC struct {
	interfaces.IKubePVC
	pvcDeleter *volume.PVCDeleter
}

func NewStoragePVC(pvcKube interfaces.IKubePVC) *PVC {
	return &PVC{
		IKubePVC:   pvcKube,
		pvcDeleter: volume.NewPVCDeleter(managers.NewNameManager(managers.NameManagerTypeClickHouse)),
	}
}

// UpdateOrCreate
func (c *PVC) UpdateOrCreate(ctx context.Context, pvc *core.PersistentVolumeClaim) (*core.PersistentVolumeClaim, error) {
	log.V(2).M(pvc).F().P()
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil, fmt.Errorf("task is done")
	}

	_, err := c.Get(ctx, pvc.Namespace, pvc.Name)
	if err != nil {
		if apiErrors.IsNotFound(err) {
			// This is not an error per se, means PVC is not created (yet)?
			_, err = c.Create(ctx, pvc)
			if err != nil {
				log.V(1).M(pvc).F().Error("unable to Create PVC err: %v", err)
			}
			return pvc, err
		}
		// In case of any non-NotFound API error - unable to proceed
		log.V(1).M(pvc).F().Error("ERROR unable to get PVC(%s/%s) err: %v", pvc.Namespace, pvc.Name, err)
		return nil, err
	}

	pvcUpdated, err := c.Update(ctx, pvc)
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
func (c *PVC) DeletePVC(ctx context.Context, host *api.Host) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	log.V(2).M(host).S().P()
	defer log.V(2).M(host).E().P()

	namespace := host.Runtime.Address.Namespace
	c.WalkDiscoveredPVCs(ctx, host, func(pvc *core.PersistentVolumeClaim) {
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
		if err := c.Delete(ctx, pvc.Namespace, pvc.Name); err == nil {
			log.V(1).M(host).Info("OK delete PVC %s/%s", namespace, pvc.Name)
		} else if apiErrors.IsNotFound(err) {
			log.V(1).M(host).Info("NEUTRAL not found PVC %s/%s", namespace, pvc.Name)
		} else {
			log.M(host).F().Error("FAIL to delete PVC %s/%s err:%v", namespace, pvc.Name, err)
		}
	})

	return nil
}

func (c *PVC) WalkDiscoveredPVCs(ctx context.Context, host *api.Host, f func(pvc *core.PersistentVolumeClaim)) {
	namespace := host.Runtime.Address.Namespace

	pvcList, err := c.ListForHost(ctx, host)
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
