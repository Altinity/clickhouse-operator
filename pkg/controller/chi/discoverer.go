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

	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chiV1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	model "github.com/altinity/clickhouse-operator/pkg/model/chi"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func (c *Controller) discovery(ctx context.Context, chi *chiV1.ClickHouseInstallation) *model.Registry {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	opts := controller.NewListOptions(model.NewLabeler(chi).GetSelectorCHIScope())
	r := model.NewRegistry()
	c.discoveryStatefulSets(ctx, r, chi, opts)
	c.discoveryConfigMaps(ctx, r, chi, opts)
	c.discoveryServices(ctx, r, chi, opts)
	c.discoverySecrets(ctx, r, chi, opts)
	c.discoveryPVCs(ctx, r, chi, opts)
	// Comment out PV
	//c.discoveryPVs(ctx, r, chi, opts)
	c.discoveryPDBs(ctx, r, chi, opts)
	return r
}

func (c *Controller) discoveryStatefulSets(ctx context.Context, r *model.Registry, chi *chiV1.ClickHouseInstallation, opts metaV1.ListOptions) {
	list, err := c.kubeClient.AppsV1().StatefulSets(chi.Namespace).List(ctx, opts)
	if err != nil {
		log.M(chi).F().Error("FAIL list StatefulSet err: %v", err)
		return
	}
	if list == nil {
		log.M(chi).F().Error("FAIL list StatefulSet list is nil")
		return
	}
	for _, obj := range list.Items {
		r.RegisterStatefulSet(obj.ObjectMeta)
	}
}

func (c *Controller) discoveryConfigMaps(ctx context.Context, r *model.Registry, chi *chiV1.ClickHouseInstallation, opts metaV1.ListOptions) {
	list, err := c.kubeClient.CoreV1().ConfigMaps(chi.Namespace).List(ctx, opts)
	if err != nil {
		log.M(chi).F().Error("FAIL list ConfigMap err: %v", err)
		return
	}
	if list == nil {
		log.M(chi).F().Error("FAIL list ConfigMap list is nil")
		return
	}
	for _, obj := range list.Items {
		r.RegisterConfigMap(obj.ObjectMeta)
	}
}

func (c *Controller) discoveryServices(ctx context.Context, r *model.Registry, chi *chiV1.ClickHouseInstallation, opts metaV1.ListOptions) {
	list, err := c.kubeClient.CoreV1().Services(chi.Namespace).List(ctx, opts)
	if err != nil {
		log.M(chi).F().Error("FAIL list Service err: %v", err)
		return
	}
	if list == nil {
		log.M(chi).F().Error("FAIL list Service list is nil")
		return
	}
	for _, obj := range list.Items {
		r.RegisterService(obj.ObjectMeta)
	}
}

func (c *Controller) discoverySecrets(ctx context.Context, r *model.Registry, chi *chiV1.ClickHouseInstallation, opts metaV1.ListOptions) {
	list, err := c.kubeClient.CoreV1().Secrets(chi.Namespace).List(ctx, opts)
	if err != nil {
		log.M(chi).F().Error("FAIL list Secret err: %v", err)
		return
	}
	if list == nil {
		log.M(chi).F().Error("FAIL list Secret list is nil")
		return
	}
	for _, obj := range list.Items {
		r.RegisterSecret(obj.ObjectMeta)
	}
}

func (c *Controller) discoveryPVCs(ctx context.Context, r *model.Registry, chi *chiV1.ClickHouseInstallation, opts metaV1.ListOptions) {
	list, err := c.kubeClient.CoreV1().PersistentVolumeClaims(chi.Namespace).List(ctx, opts)
	if err != nil {
		log.M(chi).F().Error("FAIL list PVC err: %v", err)
		return
	}
	if list == nil {
		log.M(chi).F().Error("FAIL list PVC list is nil")
		return
	}
	for _, obj := range list.Items {
		r.RegisterPVC(obj.ObjectMeta)
	}
}

// Comment out PV
//func (c *Controller) discoveryPVs(ctx context.Context, r *chopModel.Registry, chi *chiV1.ClickHouseInstallation, opts metaV1.ListOptions) {
//	list, err := c.kubeClient.CoreV1().PersistentVolumes().List(ctx, opts)
//	if err != nil {
//		log.M(chi).F().Error("FAIL list PV err: %v", err)
//		return
//	}
//	if list == nil {
//		log.M(chi).F().Error("FAIL list PV list is nil")
//		return
//	}
//	for _, obj := range list.Items {
//		r.RegisterPV(obj.ObjectMeta)
//	}
//}

func (c *Controller) discoveryPDBs(ctx context.Context, r *model.Registry, chi *chiV1.ClickHouseInstallation, opts metaV1.ListOptions) {
	list, err := c.kubeClient.PolicyV1().PodDisruptionBudgets(chi.Namespace).List(ctx, opts)
	if err != nil {
		log.M(chi).F().Error("FAIL list PDB err: %v", err)
		return
	}
	if list == nil {
		log.M(chi).F().Error("FAIL list PDB list is nil")
		return
	}
	for _, obj := range list.Items {
		r.RegisterPDB(obj.ObjectMeta)
	}
}
