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

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model"
	chiLabeler "github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func getLabeler(cr api.ICustomResource) interfaces.ILabeler {
	return chiLabeler.New(cr)
}

func (c *Controller) discovery(ctx context.Context, cr api.ICustomResource) *model.Registry {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	opts := controller.NewListOptions(getLabeler(cr).Selector(interfaces.SelectorCRScope))
	r := model.NewRegistry()
	c.discoveryStatefulSets(ctx, r, cr, opts)
	c.discoveryConfigMaps(ctx, r, cr, opts)
	c.discoveryServices(ctx, r, cr, opts)
	c.discoverySecrets(ctx, r, cr, opts)
	c.discoveryPVCs(ctx, r, cr, opts)
	// Comment out PV
	//c.discoveryPVs(ctx, r, chi, opts)
	c.discoveryPDBs(ctx, r, cr, opts)
	return r
}

func (c *Controller) discoveryStatefulSets(ctx context.Context, r *model.Registry, cr api.ICustomResource, opts meta.ListOptions) {
	list, err := c.kube.STS().List(ctx, cr.GetNamespace(), opts)
	if err != nil {
		log.M(cr).F().Error("FAIL to list StatefulSet - err: %v", err)
		return
	}
	if list == nil {
		log.M(cr).F().Error("FAIL to list StatefulSet - list is nil")
		return
	}
	for _, obj := range list {
		r.RegisterStatefulSet(obj.GetObjectMeta())
	}
}

func (c *Controller) discoveryConfigMaps(ctx context.Context, r *model.Registry, cr api.ICustomResource, opts meta.ListOptions) {
	list, err := c.kube.ConfigMap().List(ctx, cr.GetNamespace(), opts)
	if err != nil {
		log.M(cr).F().Error("FAIL to list ConfigMap - err: %v", err)
		return
	}
	if list == nil {
		log.M(cr).F().Error("FAIL to list ConfigMap - list is nil")
		return
	}
	for _, obj := range list {
		r.RegisterConfigMap(obj.GetObjectMeta())
	}
}

func (c *Controller) discoveryServices(ctx context.Context, r *model.Registry, cr api.ICustomResource, opts meta.ListOptions) {
	list, err := c.kube.Service().List(ctx, cr.GetNamespace(), opts)
	if err != nil {
		log.M(cr).F().Error("FAIL to list Service - err: %v", err)
		return
	}
	if list == nil {
		log.M(cr).F().Error("FAIL to list Service - list is nil")
		return
	}
	for _, obj := range list {
		r.RegisterService(obj.GetObjectMeta())
	}
}

func (c *Controller) discoverySecrets(ctx context.Context, r *model.Registry, cr api.ICustomResource, opts meta.ListOptions) {
	list, err := c.kube.Secret().List(ctx, cr.GetNamespace(), opts)
	if err != nil {
		log.M(cr).F().Error("FAIL to list Secret - err: %v", err)
		return
	}
	if list == nil {
		log.M(cr).F().Error("FAIL to list Secret - list is nil")
		return
	}
	for _, obj := range list {
		r.RegisterSecret(obj.GetObjectMeta())
	}
}

func (c *Controller) discoveryPVCs(ctx context.Context, r *model.Registry, cr api.ICustomResource, opts meta.ListOptions) {
	list, err := c.kube.Storage().List(ctx, cr.GetNamespace(), opts)
	if err != nil {
		log.M(cr).F().Error("FAIL to list PVC - err: %v", err)
		return
	}
	if list == nil {
		log.M(cr).F().Error("FAIL to list PVC - list is nil")
		return
	}
	for _, obj := range list {
		r.RegisterPVC(obj.GetObjectMeta())
	}
}

// Comment out PV
//func (c *Controller) discoveryPVs(ctx context.Context, r *chopModel.Registry, cr api.ICustomResource, opts meta.ListOptions) {
//	list, err := c.kubeClient.CoreV1().PersistentVolumes().List(ctx, opts)
//	if err != nil {
//		log.M(cr).F().Error("FAIL list PV err: %v", err)
//		return
//	}
//	if list == nil {
//		log.M(cr).F().Error("FAIL list PV list is nil")
//		return
//	}
//	for _, obj := range list.Items {
//		r.RegisterPV(obj.ObjectMeta)
//	}
//}

func (c *Controller) discoveryPDBs(ctx context.Context, r *model.Registry, cr api.ICustomResource, opts meta.ListOptions) {
	list, err := c.kube.PDB().List(ctx, cr.GetNamespace(), opts)
	if err != nil {
		log.M(cr).F().Error("FAIL to list PDB - err: %v", err)
		return
	}
	if list == nil {
		log.M(cr).F().Error("FAIL to list PDB - list is nil")
		return
	}
	for _, obj := range list {
		r.RegisterPDB(obj.GetObjectMeta())
	}
}
