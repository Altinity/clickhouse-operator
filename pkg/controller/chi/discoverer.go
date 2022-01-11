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
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopmodel "github.com/altinity/clickhouse-operator/pkg/model"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func (c *Controller) discovery(ctx context.Context, chi *chop.ClickHouseInstallation) *chopmodel.Registry {
	if util.IsContextDone(ctx) {
		log.V(2).Info("ctx is done")
		return nil
	}

	opts := newListOptions(chopmodel.NewLabeler(chi).GetSelectorCHIScope())
	r := chopmodel.NewRegistry()
	c.discoveryStatefulSet(ctx, r, chi, opts)
	c.discoveryConfigMap(ctx, r, chi, opts)
	c.discoveryService(ctx, r, chi, opts)
	c.discoveryPVC(ctx, r, chi, opts)
	c.discoveryPV(ctx, r, chi, opts)
	return r
}

func (c *Controller) discoveryStatefulSet(ctx context.Context, r *chopmodel.Registry, chi *chop.ClickHouseInstallation, opts v1.ListOptions) {
	list, err := c.kubeClient.AppsV1().StatefulSets(chi.Namespace).List(ctx, opts)
	if err != nil {
		log.M(chi).F().Error("FAIL list StatefulSet err:%v", err)
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

func (c *Controller) discoveryConfigMap(ctx context.Context, r *chopmodel.Registry, chi *chop.ClickHouseInstallation, opts v1.ListOptions) {
	list, err := c.kubeClient.CoreV1().ConfigMaps(chi.Namespace).List(ctx, opts)
	if err != nil {
		log.M(chi).F().Error("FAIL list ConfigMap err:%v", err)
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

func (c *Controller) discoveryService(ctx context.Context, r *chopmodel.Registry, chi *chop.ClickHouseInstallation, opts v1.ListOptions) {
	list, err := c.kubeClient.CoreV1().Services(chi.Namespace).List(ctx, opts)
	if err != nil {
		log.M(chi).F().Error("FAIL list Service err:%v", err)
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

func (c *Controller) discoveryPVC(ctx context.Context, r *chopmodel.Registry, chi *chop.ClickHouseInstallation, opts v1.ListOptions) {
	list, err := c.kubeClient.CoreV1().PersistentVolumeClaims(chi.Namespace).List(ctx, opts)
	if err != nil {
		log.M(chi).F().Error("FAIL list PVC err:%v", err)
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

func (c *Controller) discoveryPV(ctx context.Context, r *chopmodel.Registry, chi *chop.ClickHouseInstallation, opts v1.ListOptions) {
	list, err := c.kubeClient.CoreV1().PersistentVolumes().List(ctx, opts)
	if err != nil {
		log.M(chi).F().Error("FAIL list PV err:%v", err)
		return
	}
	if list == nil {
		log.M(chi).F().Error("FAIL list PV list is nil")
		return
	}
	for _, obj := range list.Items {
		r.RegisterPV(obj.ObjectMeta)
	}
}
