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

package kube

import (
	"context"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube "k8s.io/client-go/kubernetes"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	chiLabeler "github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	commonLabeler "github.com/altinity/clickhouse-operator/pkg/model/common/tags/labeler"
)

type PVC struct {
	kubeClient kube.Interface
}

func NewPVC(kubeClient kube.Interface) *PVC {
	return &PVC{
		kubeClient: kubeClient,
	}
}

func (c *PVC) Create(ctx context.Context, pvc *core.PersistentVolumeClaim) (*core.PersistentVolumeClaim, error) {
	return c.kubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Create(ctx, pvc, controller.NewCreateOptions())
}

func (c *PVC) Get(ctx context.Context, namespace, name string) (*core.PersistentVolumeClaim, error) {
	return c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, controller.NewGetOptions())
}

func (c *PVC) Update(ctx context.Context, pvc *core.PersistentVolumeClaim) (*core.PersistentVolumeClaim, error) {
	return c.kubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, controller.NewUpdateOptions())
}

func (c *PVC) Delete(ctx context.Context, namespace, name string) error {
	return c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, name, controller.NewDeleteOptions())
}

func (c *PVC) List(ctx context.Context, namespace string, opts meta.ListOptions) ([]core.PersistentVolumeClaim, error) {
	list, err := c.kubeClient.CoreV1().PersistentVolumeClaims(namespace).List(ctx, opts)
	if err != nil {
		return nil, err
	}
	if list == nil {
		return nil, err
	}
	return list.Items, nil
}

func (c *PVC) ListForHost(ctx context.Context, host *api.Host) (*core.PersistentVolumeClaimList, error) {
	return c.kubeClient.
		CoreV1().
		PersistentVolumeClaims(host.Runtime.Address.Namespace).
		List(
			ctx,
			controller.NewListOptions(labeler(host.GetCR()).Selector(interfaces.SelectorHostScope, host)),
		)
}

func labeler(cr api.ICustomResource) interfaces.ILabeler {
	return chiLabeler.NewLabelerClickHouse(cr, commonLabeler.Config{
		AppendScope: chop.Config().Label.Runtime.AppendScope,
		Include:     chop.Config().Label.Include,
		Exclude:     chop.Config().Label.Exclude,
	})
}
