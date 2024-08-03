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
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	chiLabeler "github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	commonLabeler "github.com/altinity/clickhouse-operator/pkg/model/common/tags/labeler"
)

type PVC struct {
	kubeClient client.Client
}

func NewPVC(kubeClient client.Client) *PVC {
	return &PVC{
		kubeClient: kubeClient,
	}
}

func (c *PVC) Create(ctx context.Context, pvc *core.PersistentVolumeClaim) (*core.PersistentVolumeClaim, error) {
	err := c.kubeClient.Create(ctx, pvc)
	return pvc, err
}

func (c *PVC) Get(ctx context.Context, namespace, name string) (*core.PersistentVolumeClaim, error) {
	pvc := &core.PersistentVolumeClaim{}
	err := c.kubeClient.Get(controller.NewContext(), types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, pvc)
	return pvc, err
}

func (c *PVC) Update(ctx context.Context, pvc *core.PersistentVolumeClaim) (*core.PersistentVolumeClaim, error) {
	err := c.kubeClient.Update(controller.NewContext(), pvc)
	return pvc, err
}

func (c *PVC) Delete(ctx context.Context, namespace, name string) error {
	pvc := &core.PersistentVolumeClaim{
		ObjectMeta: meta.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
	}
	return c.kubeClient.Delete(ctx, pvc)
}

func (c *PVC) ListForHost(ctx context.Context, host *api.Host) (*core.PersistentVolumeClaimList, error) {
	list := &core.PersistentVolumeClaimList{}
	opts := &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(labeler(host.GetCR()).Selector(interfaces.SelectorHostScope, host)),
		Namespace:     host.Runtime.Address.Namespace,
	}
	err := c.kubeClient.List(ctx, list, opts)
	return list, err
}

func labeler(cr api.ICustomResource) interfaces.ILabeler {
	return chiLabeler.NewLabelerClickHouse(cr, commonLabeler.Config{
		AppendScope: chop.Config().Label.Runtime.AppendScope,
		Include:     chop.Config().Label.Include,
		Exclude:     chop.Config().Label.Exclude,
	})
}
