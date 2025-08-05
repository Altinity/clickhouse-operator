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
	"fmt"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube "k8s.io/client-go/kubernetes"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/poller"
)

type ConfigMap struct {
	kubeClient kube.Interface
}

func NewConfigMap(kubeClient kube.Interface) *ConfigMap {
	return &ConfigMap{
		kubeClient: kubeClient,
	}
}

func (c *ConfigMap) Create(ctx context.Context, cm *core.ConfigMap) (*core.ConfigMap, error) {
	ctx = k8sCtx(ctx)
	return c.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Create(ctx, cm, controller.NewCreateOptions())
}

func (c *ConfigMap) Get(ctx context.Context, namespace, name string) (*core.ConfigMap, error) {
	ctx = k8sCtx(ctx)
	return c.kubeClient.CoreV1().ConfigMaps(namespace).Get(ctx, name, controller.NewGetOptions())
}

func (c *ConfigMap) Update(ctx context.Context, cm *core.ConfigMap) (*core.ConfigMap, error) {
	ctx = k8sCtx(ctx)
	return c.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, controller.NewUpdateOptions())
}

func (c *ConfigMap) Remove(ctx context.Context, namespace, name string) error {
	ctx = k8sCtx(ctx)
	return c.kubeClient.CoreV1().ConfigMaps(namespace).Delete(ctx, name, controller.NewDeleteOptions())
}

func (c *ConfigMap) Delete(ctx context.Context, namespace, name string) error {
	item := "ConfigMap"
	return poller.New(ctx, fmt.Sprintf("delete %s: %s/%s", item, namespace, name)).
		WithOptions(poller.NewOptions().FromConfig(chop.Config())).
		WithFunctions(&poller.Functions{
			IsDone: func(_ctx context.Context, _ any) bool {
				if err := c.Remove(ctx, namespace, name); err != nil {
					if !errors.IsNotFound(err) {
						log.V(1).Warning("Error deleting %s: %s/%s err: %v ", item, namespace, name, err)
					}
				}

				_, err := c.Get(ctx, namespace, name)
				return errors.IsNotFound(err)
			},
		}).Poll()
}

func (c *ConfigMap) List(ctx context.Context, namespace string, opts meta.ListOptions) ([]core.ConfigMap, error) {
	ctx = k8sCtx(ctx)
	list, err := c.kubeClient.CoreV1().ConfigMaps(namespace).List(ctx, opts)
	if err != nil {
		return nil, err
	}
	if list == nil {
		return nil, err
	}
	return list.Items, nil
}
