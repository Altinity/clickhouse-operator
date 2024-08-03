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
	kube "k8s.io/client-go/kubernetes"

	"github.com/altinity/clickhouse-operator/pkg/controller"
	core "k8s.io/api/core/v1"
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
	return c.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Create(ctx, cm, controller.NewCreateOptions())
}

func (c *ConfigMap) Get(ctx context.Context, namespace, name string) (*core.ConfigMap, error) {
	return c.kubeClient.CoreV1().ConfigMaps(namespace).Get(controller.NewContext(), name, controller.NewGetOptions())
}

func (c *ConfigMap) Update(ctx context.Context, cm *core.ConfigMap) (*core.ConfigMap, error) {
	return c.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, controller.NewUpdateOptions())
}

func (c *ConfigMap) Delete(ctx context.Context, namespace, name string) error {
	return c.kubeClient.CoreV1().ConfigMaps(namespace).Delete(controller.NewContext(), name, controller.NewDeleteOptions())
}
