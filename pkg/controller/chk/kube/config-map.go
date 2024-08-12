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
	"k8s.io/apimachinery/pkg/labels"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ConfigMap struct {
	kubeClient client.Client
}

func NewConfigMap(kubeClient client.Client) *ConfigMap {
	return &ConfigMap{
		kubeClient: kubeClient,
	}
}

func (c *ConfigMap) Create(ctx context.Context, cm *core.ConfigMap) (*core.ConfigMap, error) {
	err := c.kubeClient.Create(ctx, cm)
	return cm, err
}

func (c *ConfigMap) Get(ctx context.Context, namespace, name string) (*core.ConfigMap, error) {
	cm := &core.ConfigMap{}
	err := c.kubeClient.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, cm)
	if err == nil {
		return cm, nil
	} else {
		return nil, err
	}
}

func (c *ConfigMap) Update(ctx context.Context, cm *core.ConfigMap) (*core.ConfigMap, error) {
	err := c.kubeClient.Update(ctx, cm)
	return cm, err
}

func (c *ConfigMap) Delete(ctx context.Context, namespace, name string) error {
	cm := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
	}
	return c.kubeClient.Delete(ctx, cm)
}

func (c *ConfigMap) List(ctx context.Context, namespace string, opts meta.ListOptions) ([]core.ConfigMap, error) {
	list := &core.ConfigMapList{}
	selector, err := labels.Parse(opts.LabelSelector)
	if err != nil {
		return nil, err
	}
	err = c.kubeClient.List(ctx, list, &client.ListOptions{
		Namespace:     namespace,
		LabelSelector: selector,
	})
	if err != nil {
		return nil, err
	}
	if list == nil {
		return nil, err
	}
	return list.Items, nil
}
