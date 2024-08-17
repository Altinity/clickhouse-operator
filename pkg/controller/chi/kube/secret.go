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
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube "k8s.io/client-go/kubernetes"

	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

type Secret struct {
	kubeClient kube.Interface
	namer      interfaces.INameManager
}

func NewSecret(kubeClient kube.Interface, namer interfaces.INameManager) *Secret {
	return &Secret{
		kubeClient: kubeClient,
		namer:      namer,
	}
}

// Get gets Secret. Accepted types:
//  1. *core.Service
//  2. *chop.Host
func (c *Secret) Get(ctx context.Context, obj any) (*core.Secret, error) {
	var name, namespace string
	switch typedObj := obj.(type) {
	case *core.Secret:
		name = typedObj.Name
		namespace = typedObj.Namespace
	case *api.Host:
		name = c.namer.Name(interfaces.NameStatefulSetService, typedObj)
		namespace = typedObj.Runtime.Address.Namespace
	}
	return c.kubeClient.CoreV1().Secrets(namespace).Get(ctx, name, controller.NewGetOptions())
}

func (c *Secret) Create(ctx context.Context, svc *core.Secret) (*core.Secret, error) {
	return c.kubeClient.CoreV1().Secrets(svc.Namespace).Create(ctx, svc, controller.NewCreateOptions())
}

func (c *Secret) Update(ctx context.Context, svc *core.Secret) (*core.Secret, error) {
	return c.kubeClient.CoreV1().Secrets(svc.Namespace).Update(ctx, svc, controller.NewUpdateOptions())
}

func (c *Secret) Delete(ctx context.Context, namespace, name string) error {
	return c.kubeClient.CoreV1().Secrets(namespace).Delete(ctx, name, controller.NewDeleteOptions())
}

func (c *Secret) List(ctx context.Context, namespace string, opts meta.ListOptions) ([]core.Secret, error) {
	list, err := c.kubeClient.CoreV1().Secrets(namespace).List(ctx, opts)
	if err != nil {
		return nil, err
	}
	if list == nil {
		return nil, err
	}
	return list.Items, nil
}
