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

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/poller"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

type Service struct {
	kubeClient kube.Interface
	namer      interfaces.INameManager
}

func NewService(kubeClient kube.Interface, namer interfaces.INameManager) *Service {
	return &Service{
		kubeClient: kubeClient,
		namer:      namer,
	}
}

// Get gets Service. Accepted types:
//  1. *core.Service
//  2. *chop.Host
func (c *Service) Get(ctx context.Context, params ...any) (*core.Service, error) {
	var name, namespace string
	switch len(params) {
	case 2:
		// Expecting namespace name
		namespace = params[0].(string)
		name = params[1].(string)
	case 1:
		// Expecting obj
		obj := params[0]
		switch typedObj := obj.(type) {
		case *core.Service:
			name = typedObj.Name
			namespace = typedObj.Namespace
		case *api.Host:
			name = c.namer.Name(interfaces.NameStatefulSetService, typedObj)
			namespace = typedObj.Runtime.Address.Namespace
		}
	}
	return c.kubeClient.CoreV1().Services(namespace).Get(ctx, name, controller.NewGetOptions())
}

func (c *Service) Create(ctx context.Context, svc *core.Service) (*core.Service, error) {
	return c.kubeClient.CoreV1().Services(svc.Namespace).Create(ctx, svc, controller.NewCreateOptions())
}

func (c *Service) Update(ctx context.Context, svc *core.Service) (*core.Service, error) {
	return c.kubeClient.CoreV1().Services(svc.Namespace).Update(ctx, svc, controller.NewUpdateOptions())
}

func (c *Service) Delete(ctx context.Context, namespace, name string) error {
	c.kubeClient.CoreV1().Services(namespace).Delete(ctx, name, controller.NewDeleteOptions())
	return poller.New(ctx, fmt.Sprintf("%s/%s", namespace, name)).
		WithOptions(poller.NewOptions().FromConfig(chop.Config())).
		WithMain(&poller.Functions{
			IsDone: func(_ctx context.Context, _ any) bool {
				_, err := c.Get(ctx, namespace, name)
				return errors.IsNotFound(err)
			},
		}).Poll()
}

func (c *Service) List(ctx context.Context, namespace string, opts meta.ListOptions) ([]core.Service, error) {
	list, err := c.kubeClient.CoreV1().Services(namespace).List(ctx, opts)
	if err != nil {
		return nil, err
	}
	if list == nil {
		return nil, err
	}
	return list.Items, nil
}
