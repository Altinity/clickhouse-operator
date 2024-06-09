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
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

type ServiceKeeper struct {
	kubeClient client.Client
	namer      interfaces.INameManager
}

func NewServiceKeeper(kubeClient client.Client, namer interfaces.INameManager) *ServiceKeeper {
	return &ServiceKeeper{
		kubeClient: kubeClient,
		namer:      namer,
	}
}

// getService gets Service. Accepted types:
//  1. *core.Service
//  2. *chop.Host
func (c *ServiceKeeper) Get(obj any) (*core.Service, error) {
	var name, namespace string
	switch typedObj := obj.(type) {
	case *core.Service:
		name = typedObj.Name
		namespace = typedObj.Namespace
	case *api.Host:
		name = c.namer.Name(interfaces.NameStatefulSetService, typedObj)
		namespace = typedObj.Runtime.Address.Namespace
	}
	service := &core.Service{}
	err := c.kubeClient.Get(controller.NewContext(), types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, service)
	return service, err
}

func (c *ServiceKeeper) Update(svc *core.Service) (*core.Service, error) {
	err := c.kubeClient.Update(controller.NewContext(), svc)
	return svc, err
}
