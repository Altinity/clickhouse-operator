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
	kube "k8s.io/client-go/kubernetes"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/model/common/interfaces"
)

type ServiceClickHouse struct {
	kubeClient kube.Interface
	namer      interfaces.INameManager
}

func NewServiceClickHouse(kubeClient kube.Interface, namer interfaces.INameManager) *ServiceClickHouse {
	return &ServiceClickHouse{
		kubeClient: kubeClient,
		namer:      namer,
	}
}

// getService gets Service. Accepted types:
//  1. *core.Service
//  2. *chop.Host
func (c *ServiceClickHouse) Get(obj any) (*core.Service, error) {
	var name, namespace string
	switch typedObj := obj.(type) {
	case *core.Service:
		name = typedObj.Name
		namespace = typedObj.Namespace
	case *api.Host:
		name = c.namer.Name(interfaces.NameStatefulSetService, typedObj)
		namespace = typedObj.Runtime.Address.Namespace
	}
	return c.kubeClient.CoreV1().Services(namespace).Get(controller.NewContext(), name, controller.NewGetOptions())
}

func (c *ServiceClickHouse) Update(svc *core.Service) (*core.Service, error) {
	return c.kubeClient.CoreV1().Services(svc.Namespace).Update(controller.NewContext(), svc, controller.NewUpdateOptions())
}
