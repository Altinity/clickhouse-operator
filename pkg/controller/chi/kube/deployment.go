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
	apps "k8s.io/api/apps/v1"
	kube "k8s.io/client-go/kubernetes"

	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

type DeploymentClickHouse struct {
	kubeClient kube.Interface
	namer      interfaces.INameManager
}

func NewDeploymentClickHouse(kubeClient kube.Interface, namer interfaces.INameManager) *DeploymentClickHouse {
	return &DeploymentClickHouse{
		kubeClient: kubeClient,
		namer:      namer,
	}
}

func (c *DeploymentClickHouse) Get(namespace, name string) (*apps.Deployment, error) {
	return c.kubeClient.AppsV1().Deployments(namespace).Get(controller.NewContext(), name, controller.NewGetOptions())
}

func (c *DeploymentClickHouse) Update(deployment *apps.Deployment) (*apps.Deployment, error) {
	return c.kubeClient.AppsV1().Deployments(deployment.Namespace).Update(controller.NewContext(), deployment, controller.NewUpdateOptions())
}
