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
)

type Deployment struct {
	kubeClient kube.Interface
}

func NewDeployment(kubeClient kube.Interface) *Deployment {
	return &Deployment{
		kubeClient: kubeClient,
	}
}

func (c *Deployment) Get(namespace, name string) (*apps.Deployment, error) {
	ctx := k8sCtx(controller.NewContext())
	return c.kubeClient.AppsV1().Deployments(namespace).Get(ctx, name, controller.NewGetOptions())
}

func (c *Deployment) Update(deployment *apps.Deployment) (*apps.Deployment, error) {
	ctx := k8sCtx(controller.NewContext())
	return c.kubeClient.AppsV1().Deployments(deployment.Namespace).Update(ctx, deployment, controller.NewUpdateOptions())
}
