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

	policy "k8s.io/api/policy/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube "k8s.io/client-go/kubernetes"

	"github.com/altinity/clickhouse-operator/pkg/controller"
)

type PDB struct {
	kubeClient kube.Interface
}

func NewPDB(kubeClient kube.Interface) *PDB {
	return &PDB{
		kubeClient: kubeClient,
	}
}

func (c *PDB) Create(ctx context.Context, pdb *policy.PodDisruptionBudget) (*policy.PodDisruptionBudget, error) {
	return c.kubeClient.PolicyV1().PodDisruptionBudgets(pdb.Namespace).Create(ctx, pdb, controller.NewCreateOptions())
}

func (c *PDB) Get(ctx context.Context, namespace, name string) (*policy.PodDisruptionBudget, error) {
	return c.kubeClient.PolicyV1().PodDisruptionBudgets(namespace).Get(ctx, name, controller.NewGetOptions())
}

func (c *PDB) Update(ctx context.Context, pdb *policy.PodDisruptionBudget) (*policy.PodDisruptionBudget, error) {
	return c.kubeClient.PolicyV1().PodDisruptionBudgets(pdb.Namespace).Update(ctx, pdb, controller.NewUpdateOptions())
}

func (c *PDB) Delete(ctx context.Context, namespace, name string) error {
	return c.kubeClient.PolicyV1().PodDisruptionBudgets(namespace).Delete(ctx, name, controller.NewDeleteOptions())
}

func (c *PDB) List(ctx context.Context, namespace string, opts meta.ListOptions) ([]policy.PodDisruptionBudget, error) {
	list, err := c.kubeClient.PolicyV1().PodDisruptionBudgets(namespace).List(ctx, opts)
	if err != nil {
		return nil, err
	}
	if list == nil {
		return nil, err
	}
	return list.Items, nil
}
