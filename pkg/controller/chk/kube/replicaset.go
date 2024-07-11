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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/altinity/clickhouse-operator/pkg/controller"
)

type ReplicaSetKeeper struct {
	kubeClient client.Client
}

func NewReplicaSetKeeper(kubeClient client.Client) *ReplicaSetKeeper {
	return &ReplicaSetKeeper{
		kubeClient: kubeClient,
	}
}

func (c *ReplicaSetKeeper) Get(namespace, name string) (*apps.ReplicaSet, error) {
	rs := &apps.ReplicaSet{}
	err := c.kubeClient.Get(controller.NewContext(), types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, rs)
	return rs, err
}

func (c *ReplicaSetKeeper) Update(replicaSet *apps.ReplicaSet) (*apps.ReplicaSet, error) {
	err := c.kubeClient.Update(controller.NewContext(), replicaSet)
	return replicaSet, err
}