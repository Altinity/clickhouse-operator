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

	apps "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ReplicaSet struct {
	kubeClient client.Client
}

func NewReplicaSet(kubeClient client.Client) *ReplicaSet {
	return &ReplicaSet{
		kubeClient: kubeClient,
	}
}

func (c *ReplicaSet) Get(ctx context.Context, namespace, name string) (*apps.ReplicaSet, error) {
	rs := &apps.ReplicaSet{}
	err := c.kubeClient.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, rs)
	if err == nil {
		return rs, nil
	} else {
		return nil, err
	}
}

func (c *ReplicaSet) Update(ctx context.Context, replicaSet *apps.ReplicaSet) (*apps.ReplicaSet, error) {
	err := c.kubeClient.Update(ctx, replicaSet)
	return replicaSet, err
}
