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
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

type Pod struct {
	kubeClient client.Client
	namer      interfaces.INameManager
}

func NewPod(kubeClient client.Client, namer interfaces.INameManager) *Pod {
	return &Pod{
		kubeClient: kubeClient,
		namer:      namer,
	}
}

// Get gets pod. Accepted types:
//  1. *apps.StatefulSet
//  2. *chop.Host
func (c *Pod) Get(ctx context.Context, params ...any) (*core.Pod, error) {
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
		case *apps.StatefulSet:
			name = c.namer.Name(interfaces.NamePod, obj)
			namespace = typedObj.Namespace
		case *api.Host:
			name = c.namer.Name(interfaces.NamePod, obj)
			namespace = typedObj.Runtime.Address.Namespace
		default:
			panic(any("unknown param"))
		}
	default:
		panic(any("incorrect number or params"))
	}
	pod := &core.Pod{}
	err := c.kubeClient.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, pod)
	return pod, err
}

// GetAll gets all pods for provided entity
func (c *Pod) GetAll(ctx context.Context, obj any) []*core.Pod {
	switch typed := obj.(type) {
	case api.ICustomResource:
		return c.getPodsOfCR(ctx, typed)
	case api.ICluster:
		return c.getPodsOfCluster(ctx, typed)
	case api.IShard:
		return c.getPodsOfShard(ctx, typed)
	case *api.Host:
		if pod, err := c.Get(ctx, typed); err == nil {
			return []*core.Pod{
				pod,
			}
		}
	default:
		panic(any("unknown type"))
	}
	return nil
}

func (c *Pod) Update(ctx context.Context, pod *core.Pod) (*core.Pod, error) {
	err := c.kubeClient.Update(ctx, pod)
	return pod, err
}

// getPodsOfCluster gets all pods in a cluster
func (c *Pod) getPodsOfCluster(ctx context.Context, cluster api.ICluster) (pods []*core.Pod) {
	cluster.WalkHosts(func(host *api.Host) error {
		if pod, err := c.Get(ctx, host); err == nil {
			pods = append(pods, pod)
		}
		return nil
	})
	return pods
}

// getPodsOfShard gets all pods in a shard
func (c *Pod) getPodsOfShard(ctx context.Context, shard api.IShard) (pods []*core.Pod) {
	shard.WalkHosts(func(host *api.Host) error {
		if pod, err := c.Get(ctx, host); err == nil {
			pods = append(pods, pod)
		}
		return nil
	})
	return pods
}

// getPodsOfCR gets all pods in a CHI
func (c *Pod) getPodsOfCR(ctx context.Context, cr api.ICustomResource) (pods []*core.Pod) {
	cr.WalkHosts(func(host *api.Host) error {
		if pod, err := c.Get(ctx, host); err == nil {
			pods = append(pods, pod)
		}
		return nil
	})
	return pods
}

func (c *Pod) Delete(ctx context.Context, namespace, name string) error {
	pod := &core.Pod{
		ObjectMeta: meta.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
	}
	return c.kubeClient.Delete(ctx, pod)
}
