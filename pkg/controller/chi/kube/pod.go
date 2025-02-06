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
	kube "k8s.io/client-go/kubernetes"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
)

type Pod struct {
	kubeClient kube.Interface
	namer      interfaces.INameManager
}

func NewPod(kubeClient kube.Interface, namer interfaces.INameManager) *Pod {
	return &Pod{
		kubeClient: kubeClient,
		namer:      namer,
	}
}

// getPod gets pod. Accepted types:
//  1. *apps.StatefulSet
//  2. *chop.Host
func (c *Pod) Get(params ...any) (*core.Pod, error) {
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
	return c.kubeClient.CoreV1().Pods(namespace).Get(controller.NewContext(), name, controller.NewGetOptions())
}

func (c *Pod) GetRestartCounters(params ...any) (map[string]int, error) {
	pod, err := c.Get(params...)
	if err != nil {
		return nil, err
	}
	return k8s.PodRestartCountersGet(pod), nil
}

// GetAll gets all pods for provided entity
func (c *Pod) GetAll(obj any) []*core.Pod {
	switch typed := obj.(type) {
	case api.ICustomResource:
		return c.getPodsOfCHI(typed)
	case api.ICluster:
		return c.getPodsOfCluster(typed)
	case api.IShard:
		return c.getPodsOfShard(typed)
	case *api.Host:
		if pod, err := c.Get(typed); err == nil {
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
	return c.kubeClient.CoreV1().Pods(pod.GetNamespace()).Update(ctx, pod, controller.NewUpdateOptions())
}

// getPodsOfCluster gets all pods in a cluster
func (c *Pod) getPodsOfCluster(cluster api.ICluster) (pods []*core.Pod) {
	cluster.WalkHosts(func(host *api.Host) error {
		if pod, err := c.Get(host); err == nil {
			pods = append(pods, pod)
		}
		return nil
	})
	return pods
}

// getPodsOfShard gets all pods in a shard
func (c *Pod) getPodsOfShard(shard api.IShard) (pods []*core.Pod) {
	shard.WalkHosts(func(host *api.Host) error {
		if pod, err := c.Get(host); err == nil {
			pods = append(pods, pod)
		}
		return nil
	})
	return pods
}

// getPodsOfCHI gets all pods in a CHI
func (c *Pod) getPodsOfCHI(cr api.ICustomResource) (pods []*core.Pod) {
	cr.WalkHosts(func(host *api.Host) error {
		if pod, err := c.Get(host); err == nil {
			pods = append(pods, pod)
		}
		return nil
	})
	return pods
}

func (c *Pod) Delete(ctx context.Context, namespace, name string) error {
	return c.kubeClient.CoreV1().Pods(namespace).Delete(ctx, name, controller.NewDeleteOptions())
}
