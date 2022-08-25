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

package chi

import (
	"fmt"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8slabels "k8s.io/apimachinery/pkg/labels"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopmodel "github.com/altinity/clickhouse-operator/pkg/model"
)

// getConfigMap gets ConfigMap either by namespaced name or by labels
// TODO review byNameOnly params
func (c *Controller) getConfigMap(objMeta *meta.ObjectMeta, byNameOnly bool) (*core.ConfigMap, error) {
	get := c.configMapLister.ConfigMaps(objMeta.Namespace).Get
	list := c.configMapLister.ConfigMaps(objMeta.Namespace).List
	var objects []*core.ConfigMap

	// Check whether object with such name already exists
	obj, err := get(objMeta.Name)

	if (obj != nil) && (err == nil) {
		// Object found by name
		return obj, nil
	}

	if !apierrors.IsNotFound(err) {
		// Error, which is not related to "Object not found"
		return nil, err
	}

	// Object not found by name

	if byNameOnly {
		return nil, err
	}

	// Try to find by labels

	var selector k8slabels.Selector
	if selector, err = chopmodel.MakeSelectorFromObjectMeta(objMeta); err != nil {
		return nil, err
	}

	if objects, err = list(selector); err != nil {
		return nil, err
	}

	if len(objects) == 0 {
		return nil, apierrors.NewNotFound(apps.Resource("ConfigMap"), objMeta.Name)
	}

	if len(objects) == 1 {
		// Exactly one object found by labels
		return objects[0], nil
	}

	// Too much objects found by labels
	return nil, fmt.Errorf("too much objects found %d expecting 1", len(objects))
}

// getService gets Service. Accepted types:
//   1. *core.Service
//   2. *chop.ChiHost
func (c *Controller) getService(obj interface{}) (*core.Service, error) {
	var name, namespace string
	switch typedObj := obj.(type) {
	case *core.Service:
		name = typedObj.Name
		namespace = typedObj.Namespace
	case *chiv1.ChiHost:
		name = chopmodel.CreateStatefulSetServiceName(typedObj)
		namespace = typedObj.Address.Namespace
	}
	return c.serviceLister.Services(namespace).Get(name)
	//return c.kubeClient.CoreV1().Services(namespace).Get(newContext(), name, newGetOptions())
}

// getStatefulSet gets StatefulSet. Accepted types:
//   1. *meta.ObjectMeta
//   2. *chop.ChiHost
func (c *Controller) getStatefulSet(obj interface{}, byName ...bool) (*apps.StatefulSet, error) {
	switch typedObj := obj.(type) {
	case *meta.ObjectMeta:
		var b bool
		if len(byName) > 0 {
			b = byName[0]
		}
		return c.getStatefulSetByMeta(typedObj, b)
	case *chiv1.ChiHost:
		return c.getStatefulSetByHost(typedObj)
	}
	return nil, fmt.Errorf("unknown type")
}

// getStatefulSet gets StatefulSet either by namespaced name or by labels
// TODO review byNameOnly params
func (c *Controller) getStatefulSetByMeta(meta *meta.ObjectMeta, byNameOnly bool) (*apps.StatefulSet, error) {
	get := c.statefulSetLister.StatefulSets(meta.Namespace).Get
	list := c.statefulSetLister.StatefulSets(meta.Namespace).List
	var objects []*apps.StatefulSet

	// Check whether object with such name already exists
	obj, err := get(meta.Name)

	if (obj != nil) && (err == nil) {
		// Object found by name
		return obj, nil
	}

	if !apierrors.IsNotFound(err) {
		// Error, which is not related to "Object not found"
		return nil, err
	}

	// Object not found by name. Try to find by labels

	if byNameOnly {
		return nil, fmt.Errorf("object not found by name %s/%s and no label search allowed ", meta.Namespace, meta.Name)
	}

	var selector k8slabels.Selector
	if selector, err = chopmodel.MakeSelectorFromObjectMeta(meta); err != nil {
		return nil, err
	}

	if objects, err = list(selector); err != nil {
		return nil, err
	}

	if len(objects) == 0 {
		return nil, apierrors.NewNotFound(apps.Resource("StatefulSet"), meta.Name)
	}

	if len(objects) == 1 {
		// Exactly one object found by labels
		return objects[0], nil
	}

	// Too much objects found by labels
	return nil, fmt.Errorf("too much objects found %d expecting 1", len(objects))
}

// getStatefulSetByHost finds StatefulSet of a specified host
func (c *Controller) getStatefulSetByHost(host *chiv1.ChiHost) (*apps.StatefulSet, error) {
	// Namespaced name
	name := chopmodel.CreateStatefulSetName(host)
	namespace := host.Address.Namespace

	return c.kubeClient.AppsV1().StatefulSets(namespace).Get(newContext(), name, newGetOptions())
}

// getPod gets pod. Accepted types:
//   1. *apps.StatefulSet
//   2. *chop.ChiHost
func (c *Controller) getPod(obj interface{}) (*core.Pod, error) {
	var name, namespace string
	switch typedObj := obj.(type) {
	case *apps.StatefulSet:
		name = chopmodel.CreatePodName(obj)
		namespace = typedObj.Namespace
	case *chiv1.ChiHost:
		name = chopmodel.CreatePodName(obj)
		namespace = typedObj.Address.Namespace
	}
	return c.kubeClient.CoreV1().Pods(namespace).Get(newContext(), name, newGetOptions())
}

// getPods gets all pods for provided entity
func (c *Controller) getPods(obj interface{}) []*core.Pod {
	switch typed := obj.(type) {
	case *chiv1.ClickHouseInstallation:
		return c.getPodsOfCHI(typed)
	case *chiv1.ChiCluster:
		return c.getPodsOfCluster(typed)
	case *chiv1.ChiShard:
		return c.getPodsOfShard(typed)
	case
		*chiv1.ChiHost,
		*apps.StatefulSet:
		if pod, err := c.getPod(typed); err == nil {
			return []*core.Pod{
				pod,
			}
		}
	}
	return nil
}

// getPodsOfCluster gets all pods in a cluster
func (c *Controller) getPodsOfCluster(cluster *chiv1.ChiCluster) (pods []*core.Pod) {
	cluster.WalkHosts(func(host *chiv1.ChiHost) error {
		if pod, err := c.getPod(host); err == nil {
			pods = append(pods, pod)
		}
		return nil
	})
	return pods
}

// getPodsOfShard gets all pods in a shard
func (c *Controller) getPodsOfShard(shard *chiv1.ChiShard) (pods []*core.Pod) {
	shard.WalkHosts(func(host *chiv1.ChiHost) error {
		if pod, err := c.getPod(host); err == nil {
			pods = append(pods, pod)
		}
		return nil
	})
	return pods
}

// getPodsOfCHI gets all pods in a CHI
func (c *Controller) getPodsOfCHI(chi *chiv1.ClickHouseInstallation) (pods []*core.Pod) {
	chi.WalkHosts(func(host *chiv1.ChiHost) error {
		if pod, err := c.getPod(host); err == nil {
			pods = append(pods, pod)
		}
		return nil
	})
	return pods
}

// getPodsIPs gets all pod IPs
func (c *Controller) getPodsIPs(obj interface{}) (ips []string) {
	for _, pod := range c.getPods(obj) {
		if ip := pod.Status.PodIP; ip != "" {
			ips = append(ips, ip)
		}
	}
	return ips
}

// GetCHIByObjectMeta gets CHI by namespaced name
func (c *Controller) GetCHIByObjectMeta(objectMeta *meta.ObjectMeta, isCHI bool) (*chiv1.ClickHouseInstallation, error) {
	var chiName string
	var err error
	if isCHI {
		chiName = objectMeta.Name
	} else {
		chiName, err = chopmodel.GetCHINameFromObjectMeta(objectMeta)
		if err != nil {
			return nil, fmt.Errorf("unable to find CHI by name: '%s'. More info: %v", objectMeta.Name, err)
		}
	}

	return c.chopClient.ClickhouseV1().ClickHouseInstallations(objectMeta.Namespace).Get(newContext(), chiName, newGetOptions())
}
