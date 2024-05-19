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
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sLabels "k8s.io/apimachinery/pkg/labels"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller"
)

// getConfigMap gets ConfigMap either by namespaced name or by labels
// TODO review byNameOnly params
func (c *Controller) getConfigMap(meta meta.Object, byNameOnly bool) (*core.ConfigMap, error) {
	get := c.configMapLister.ConfigMaps(meta.GetNamespace()).Get
	list := c.configMapLister.ConfigMaps(meta.GetNamespace()).List
	var objects []*core.ConfigMap

	// Check whether object with such name already exists
	obj, err := get(meta.GetName())

	if (obj != nil) && (err == nil) {
		// Object found by name
		return obj, nil
	}

	if !apiErrors.IsNotFound(err) {
		// Error, which is not related to "Object not found"
		return nil, err
	}

	// Object not found by name

	if byNameOnly {
		return nil, err
	}

	// Try to find by labels

	var selector k8sLabels.Selector
	if selector, err = tags.MakeSelectorFromObjectMeta(meta); err != nil {
		return nil, err
	}

	if objects, err = list(selector); err != nil {
		return nil, err
	}

	if len(objects) == 0 {
		return nil, apiErrors.NewNotFound(apps.Resource("ConfigMap"), meta.GetName())
	}

	if len(objects) == 1 {
		// Exactly one object found by labels
		return objects[0], nil
	}

	// Too much objects found by labels
	return nil, fmt.Errorf("too much objects found %d expecting 1", len(objects))
}

// getService gets Service. Accepted types:
//  1. *core.Service
//  2. *chop.Host
func (c *Controller) getService(obj interface{}) (*core.Service, error) {
	var name, namespace string
	switch typedObj := obj.(type) {
	case *core.Service:
		name = typedObj.Name
		namespace = typedObj.Namespace
	case *api.Host:
		name = namer.Name(namer.NameStatefulSetService, typedObj)
		namespace = typedObj.Runtime.Address.Namespace
	}
	return c.serviceLister.Services(namespace).Get(name)
	//return c.kubeClient.CoreV1().Services(namespace).Get(newTask(), name, newGetOptions())
}

// getStatefulSet gets StatefulSet. Accepted types:
//  1. *meta.ObjectMeta
//  2. *chop.Host
func (c *Controller) getStatefulSet(obj interface{}, byName ...bool) (*apps.StatefulSet, error) {
	switch typedObj := obj.(type) {
	case *meta.ObjectMeta:
		var b bool
		if len(byName) > 0 {
			b = byName[0]
		}
		return c.getStatefulSetByMeta(typedObj, b)
	case *api.Host:
		return c.getStatefulSetByHost(typedObj)
	}
	return nil, fmt.Errorf("unknown type")
}

// getStatefulSet gets StatefulSet either by namespaced name or by labels
// TODO review byNameOnly params
func (c *Controller) getStatefulSetByMeta(meta meta.Object, byNameOnly bool) (*apps.StatefulSet, error) {
	get := c.statefulSetLister.StatefulSets(meta.GetNamespace()).Get
	list := c.statefulSetLister.StatefulSets(meta.GetNamespace()).List
	var objects []*apps.StatefulSet

	// Check whether object with such name already exists
	obj, err := get(meta.GetName())

	if (obj != nil) && (err == nil) {
		// Object found by name
		return obj, nil
	}

	if !apiErrors.IsNotFound(err) {
		// Error, which is not related to "Object not found"
		return nil, err
	}

	// Object not found by name. Try to find by labels

	if byNameOnly {
		return nil, fmt.Errorf("object not found by name %s/%s and no label search allowed ", meta.GetNamespace(), meta.GetName())
	}

	var selector k8sLabels.Selector
	if selector, err = tags.MakeSelectorFromObjectMeta(meta); err != nil {
		return nil, err
	}

	if objects, err = list(selector); err != nil {
		return nil, err
	}

	if len(objects) == 0 {
		return nil, apiErrors.NewNotFound(apps.Resource("StatefulSet"), meta.GetName())
	}

	if len(objects) == 1 {
		// Exactly one object found by labels
		return objects[0], nil
	}

	// Too much objects found by labels
	return nil, fmt.Errorf("too much objects found %d expecting 1", len(objects))
}

// getStatefulSetByHost finds StatefulSet of a specified host
func (c *Controller) getStatefulSetByHost(host *api.Host) (*apps.StatefulSet, error) {
	// Namespaced name
	name := namer.Name(namer.NameStatefulSet, host)
	namespace := host.Runtime.Address.Namespace

	return c.kubeClient.AppsV1().StatefulSets(namespace).Get(controller.NewContext(), name, controller.NewGetOptions())
}

// getSecret gets secret
func (c *Controller) getSecret(secret *core.Secret) (*core.Secret, error) {
	return c.kubeClient.CoreV1().Secrets(secret.Namespace).Get(controller.NewContext(), secret.Name, controller.NewGetOptions())
}

// getPod gets pod. Accepted types:
//  1. *apps.StatefulSet
//  2. *chop.Host
func (c *Controller) getPod(obj interface{}) (*core.Pod, error) {
	var name, namespace string
	switch typedObj := obj.(type) {
	case *apps.StatefulSet:
		name = namer.Name(namer.NamePod, obj)
		namespace = typedObj.Namespace
	case *api.Host:
		name = namer.Name(namer.NamePod, obj)
		namespace = typedObj.Runtime.Address.Namespace
	}
	return c.kubeClient.CoreV1().Pods(namespace).Get(controller.NewContext(), name, controller.NewGetOptions())
}

// getPods gets all pods for provided entity
func (c *Controller) getPods(obj interface{}) []*core.Pod {
	switch typed := obj.(type) {
	case *api.ClickHouseInstallation:
		return c.getPodsOfCHI(typed)
	case *api.Cluster:
		return c.getPodsOfCluster(typed)
	case *api.ChiShard:
		return c.getPodsOfShard(typed)
	case
		*api.Host,
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
func (c *Controller) getPodsOfCluster(cluster *api.Cluster) (pods []*core.Pod) {
	cluster.WalkHosts(func(host *api.Host) error {
		if pod, err := c.getPod(host); err == nil {
			pods = append(pods, pod)
		}
		return nil
	})
	return pods
}

// getPodsOfShard gets all pods in a shard
func (c *Controller) getPodsOfShard(shard *api.ChiShard) (pods []*core.Pod) {
	shard.WalkHosts(func(host *api.Host) error {
		if pod, err := c.getPod(host); err == nil {
			pods = append(pods, pod)
		}
		return nil
	})
	return pods
}

// getPodsOfCHI gets all pods in a CHI
func (c *Controller) getPodsOfCHI(chi *api.ClickHouseInstallation) (pods []*core.Pod) {
	chi.WalkHosts(func(host *api.Host) error {
		if pod, err := c.getPod(host); err == nil {
			pods = append(pods, pod)
		}
		return nil
	})
	return pods
}

// getPodsIPs gets all pod IPs
func (c *Controller) getPodsIPs(obj interface{}) (ips []string) {
	log.V(3).M(obj).F().S().Info("looking for pods IPs")
	defer log.V(3).M(obj).F().E().Info("looking for pods IPs")

	for _, pod := range c.getPods(obj) {
		if ip := pod.Status.PodIP; ip == "" {
			log.V(3).M(pod).F().Warning("Pod NO IP address found. Pod: %s/%s", pod.Namespace, pod.Name)
		} else {
			ips = append(ips, ip)
			log.V(3).M(pod).F().Info("Pod IP address found. Pod: %s/%s IP: %s", pod.Namespace, pod.Name, ip)
		}
	}
	return ips
}

// GetCHIByObjectMeta gets CHI by namespaced name
func (c *Controller) GetCHIByObjectMeta(meta meta.Object, isCHI bool) (*api.ClickHouseInstallation, error) {
	var chiName string
	var err error
	if isCHI {
		chiName = meta.GetName()
	} else {
		chiName, err = tags.GetCRNameFromObjectMeta(meta)
		if err != nil {
			return nil, fmt.Errorf("unable to find CHI by name: '%s'. More info: %v", meta.GetName(), err)
		}
	}

	return c.chopClient.ClickhouseV1().ClickHouseInstallations(meta.GetNamespace()).Get(controller.NewContext(), chiName, controller.NewGetOptions())
}
