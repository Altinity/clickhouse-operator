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
	kublabels "k8s.io/apimachinery/pkg/labels"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
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

	var selector kublabels.Selector
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

// getService gets Service either by namespaced name or by labels
// TODO review byNameOnly params
func (c *Controller) getService(objMeta *meta.ObjectMeta, byNameOnly bool) (*core.Service, error) {
	get := c.serviceLister.Services(objMeta.Namespace).Get
	list := c.serviceLister.Services(objMeta.Namespace).List
	var objects []*core.Service

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

	// Object not found by name. Try to find by labels

	if byNameOnly {
		return nil, fmt.Errorf("object not found by name %s/%s and no label search allowed ", objMeta.Namespace, objMeta.Name)
	}

	var selector kublabels.Selector
	if selector, err = chopmodel.MakeSelectorFromObjectMeta(objMeta); err != nil {
		return nil, err
	}

	if objects, err = list(selector); err != nil {
		return nil, err
	}

	if len(objects) == 0 {
		return nil, apierrors.NewNotFound(apps.Resource("Service"), objMeta.Name)
	}

	if len(objects) == 1 {
		// Exactly one object found by labels
		return objects[0], nil
	}

	// Too much objects found by labels
	return nil, fmt.Errorf("too much objects found %d expecting 1", len(objects))
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
	case *chop.ChiHost:
		return c.getStatefulSetByHost(typedObj)
	}
	return nil, fmt.Errorf("unknown type")
}

// getStatefulSet gets StatefulSet either by namespaced name or by labels
// TODO review byNameOnly params
func (c *Controller) getStatefulSetByMeta(objMeta *meta.ObjectMeta, byNameOnly bool) (*apps.StatefulSet, error) {
	get := c.statefulSetLister.StatefulSets(objMeta.Namespace).Get
	list := c.statefulSetLister.StatefulSets(objMeta.Namespace).List
	var objects []*apps.StatefulSet

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

	// Object not found by name. Try to find by labels

	if byNameOnly {
		return nil, fmt.Errorf("object not found by name %s/%s and no label search allowed ", objMeta.Namespace, objMeta.Name)
	}

	var selector kublabels.Selector
	if selector, err = chopmodel.MakeSelectorFromObjectMeta(objMeta); err != nil {
		return nil, err
	}

	if objects, err = list(selector); err != nil {
		return nil, err
	}

	if len(objects) == 0 {
		return nil, apierrors.NewNotFound(apps.Resource("StatefulSet"), objMeta.Name)
	}

	if len(objects) == 1 {
		// Exactly one object found by labels
		return objects[0], nil
	}

	// Too much objects found by labels
	return nil, fmt.Errorf("too much objects found %d expecting 1", len(objects))
}

// getStatefulSetByHost finds StatefulSet of a specified host
func (c *Controller) getStatefulSetByHost(host *chop.ChiHost) (*apps.StatefulSet, error) {
	// Namespaced name
	name := chopmodel.CreateStatefulSetName(host)
	namespace := host.Address.Namespace

	return c.kubeClient.AppsV1().StatefulSets(namespace).Get(newContext(), name, newGetOptions())
}

// getPod gets pod for host or StatefulSet. Accepted types:
//   1. *apps.StatefulSet
//   2. *chop.ChiHost
func (c *Controller) getPod(obj interface{}) (*core.Pod, error) {
	var name, namespace string
	switch typedObj := obj.(type) {
	case *chop.ChiHost:
		name = chopmodel.CreatePodName(obj)
		namespace = typedObj.Address.Namespace
	case *apps.StatefulSet:
		name = chopmodel.CreatePodName(obj)
		namespace = typedObj.Namespace
	}
	return c.kubeClient.CoreV1().Pods(namespace).Get(newContext(), name, newGetOptions())
}

// GetCHIByObjectMeta gets CHI by namespaced name
func (c *Controller) GetCHIByObjectMeta(objectMeta *meta.ObjectMeta) (*chiv1.ClickHouseInstallation, error) {
	chiName, err := chopmodel.GetCHINameFromObjectMeta(objectMeta)
	if err != nil {
		return nil, fmt.Errorf("unable to find CHI by name: '%s'. More info: %v", objectMeta.Name, err)
	}

	return c.chopClient.ClickhouseV1().ClickHouseInstallations(objectMeta.Namespace).Get(newContext(), chiName, newGetOptions())
}
