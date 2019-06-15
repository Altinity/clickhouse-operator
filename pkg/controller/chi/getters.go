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
	"k8s.io/apimachinery/pkg/labels"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopmodel "github.com/altinity/clickhouse-operator/pkg/model"
)

// getConfigMap gets ConfigMap either by namespaced name or by labels
func (c *Controller) getConfigMap(obj *meta.ObjectMeta) (*core.ConfigMap, error) {
	// Check whether object with such name already exists in k8s
	res, err := c.configMapLister.ConfigMaps(obj.Namespace).Get(obj.Name)

	if res != nil {
		// Object found by name
		return res, nil
	}

	if apierrors.IsNotFound(err) {
		// Object with such name not found
		// Try to find by labels
		if set, err := chopmodel.GetSelectorReplicaFromObjectMeta(obj); err == nil {
			selector := labels.SelectorFromSet(set)
			objects, err := c.configMapLister.ConfigMaps(obj.Namespace).List(selector)
			if err != nil {
				return nil, err
			}
			if len(objects) == 1 {
				// Object found by labels
				return objects[0], nil
			}
		}
	}

	// Object not found
	return nil, err
}

// getService gets Service either by namespaced name or by labels
func (c *Controller) getService(obj *meta.ObjectMeta) (*core.Service, error) {
	// Check whether object with such name already exists in k8s
	res, err := c.serviceLister.Services(obj.Namespace).Get(obj.Name)

	if res != nil {
		// Object found by name
		return res, nil
	}

	if apierrors.IsNotFound(err) {
		// Object with such name not found
		// Try to find by labels
		if set, err := chopmodel.GetSelectorReplicaFromObjectMeta(obj); err == nil {
			selector := labels.SelectorFromSet(set)

			objects, err := c.serviceLister.Services(obj.Namespace).List(selector)
			if err != nil {
				return nil, err
			}
			if len(objects) == 1 {
				// Object found by labels
				return objects[0], nil
			}
		}
	}

	// Object not found
	return nil, err
}

// getStatefulSet gets StatefulSet either by namespaced name or by labels
func (c *Controller) getStatefulSet(obj *meta.ObjectMeta) (*apps.StatefulSet, error) {
	// Check whether object with such name already exists in k8s
	res, err := c.statefulSetLister.StatefulSets(obj.Namespace).Get(obj.Name)

	if res != nil {
		// Object found by name
		return res, nil
	}

	if apierrors.IsNotFound(err) {
		// Object with such name not found
		// Try to find by labels
		if set, err := chopmodel.GetSelectorReplicaFromObjectMeta(obj); err != nil {
			return nil, err
		} else {
			selector := labels.SelectorFromSet(set)
			if objects, err := c.statefulSetLister.StatefulSets(obj.Namespace).List(selector); err != nil {
				return nil, err
			} else if len(objects) == 1 {
				// Object found by labels
				return objects[0], nil
			} else if len(objects) > 1 {
				// Object found by labels
				return nil, fmt.Errorf("ERROR too much objects returned by selector")
			} else {
				// Zero? Fall through and return IsNotFound() error
			}
		}
	}

	// Object not found
	return nil, err
}

func (c *Controller) createChiFromObjectMeta(objectMeta *meta.ObjectMeta) (*chiv1.ClickHouseInstallation, error) {
	chiName, err := chopmodel.GetChiNameFromObjectMeta(objectMeta)
	if err != nil {
		return nil, fmt.Errorf("ObjectMeta %s does not generated by CHI %v", objectMeta.Name, err)
	}

	chi, err := c.chiLister.ClickHouseInstallations(objectMeta.Namespace).Get(chiName)
	if err != nil {
		return nil, err
	}

	chi, err = c.normalizer.DoChi(chi)
	if err != nil {
		return nil, err
	}

	return chi, nil
}

func (c *Controller) createClusterFromObjectMeta(objectMeta *meta.ObjectMeta) (*chiv1.ChiCluster, error) {
	clusterName, err := chopmodel.GetClusterNameFromObjectMeta(objectMeta)
	if err != nil {
		return nil, fmt.Errorf("ObjectMeta %s does not generated by CHI %v", objectMeta.Name, err)
	}

	chi, err := c.createChiFromObjectMeta(objectMeta)
	if err != nil {
		return nil, err
	}

	cluster := chi.FindCluster(clusterName)
	if cluster == nil {
		return nil, fmt.Errorf("can't find cluster %s in CHI %s", clusterName, chi.Name)
	}

	return cluster, nil
}
