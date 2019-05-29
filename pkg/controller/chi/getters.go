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

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
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
		labelApp, ok1 := obj.Labels[chopmodel.LabelApp]
		labelChi, ok2 := obj.Labels[chopmodel.LabelChi]
		labelCluster, ok3 := obj.Labels[chopmodel.LabelCluster]
		labelClusterIndex, ok4 := obj.Labels[chopmodel.LabelClusterIndex]
		labelReplicaIndex, ok5 := obj.Labels[chopmodel.LabelReplicaIndex]

		if ok1 && ok2 && ok3 && ok4 && ok5 {
			set := labels.Set{
				chopmodel.LabelApp:          labelApp,
				chopmodel.LabelChi:          labelChi,
				chopmodel.LabelCluster:      labelCluster,
				chopmodel.LabelClusterIndex: labelClusterIndex,
				chopmodel.LabelReplicaIndex: labelReplicaIndex,
			}
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
		labelApp, ok1 := obj.Labels[chopmodel.LabelApp]
		labelChi, ok2 := obj.Labels[chopmodel.LabelChi]
		labelCluster, ok3 := obj.Labels[chopmodel.LabelCluster]
		labelClusterIndex, ok4 := obj.Labels[chopmodel.LabelClusterIndex]
		labelReplicaIndex, ok5 := obj.Labels[chopmodel.LabelReplicaIndex]

		if ok1 && ok2 && ok3 && ok4 && ok5 {
			set := labels.Set{
				chopmodel.LabelApp:          labelApp,
				chopmodel.LabelChi:          labelChi,
				chopmodel.LabelCluster:      labelCluster,
				chopmodel.LabelClusterIndex: labelClusterIndex,
				chopmodel.LabelReplicaIndex: labelReplicaIndex,
			}
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
		labelApp, ok1 := obj.Labels[chopmodel.LabelApp]
		labelChi, ok2 := obj.Labels[chopmodel.LabelChi]
		labelCluster, ok3 := obj.Labels[chopmodel.LabelCluster]
		labelClusterIndex, ok4 := obj.Labels[chopmodel.LabelClusterIndex]
		labelReplicaIndex, ok5 := obj.Labels[chopmodel.LabelReplicaIndex]

		if ok1 && ok2 && ok3 && ok4 && ok5 {
			set := labels.Set{
				chopmodel.LabelApp:          labelApp,
				chopmodel.LabelChi:          labelChi,
				chopmodel.LabelCluster:      labelCluster,
				chopmodel.LabelClusterIndex: labelClusterIndex,
				chopmodel.LabelReplicaIndex: labelReplicaIndex,
			}
			selector := labels.SelectorFromSet(set)

			objects, err := c.statefulSetLister.StatefulSets(obj.Namespace).List(selector)
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

// TODO move labels into models modules
func (c *Controller) createChiFromObjectMeta(objectMeta *meta.ObjectMeta) (*chi.ClickHouseInstallation, error) {
	// Parse Labels
	//			Labels: map[string]string{
	//				labelChop: AppVersion,
	//				LabelChi:  replica.Address.ChiName,
	//				LabelCluster: replica.Address.ClusterName,
	//				LabelClusterIndex: strconv.Itoa(replica.Address.ClusterIndex),
	//				LabelReplicaIndex: strconv.Itoa(replica.Address.ReplicaIndex),
	//			},

	// ObjectMeta must have some labels
	if len(objectMeta.Labels) == 0 {
		return nil, fmt.Errorf("ObjectMeta %s does not have labels", objectMeta.Name)
	}

	// ObjectMeta must have LabelChi:  chi.Name label
	chiName, ok := objectMeta.Labels[chopmodel.LabelChi]
	if !ok {
		return nil, fmt.Errorf("ObjectMeta %s does not generated by CHI", objectMeta.Name)
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

// TODO move labels into models modules
func (c *Controller) createClusterFromObjectMeta(objectMeta *meta.ObjectMeta) (*chi.ChiCluster, error) {
	// Parse Labels
	// 			Labels: map[string]string{
	//				labelChop: AppVersion,
	//				LabelChi:  replica.Address.ChiName,
	//				LabelCluster: replica.Address.ClusterName,
	//				LabelClusterIndex: strconv.Itoa(replica.Address.ClusterIndex),
	//				LabelReplicaIndex: strconv.Itoa(replica.Address.ReplicaIndex),
	//			},

	// ObjectMeta must have some labels
	if len(objectMeta.Labels) == 0 {
		return nil, fmt.Errorf("ObjectMeta %s does not have labels", objectMeta.Name)
	}

	// ObjectMeta must have LabelCluster
	clusterName, ok := objectMeta.Labels[chopmodel.LabelCluster]
	if !ok {
		return nil, fmt.Errorf("ObjectMeta %s does not generated by CHI", objectMeta.Name)
	}

	chi, err := c.createChiFromObjectMeta(objectMeta)
	if err != nil {
		return nil, err
	}

	cluster := chi.FindCluster(clusterName)
	if cluster == nil {
		return nil, fmt.Errorf("Can't find cluster %s in CHI %s", clusterName, chi.Name)
	}

	return cluster, nil
}
