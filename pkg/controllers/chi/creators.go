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
	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopparser "github.com/altinity/clickhouse-operator/pkg/parser"
	"github.com/golang/glog"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// createCHIResources creates k8s resources based on ClickHouseInstallation object specification
func (c *Controller) createCHIResources(chi *chop.ClickHouseInstallation) (*chop.ClickHouseInstallation, error) {
	chiCopy := chi.DeepCopy()
	_ = chopparser.CHINormalize(chiCopy)
	listOfLists := chopparser.CHICreateObjects(chiCopy)
	chiCopy.Status = chop.ChiStatus{}

	for i := range listOfLists {
		switch listOfLists[i].(type) {
		case chopparser.ConfigMapList:
			for j := range listOfLists[i].(chopparser.ConfigMapList) {
				if err := c.createConfigMapResource(chiCopy, listOfLists[i].(chopparser.ConfigMapList)[j]); err != nil {
					return nil, err
				}
			}
		case chopparser.ServiceList:
			for j := range listOfLists[i].(chopparser.ConfigMapList) {
				if err := c.createServiceResource(chiCopy, listOfLists[i].(chopparser.ServiceList)[j]); err != nil {
					return nil, err
				}
			}
		case chopparser.StatefulSetList:
			for j := range listOfLists[i].(chopparser.StatefulSetList) {
				if err := c.createStatefulSetResource(chiCopy, listOfLists[i].(chopparser.StatefulSetList)[j]); err != nil {
					return nil, err
				}
			}
		}
	}

	return chiCopy, nil
}

// createConfigMapResource creates core.ConfigMap resource
func (c *Controller) createConfigMapResource(chi *chop.ClickHouseInstallation, newConfigMap *core.ConfigMap) error {
	// Check whether object with such name already exists in k8s
	res, err := c.configMapLister.ConfigMaps(chi.Namespace).Get(newConfigMap.Name)
	if res != nil {
		// Object with such name already exists, this is not an error
		glog.Infof("Update ConfigMap %s/%s\n", newConfigMap.Namespace, newConfigMap.Name)
		_, err := c.kubeClient.CoreV1().ConfigMaps(chi.Namespace).Update(newConfigMap)
		if err != nil {
			return err
		}
		return nil
	}

	// Object with such name does not exist or error happened

	if apierrors.IsNotFound(err) {
		// Object with such name not found - create it
		_, err = c.kubeClient.CoreV1().ConfigMaps(chi.Namespace).Create(newConfigMap)
	}
	if err != nil {
		return err
	}

	// Object created
	return nil
}

// createServiceResource creates core.Service resource
func (c *Controller) createServiceResource(chi *chop.ClickHouseInstallation, newService *core.Service) error {
	// Check whether object with such name already exists in k8s
	res, err := c.serviceLister.Services(chi.Namespace).Get(newService.Name)
	if res != nil {
		// Object with such name already exists, this is not an error
		return nil
	}

	// Object with such name does not exist or error happened

	if apierrors.IsNotFound(err) {
		// Object with such name not found - create it
		_, err = c.kubeClient.CoreV1().Services(chi.Namespace).Create(newService)
	}
	if err != nil {
		return err
	}

	// Object created
	return nil
}

// createStatefulSetResource creates apps.StatefulSet resource
func (c *Controller) createStatefulSetResource(chi *chop.ClickHouseInstallation, newStatefulSet *apps.StatefulSet) error {
	// Check whether object with such name already exists in k8s
	res, err := c.statefulSetLister.StatefulSets(chi.Namespace).Get(newStatefulSet.Name)
	if res != nil {
		// Object with such name already exists, this is not an error
		//		glog.Infof("Update StatefulSet %s/%s\n", newStatefulSet.Namespace, newStatefulSet.Name)
		//		_, err := c.kubeClient.AppsV1().StatefulSets(chi.Namespace).Update(newStatefulSet)
		//		if err != nil {
		//			return err
		//		}
		return nil
	}

	if apierrors.IsNotFound(err) {
		// Object with such name not found - create it
		_, err = c.kubeClient.AppsV1().StatefulSets(chi.Namespace).Create(newStatefulSet)
	}
	if err != nil {
		return err
	}

	// Object created
	return nil
}
