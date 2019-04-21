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
	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/labels"
)

func (c *Controller) listStatefulSetResources(chi *chop.ClickHouseInstallation) error {
	glog.V(1).Infof("listStatefulSetResources(%s/%s)", chi.Namespace, chi.Name)

	//var selector labels.Selector = labels.Everything()
	var selector = labels.Everything()
	statefulSets, err := c.statefulSetLister.StatefulSets(chi.Namespace).List(selector)
	if err != nil {
		glog.V(1).Infof("listStatefulSetResources(%s/%s) err:%v", chi.Namespace, chi.Name, err)
	}

	for i := range statefulSets {
		glog.V(1).Infof("listStatefulSetResources(%s/%s) ss %s", chi.Namespace, chi.Name, statefulSets[i].Name)
	}

	return nil
}
