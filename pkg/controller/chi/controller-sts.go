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
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/model/common/interfaces"
)

// getStatefulSet gets StatefulSet. Accepted types:
//  1. *meta.ObjectMeta
//  2. *chop.Host
func (c *Controller) getStatefulSet(obj interface{}) (*apps.StatefulSet, error) {
	switch typedObj := obj.(type) {
	case meta.Object:
		return c.getStatefulSetByMeta(typedObj)
	case *api.Host:
		return c.getStatefulSetByHost(typedObj)
	}
	return nil, fmt.Errorf("unknown type")
}

// getStatefulSet gets StatefulSet either by namespaced name or by labels
func (c *Controller) getStatefulSetByMeta(meta meta.Object) (*apps.StatefulSet, error) {
	return c.kubeClient.AppsV1().StatefulSets(meta.GetNamespace()).Get(controller.NewContext(), meta.GetName(), controller.NewGetOptions())
}

// getStatefulSetByHost finds StatefulSet of a specified host
func (c *Controller) getStatefulSetByHost(host *api.Host) (*apps.StatefulSet, error) {
	// Namespaced name
	name := c.namer.Name(interfaces.NameStatefulSet, host)
	namespace := host.Runtime.Address.Namespace

	return c.kubeClient.AppsV1().StatefulSets(namespace).Get(controller.NewContext(), name, controller.NewGetOptions())
}
