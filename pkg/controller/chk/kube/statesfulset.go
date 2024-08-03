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
	"fmt"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	apps "k8s.io/api/apps/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type STS struct {
	kubeClient client.Client
	namer      interfaces.INameManager
}

func NewSTS(kubeClient client.Client, namer interfaces.INameManager) *STS {
	return &STS{
		kubeClient: kubeClient,
		namer:      namer,
	}
}

// getStatefulSet gets StatefulSet. Accepted types:
//  1. *meta.ObjectMeta
//  2. *chop.Host
func (c *STS) Get(obj any) (*apps.StatefulSet, error) {
	switch obj := obj.(type) {
	case meta.Object:
		return c.get(obj.GetNamespace(), obj.GetName())
	case *api.Host:
		// Namespaced name
		name := c.namer.Name(interfaces.NameStatefulSet, obj)
		namespace := obj.Runtime.Address.Namespace

		return c.get(namespace, name)
	}
	return nil, fmt.Errorf("unknown type")
}

func (c *STS) get(namespace, name string) (*apps.StatefulSet, error) {
	sts := &apps.StatefulSet{}
	err := c.kubeClient.Get(controller.NewContext(), types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, sts)
	return sts, err
}

func (c *STS) Create(statefulSet *apps.StatefulSet) (*apps.StatefulSet, error) {
	err := c.kubeClient.Create(controller.NewContext(), statefulSet)
	return statefulSet, err

}

// updateStatefulSet is an internal function, used in reconcileStatefulSet only
func (c *STS) Update(sts *apps.StatefulSet) (*apps.StatefulSet, error) {
	err := c.kubeClient.Update(controller.NewContext(), sts)
	return sts, err
}

// deleteStatefulSet gracefully deletes StatefulSet through zeroing Pod's count
func (c *STS) Delete(namespace, name string) error {
	sts := &apps.StatefulSet{
		ObjectMeta: meta.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
	}
	return c.kubeClient.Delete(controller.NewContext(), sts)
}
