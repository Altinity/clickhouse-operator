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

	"gopkg.in/yaml.v3"

	apps "k8s.io/api/apps/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type STS struct {
	kube  client.Client
	namer interfaces.INameManager
}

func NewSTS(kubeClient client.Client, namer interfaces.INameManager) *STS {
	return &STS{
		kube:  kubeClient,
		namer: namer,
	}
}

// Get gets StatefulSet. Accepted types:
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
	err := c.kube.Get(controller.NewContext(), types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, sts)
	if err == nil {
		return sts, nil
	} else {
		return nil, err
	}
}

func (c *STS) Create(sts *apps.StatefulSet) (*apps.StatefulSet, error) {
	yamlBytes, _ := yaml.Marshal(sts)
	log.V(3).M(sts).Info("Going to create STS: %s\n%s", util.NamespaceNameString(sts), string(yamlBytes))
	err := c.kube.Create(controller.NewContext(), sts)
	return sts, err
}

func (c *STS) Update(sts *apps.StatefulSet) (*apps.StatefulSet, error) {
	log.V(3).M(sts).Info("Going to update STS: %s", util.NamespaceNameString(sts))
	err := c.kube.Update(controller.NewContext(), sts)
	return sts, err
}

func (c *STS) Delete(namespace, name string) error {
	log.V(3).M(namespace, name).Info("Going to delete STS: %s/%s", namespace, name)
	sts := &apps.StatefulSet{
		ObjectMeta: meta.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
	}
	return c.kube.Delete(controller.NewContext(), sts)
}
