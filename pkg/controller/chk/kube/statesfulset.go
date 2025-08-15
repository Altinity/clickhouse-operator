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
	"context"

	"gopkg.in/yaml.v3"

	apps "k8s.io/api/apps/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
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

// Get gets StatefulSet. Accepted types:
//  1. *meta.ObjectMeta
//  2. *chop.Host
func (c *STS) Get(ctx context.Context, params ...any) (*apps.StatefulSet, error) {
	var name, namespace string
	switch len(params) {
	case 2:
		// Expecting namespace name
		namespace = params[0].(string)
		name = params[1].(string)
	case 1:
		// Expecting obj
		obj := params[0]
		switch typedObj := obj.(type) {
		case meta.Object:
			name = typedObj.GetName()
			namespace = typedObj.GetNamespace()
		case *api.Host:
			// Namespaced name
			name = c.namer.Name(interfaces.NameStatefulSet, obj)
			namespace = typedObj.Runtime.Address.Namespace
		}
	}
	return c.get(ctx, namespace, name)
}

func (c *STS) get(ctx context.Context, namespace, name string) (*apps.StatefulSet, error) {
	sts := &apps.StatefulSet{}
	err := c.kubeClient.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, sts)
	if err == nil {
		return sts, nil
	} else {
		return nil, err
	}
}

func (c *STS) Create(ctx context.Context, sts *apps.StatefulSet) (*apps.StatefulSet, error) {
	yamlBytes, _ := yaml.Marshal(sts)
	log.V(3).M(sts).Info("Going to create STS: %s\n%s", util.NamespaceNameString(sts), string(yamlBytes))
	err := c.kubeClient.Create(ctx, sts)
	return sts, err
}

func (c *STS) Update(ctx context.Context, sts *apps.StatefulSet) (*apps.StatefulSet, error) {
	log.V(3).M(sts).Info("Going to update STS: %s", util.NamespaceNameString(sts))
	err := c.kubeClient.Update(ctx, sts)
	return sts, err
}

func (c *STS) Delete(ctx context.Context, namespace, name string) error {
	log.V(3).M(namespace, name).Info("Going to delete STS: %s/%s", namespace, name)
	sts := &apps.StatefulSet{
		ObjectMeta: meta.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
	}
	return c.kubeClient.Delete(ctx, sts)
}

func (c *STS) List(ctx context.Context, namespace string, opts meta.ListOptions) ([]apps.StatefulSet, error) {
	list := &apps.StatefulSetList{}
	selector, err := labels.Parse(opts.LabelSelector)
	if err != nil {
		return nil, err
	}
	err = c.kubeClient.List(ctx, list, &client.ListOptions{
		Namespace:     namespace,
		LabelSelector: selector,
	})
	if err != nil {
		return nil, err
	}
	if list == nil {
		return nil, err
	}
	return list.Items, nil
}
