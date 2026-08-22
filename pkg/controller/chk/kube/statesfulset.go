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
	"fmt"

	"gopkg.in/yaml.v3"

	commonKube "github.com/altinity/clickhouse-operator/pkg/controller/common/kube"
	apps "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/poller"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type STS struct {
	kubeClient client.Client
	// apiReader bypasses the controller-runtime cache for reads, giving up-to-date STS state.
	// The cache is write-through but not synchronously invalidated, so a Get() immediately after
	// an Update() can still return a stale object. Using a direct reader avoids that race.
	apiReader client.Reader
	namer     interfaces.INameManager
}

func NewSTS(kubeClient client.Client, apiReader client.Reader, namer interfaces.INameManager) *STS {
	return &STS{
		kubeClient: kubeClient,
		apiReader:  apiReader,
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
	// Use the direct (non-cached) reader so callers always see authoritative STS state from
	// the API server. The controller-runtime cache is updated via watch events and can lag
	// behind writes. Since the STS reconciler makes branching decisions based on live STS
	// state (fingerprint comparison, IsStatefulSetReady, create-vs-update routing), a stale
	// read could cause incorrect reconcile outcomes. Note: this does NOT help with
	// Status.ReadyReplicas lag after a scale-down — that reflects real pod termination time
	// on the API server itself, not a cache artifact. The WaitUntilReady wait in hostScaleDown
	// is what handles that timing gap.
	return commonKube.GetWithRetry(ctx, func() (*apps.StatefulSet, error) {
		sts := &apps.StatefulSet{}
		err := c.apiReader.Get(ctx, types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		}, sts)
		if err != nil {
			return nil, err
		}
		return sts, nil
	})
}

func (c *STS) Create(ctx context.Context, sts *apps.StatefulSet) (*apps.StatefulSet, error) {
	yamlBytes, _ := yaml.Marshal(sts)
	log.V(3).M(sts).Info("Going to create STS: %s\n%s", util.NamespaceNameString(sts), string(yamlBytes))
	err := c.kubeClient.Create(ctx, sts)
	return sts, err
}

// ValidateCreate runs a server-side dry-run create: validation runs, nothing is persisted.
func (c *STS) ValidateCreate(ctx context.Context, sts *apps.StatefulSet) error {
	return c.kubeClient.Create(ctx, sts.DeepCopy(), client.DryRunAll)
}

func (c *STS) Update(ctx context.Context, sts *apps.StatefulSet) (*apps.StatefulSet, error) {
	log.V(3).M(sts).Info("Going to update STS: %s", util.NamespaceNameString(sts))
	err := c.kubeClient.Update(ctx, sts)
	return sts, err
}

// Remove issues a fire-and-forget delete against the API server. It returns immediately after
// the API call accepts (or rejects) the request — the StatefulSet may still be terminating
// when this returns. Callers that need to know the object is actually gone should use Delete.
func (c *STS) Remove(ctx context.Context, namespace, name string) error {
	log.V(3).M(namespace, name).Info("Going to delete STS: %s/%s", namespace, name)
	sts := &apps.StatefulSet{
		ObjectMeta: meta.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
	}
	return c.kubeClient.Delete(ctx, sts)
}

// Delete deletes a StatefulSet and waits until it is actually gone from the API server.
// It polls Get() until IsNotFound to avoid races where a follow-up Create() lands while the
// previous STS is still terminating and fails with AlreadyExists. Mirrors the CHI adapter pattern.
func (c *STS) Delete(ctx context.Context, namespace, name string) error {
	item := "StatefulSet"
	return poller.New(ctx, fmt.Sprintf("delete %s: %s/%s", item, namespace, name)).
		WithOptions(poller.NewOptionsFromConfig()).
		WithFunctions(&poller.Functions{
			IsDone: func(_ctx context.Context, _ any) bool {
				if err := c.Remove(ctx, namespace, name); err != nil {
					if !errors.IsNotFound(err) {
						log.V(1).Warning("Error deleting %s: %s/%s err: %v ", item, namespace, name, err)
					}
				}

				_, err := c.Get(ctx, namespace, name)
				return errors.IsNotFound(err)
			},
		}).Poll()
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
