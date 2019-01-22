/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	time "time"

	clickhouse_example_com_v1 "github.com/schechun/k8s-custom-resource-operator/pkg/apis/clickhouse.example.com/v1"
	versioned "github.com/schechun/k8s-custom-resource-operator/pkg/client/clientset/versioned"
	internalinterfaces "github.com/schechun/k8s-custom-resource-operator/pkg/client/informers/externalversions/internalinterfaces"
	v1 "github.com/schechun/k8s-custom-resource-operator/pkg/client/listers/clickhouse.example.com/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	watch "k8s.io/apimachinery/pkg/watch"
	cache "k8s.io/client-go/tools/cache"
)

// ClickHouseClusterInformer provides access to a shared informer and lister for
// ClickHouseClusters.
type ClickHouseClusterInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() v1.ClickHouseClusterLister
}

type clickHouseClusterInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
	namespace        string
}

// NewClickHouseClusterInformer constructs a new informer for ClickHouseCluster type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewClickHouseClusterInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredClickHouseClusterInformer(client, namespace, resyncPeriod, indexers, nil)
}

// NewFilteredClickHouseClusterInformer constructs a new informer for ClickHouseCluster type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredClickHouseClusterInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.ClickhouseV1().ClickHouseClusters(namespace).List(options)
			},
			WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.ClickhouseV1().ClickHouseClusters(namespace).Watch(options)
			},
		},
		&clickhouse_example_com_v1.ClickHouseCluster{},
		resyncPeriod,
		indexers,
	)
}

func (f *clickHouseClusterInformer) defaultInformer(client versioned.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredClickHouseClusterInformer(client, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *clickHouseClusterInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&clickhouse_example_com_v1.ClickHouseCluster{}, f.defaultInformer)
}

func (f *clickHouseClusterInformer) Lister() v1.ClickHouseClusterLister {
	return v1.NewClickHouseClusterLister(f.Informer().GetIndexer())
}
