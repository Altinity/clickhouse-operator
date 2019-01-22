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
	v1 "github.com/schechun/k8s-custom-resource-operator/pkg/apis/clickhouse.example.com/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
)

// ClickHouseClusterLister helps list ClickHouseClusters.
type ClickHouseClusterLister interface {
	// List lists all ClickHouseClusters in the indexer.
	List(selector labels.Selector) (ret []*v1.ClickHouseCluster, err error)
	// ClickHouseClusters returns an object that can list and get ClickHouseClusters.
	ClickHouseClusters(namespace string) ClickHouseClusterNamespaceLister
	ClickHouseClusterListerExpansion
}

// clickHouseClusterLister implements the ClickHouseClusterLister interface.
type clickHouseClusterLister struct {
	indexer cache.Indexer
}

// NewClickHouseClusterLister returns a new ClickHouseClusterLister.
func NewClickHouseClusterLister(indexer cache.Indexer) ClickHouseClusterLister {
	return &clickHouseClusterLister{indexer: indexer}
}

// List lists all ClickHouseClusters in the indexer.
func (s *clickHouseClusterLister) List(selector labels.Selector) (ret []*v1.ClickHouseCluster, err error) {
	err = cache.ListAll(s.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*v1.ClickHouseCluster))
	})
	return ret, err
}

// ClickHouseClusters returns an object that can list and get ClickHouseClusters.
func (s *clickHouseClusterLister) ClickHouseClusters(namespace string) ClickHouseClusterNamespaceLister {
	return clickHouseClusterNamespaceLister{indexer: s.indexer, namespace: namespace}
}

// ClickHouseClusterNamespaceLister helps list and get ClickHouseClusters.
type ClickHouseClusterNamespaceLister interface {
	// List lists all ClickHouseClusters in the indexer for a given namespace.
	List(selector labels.Selector) (ret []*v1.ClickHouseCluster, err error)
	// Get retrieves the ClickHouseCluster from the indexer for a given namespace and name.
	Get(name string) (*v1.ClickHouseCluster, error)
	ClickHouseClusterNamespaceListerExpansion
}

// clickHouseClusterNamespaceLister implements the ClickHouseClusterNamespaceLister
// interface.
type clickHouseClusterNamespaceLister struct {
	indexer   cache.Indexer
	namespace string
}

// List lists all ClickHouseClusters in the indexer for a given namespace.
func (s clickHouseClusterNamespaceLister) List(selector labels.Selector) (ret []*v1.ClickHouseCluster, err error) {
	err = cache.ListAllByNamespace(s.indexer, s.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*v1.ClickHouseCluster))
	})
	return ret, err
}

// Get retrieves the ClickHouseCluster from the indexer for a given namespace and name.
func (s clickHouseClusterNamespaceLister) Get(name string) (*v1.ClickHouseCluster, error) {
	obj, exists, err := s.indexer.GetByKey(s.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(v1.Resource("clickhousecluster"), name)
	}
	return obj.(*v1.ClickHouseCluster), nil
}
