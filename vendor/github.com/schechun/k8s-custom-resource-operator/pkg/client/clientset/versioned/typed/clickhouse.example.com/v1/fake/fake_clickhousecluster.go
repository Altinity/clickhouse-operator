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

package fake

import (
	clickhouse_example_com_v1 "github.com/schechun/k8s-custom-resource-operator/pkg/apis/clickhouse.example.com/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
)

// FakeClickHouseClusters implements ClickHouseClusterInterface
type FakeClickHouseClusters struct {
	Fake *FakeClickhouseV1
	ns   string
}

var clickhouseclustersResource = schema.GroupVersionResource{Group: "clickhouse.example.com", Version: "v1", Resource: "clickhouseclusters"}

var clickhouseclustersKind = schema.GroupVersionKind{Group: "clickhouse.example.com", Version: "v1", Kind: "ClickHouseCluster"}

// Get takes name of the clickHouseCluster, and returns the corresponding clickHouseCluster object, and an error if there is any.
func (c *FakeClickHouseClusters) Get(name string, options v1.GetOptions) (result *clickhouse_example_com_v1.ClickHouseCluster, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewGetAction(clickhouseclustersResource, c.ns, name), &clickhouse_example_com_v1.ClickHouseCluster{})

	if obj == nil {
		return nil, err
	}
	return obj.(*clickhouse_example_com_v1.ClickHouseCluster), err
}

// List takes label and field selectors, and returns the list of ClickHouseClusters that match those selectors.
func (c *FakeClickHouseClusters) List(opts v1.ListOptions) (result *clickhouse_example_com_v1.ClickHouseClusterList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewListAction(clickhouseclustersResource, clickhouseclustersKind, c.ns, opts), &clickhouse_example_com_v1.ClickHouseClusterList{})

	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &clickhouse_example_com_v1.ClickHouseClusterList{}
	for _, item := range obj.(*clickhouse_example_com_v1.ClickHouseClusterList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested clickHouseClusters.
func (c *FakeClickHouseClusters) Watch(opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchAction(clickhouseclustersResource, c.ns, opts))

}

// Create takes the representation of a clickHouseCluster and creates it.  Returns the server's representation of the clickHouseCluster, and an error, if there is any.
func (c *FakeClickHouseClusters) Create(clickHouseCluster *clickhouse_example_com_v1.ClickHouseCluster) (result *clickhouse_example_com_v1.ClickHouseCluster, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewCreateAction(clickhouseclustersResource, c.ns, clickHouseCluster), &clickhouse_example_com_v1.ClickHouseCluster{})

	if obj == nil {
		return nil, err
	}
	return obj.(*clickhouse_example_com_v1.ClickHouseCluster), err
}

// Update takes the representation of a clickHouseCluster and updates it. Returns the server's representation of the clickHouseCluster, and an error, if there is any.
func (c *FakeClickHouseClusters) Update(clickHouseCluster *clickhouse_example_com_v1.ClickHouseCluster) (result *clickhouse_example_com_v1.ClickHouseCluster, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateAction(clickhouseclustersResource, c.ns, clickHouseCluster), &clickhouse_example_com_v1.ClickHouseCluster{})

	if obj == nil {
		return nil, err
	}
	return obj.(*clickhouse_example_com_v1.ClickHouseCluster), err
}

// Delete takes name of the clickHouseCluster and deletes it. Returns an error if one occurs.
func (c *FakeClickHouseClusters) Delete(name string, options *v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteAction(clickhouseclustersResource, c.ns, name), &clickhouse_example_com_v1.ClickHouseCluster{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeClickHouseClusters) DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error {
	action := testing.NewDeleteCollectionAction(clickhouseclustersResource, c.ns, listOptions)

	_, err := c.Fake.Invokes(action, &clickhouse_example_com_v1.ClickHouseClusterList{})
	return err
}

// Patch applies the patch and returns the patched clickHouseCluster.
func (c *FakeClickHouseClusters) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *clickhouse_example_com_v1.ClickHouseCluster, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceAction(clickhouseclustersResource, c.ns, name, data, subresources...), &clickhouse_example_com_v1.ClickHouseCluster{})

	if obj == nil {
		return nil, err
	}
	return obj.(*clickhouse_example_com_v1.ClickHouseCluster), err
}
