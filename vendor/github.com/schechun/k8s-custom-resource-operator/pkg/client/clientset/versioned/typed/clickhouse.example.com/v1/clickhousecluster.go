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
	scheme "github.com/schechun/k8s-custom-resource-operator/pkg/client/clientset/versioned/scheme"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	rest "k8s.io/client-go/rest"
)

// ClickHouseClustersGetter has a method to return a ClickHouseClusterInterface.
// A group's client should implement this interface.
type ClickHouseClustersGetter interface {
	ClickHouseClusters(namespace string) ClickHouseClusterInterface
}

// ClickHouseClusterInterface has methods to work with ClickHouseCluster resources.
type ClickHouseClusterInterface interface {
	Create(*v1.ClickHouseCluster) (*v1.ClickHouseCluster, error)
	Update(*v1.ClickHouseCluster) (*v1.ClickHouseCluster, error)
	Delete(name string, options *meta_v1.DeleteOptions) error
	DeleteCollection(options *meta_v1.DeleteOptions, listOptions meta_v1.ListOptions) error
	Get(name string, options meta_v1.GetOptions) (*v1.ClickHouseCluster, error)
	List(opts meta_v1.ListOptions) (*v1.ClickHouseClusterList, error)
	Watch(opts meta_v1.ListOptions) (watch.Interface, error)
	Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1.ClickHouseCluster, err error)
	ClickHouseClusterExpansion
}

// clickHouseClusters implements ClickHouseClusterInterface
type clickHouseClusters struct {
	client rest.Interface
	ns     string
}

// newClickHouseClusters returns a ClickHouseClusters
func newClickHouseClusters(c *ClickhouseV1Client, namespace string) *clickHouseClusters {
	return &clickHouseClusters{
		client: c.RESTClient(),
		ns:     namespace,
	}
}

// Get takes name of the clickHouseCluster, and returns the corresponding clickHouseCluster object, and an error if there is any.
func (c *clickHouseClusters) Get(name string, options meta_v1.GetOptions) (result *v1.ClickHouseCluster, err error) {
	result = &v1.ClickHouseCluster{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("clickhouseclusters").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do().
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of ClickHouseClusters that match those selectors.
func (c *clickHouseClusters) List(opts meta_v1.ListOptions) (result *v1.ClickHouseClusterList, err error) {
	result = &v1.ClickHouseClusterList{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("clickhouseclusters").
		VersionedParams(&opts, scheme.ParameterCodec).
		Do().
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested clickHouseClusters.
func (c *clickHouseClusters) Watch(opts meta_v1.ListOptions) (watch.Interface, error) {
	opts.Watch = true
	return c.client.Get().
		Namespace(c.ns).
		Resource("clickhouseclusters").
		VersionedParams(&opts, scheme.ParameterCodec).
		Watch()
}

// Create takes the representation of a clickHouseCluster and creates it.  Returns the server's representation of the clickHouseCluster, and an error, if there is any.
func (c *clickHouseClusters) Create(clickHouseCluster *v1.ClickHouseCluster) (result *v1.ClickHouseCluster, err error) {
	result = &v1.ClickHouseCluster{}
	err = c.client.Post().
		Namespace(c.ns).
		Resource("clickhouseclusters").
		Body(clickHouseCluster).
		Do().
		Into(result)
	return
}

// Update takes the representation of a clickHouseCluster and updates it. Returns the server's representation of the clickHouseCluster, and an error, if there is any.
func (c *clickHouseClusters) Update(clickHouseCluster *v1.ClickHouseCluster) (result *v1.ClickHouseCluster, err error) {
	result = &v1.ClickHouseCluster{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("clickhouseclusters").
		Name(clickHouseCluster.Name).
		Body(clickHouseCluster).
		Do().
		Into(result)
	return
}

// Delete takes name of the clickHouseCluster and deletes it. Returns an error if one occurs.
func (c *clickHouseClusters) Delete(name string, options *meta_v1.DeleteOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("clickhouseclusters").
		Name(name).
		Body(options).
		Do().
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *clickHouseClusters) DeleteCollection(options *meta_v1.DeleteOptions, listOptions meta_v1.ListOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("clickhouseclusters").
		VersionedParams(&listOptions, scheme.ParameterCodec).
		Body(options).
		Do().
		Error()
}

// Patch applies the patch and returns the patched clickHouseCluster.
func (c *clickHouseClusters) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1.ClickHouseCluster, err error) {
	result = &v1.ClickHouseCluster{}
	err = c.client.Patch(pt).
		Namespace(c.ns).
		Resource("clickhouseclusters").
		SubResource(subresources...).
		Name(name).
		Body(data).
		Do().
		Into(result)
	return
}
