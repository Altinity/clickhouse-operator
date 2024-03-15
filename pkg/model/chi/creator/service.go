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

package creator

import (
	"fmt"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chi"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// CreateServiceCHI creates new core.Service for specified CHI
func (c *Creator) CreateServiceCHI() *core.Service {
	serviceName := model.CreateCHIServiceName(c.chi)
	ownerReferences := getOwnerReferences(c.chi)

	c.a.V(1).F().Info("%s/%s", c.chi.Namespace, serviceName)
	if template, ok := c.chi.GetCHIServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return c.createServiceFromTemplate(
			template,
			c.chi.Namespace,
			serviceName,
			c.labels.GetServiceCHI(c.chi),
			c.annotations.GetServiceCHI(c.chi),
			c.labels.GetSelectorCHIScopeReady(),
			ownerReferences,
			model.Macro(c.chi),
		)
	}

	// Create default Service
	// We do not have .templates.ServiceTemplate specified or it is incorrect
	svc := &core.Service{
		ObjectMeta: meta.ObjectMeta{
			Name:            serviceName,
			Namespace:       c.chi.Namespace,
			Labels:          model.Macro(c.chi).Map(c.labels.GetServiceCHI(c.chi)),
			Annotations:     model.Macro(c.chi).Map(c.annotations.GetServiceCHI(c.chi)),
			OwnerReferences: ownerReferences,
		},
		Spec: core.ServiceSpec{
			ClusterIP: model.TemplateDefaultsServiceClusterIP,
			Ports: []core.ServicePort{
				{
					Name:       model.ChDefaultHTTPPortName,
					Protocol:   core.ProtocolTCP,
					Port:       model.ChDefaultHTTPPortNumber,
					TargetPort: intstr.FromString(model.ChDefaultHTTPPortName),
				},
				{
					Name:       model.ChDefaultTCPPortName,
					Protocol:   core.ProtocolTCP,
					Port:       model.ChDefaultTCPPortNumber,
					TargetPort: intstr.FromString(model.ChDefaultTCPPortName),
				},
			},
			Selector: c.labels.GetSelectorCHIScopeReady(),
			Type:     core.ServiceTypeClusterIP,
			// ExternalTrafficPolicy: core.ServiceExternalTrafficPolicyTypeLocal, // For core.ServiceTypeLoadBalancer only
		},
	}
	model.MakeObjectVersion(&svc.ObjectMeta, svc)
	return svc
}

// CreateServiceCluster creates new core.Service for specified Cluster
func (c *Creator) CreateServiceCluster(cluster *api.Cluster) *core.Service {
	serviceName := model.CreateClusterServiceName(cluster)
	ownerReferences := getOwnerReferences(c.chi)

	c.a.V(1).F().Info("%s/%s", cluster.Address.Namespace, serviceName)
	if template, ok := cluster.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return c.createServiceFromTemplate(
			template,
			cluster.Address.Namespace,
			serviceName,
			c.labels.GetServiceCluster(cluster),
			c.annotations.GetServiceCluster(cluster),
			model.GetSelectorClusterScopeReady(cluster),
			ownerReferences,
			model.Macro(cluster),
		)
	}
	// No template specified, no need to create service
	return nil
}

// CreateServiceShard creates new core.Service for specified Shard
func (c *Creator) CreateServiceShard(shard *api.ChiShard) *core.Service {
	serviceName := model.CreateShardServiceName(shard)
	ownerReferences := getOwnerReferences(c.chi)

	c.a.V(1).F().Info("%s/%s", shard.Address.Namespace, serviceName)
	if template, ok := shard.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return c.createServiceFromTemplate(
			template,
			shard.Address.Namespace,
			serviceName,
			c.labels.GetServiceShard(shard),
			c.annotations.GetServiceShard(shard),
			model.GetSelectorShardScopeReady(shard),
			ownerReferences,
			model.Macro(shard),
		)
	}
	// No template specified, no need to create service
	return nil
}

// CreateServiceHost creates new core.Service for specified host
func (c *Creator) CreateServiceHost(host *api.ChiHost) *core.Service {
	serviceName := model.CreateStatefulSetServiceName(host)
	statefulSetName := model.CreateStatefulSetName(host)
	ownerReferences := getOwnerReferences(c.chi)

	c.a.V(1).F().Info("%s/%s for Set %s", host.Runtime.Address.Namespace, serviceName, statefulSetName)
	if template, ok := host.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return c.createServiceFromTemplate(
			template,
			host.Runtime.Address.Namespace,
			serviceName,
			c.labels.GetServiceHost(host),
			c.annotations.GetServiceHost(host),
			model.GetSelectorHostScope(host),
			ownerReferences,
			model.Macro(host),
		)
	}

	// Create default Service
	// We do not have .templates.ServiceTemplate specified or it is incorrect
	svc := &core.Service{
		ObjectMeta: meta.ObjectMeta{
			Name:            serviceName,
			Namespace:       host.Runtime.Address.Namespace,
			Labels:          model.Macro(host).Map(c.labels.GetServiceHost(host)),
			Annotations:     model.Macro(host).Map(c.annotations.GetServiceHost(host)),
			OwnerReferences: ownerReferences,
		},
		Spec: core.ServiceSpec{
			Selector:                 model.GetSelectorHostScope(host),
			ClusterIP:                model.TemplateDefaultsServiceClusterIP,
			Type:                     "ClusterIP",
			PublishNotReadyAddresses: true,
		},
	}
	appendServicePorts(svc, host)
	model.MakeObjectVersion(&svc.ObjectMeta, svc)
	return svc
}

func appendServicePorts(service *core.Service, host *api.ChiHost) {
	if api.IsPortAssigned(host.TCPPort) {
		service.Spec.Ports = append(service.Spec.Ports,
			core.ServicePort{
				Name:       model.ChDefaultTCPPortName,
				Protocol:   core.ProtocolTCP,
				Port:       host.TCPPort,
				TargetPort: intstr.FromInt(int(host.TCPPort)),
			},
		)
	}
	if api.IsPortAssigned(host.TLSPort) {
		service.Spec.Ports = append(service.Spec.Ports,
			core.ServicePort{
				Name:       model.ChDefaultTLSPortName,
				Protocol:   core.ProtocolTCP,
				Port:       host.TLSPort,
				TargetPort: intstr.FromInt(int(host.TLSPort)),
			},
		)
	}
	if api.IsPortAssigned(host.HTTPPort) {
		service.Spec.Ports = append(service.Spec.Ports,
			core.ServicePort{
				Name:       model.ChDefaultHTTPPortName,
				Protocol:   core.ProtocolTCP,
				Port:       host.HTTPPort,
				TargetPort: intstr.FromInt(int(host.HTTPPort)),
			},
		)
	}
	if api.IsPortAssigned(host.HTTPSPort) {
		service.Spec.Ports = append(service.Spec.Ports,
			core.ServicePort{
				Name:       model.ChDefaultHTTPSPortName,
				Protocol:   core.ProtocolTCP,
				Port:       host.HTTPSPort,
				TargetPort: intstr.FromInt(int(host.HTTPSPort)),
			},
		)
	}
	if api.IsPortAssigned(host.InterserverHTTPPort) {
		service.Spec.Ports = append(service.Spec.Ports,
			core.ServicePort{
				Name:       model.ChDefaultInterserverHTTPPortName,
				Protocol:   core.ProtocolTCP,
				Port:       host.InterserverHTTPPort,
				TargetPort: intstr.FromInt(int(host.InterserverHTTPPort)),
			},
		)
	}
}

// verifyServiceTemplatePorts verifies ChiServiceTemplate to have reasonable ports specified
func (c *Creator) verifyServiceTemplatePorts(template *api.ChiServiceTemplate) error {
	for i := range template.Spec.Ports {
		servicePort := &template.Spec.Ports[i]
		if api.IsPortInvalid(servicePort.Port) {
			msg := fmt.Sprintf("template:%s INCORRECT PORT:%d", template.Name, servicePort.Port)
			c.a.V(1).F().Warning(msg)
			return fmt.Errorf(msg)
		}
	}
	return nil
}

// createServiceFromTemplate create Service from ChiServiceTemplate and additional info
func (c *Creator) createServiceFromTemplate(
	template *api.ChiServiceTemplate,
	namespace string,
	name string,
	labels map[string]string,
	annotations map[string]string,
	selector map[string]string,
	ownerReferences []meta.OwnerReference,
	macro *model.MacrosEngine,
) *core.Service {

	// Verify Ports
	if err := c.verifyServiceTemplatePorts(template); err != nil {
		return nil
	}

	// Create Service
	service := &core.Service{
		ObjectMeta: *template.ObjectMeta.DeepCopy(),
		Spec:       *template.Spec.DeepCopy(),
	}

	// Overwrite .name and .namespace - they are not allowed to be specified in template
	service.Name = name
	service.Namespace = namespace
	service.OwnerReferences = ownerReferences

	// Combine labels and annotations
	service.Labels = macro.Map(util.MergeStringMapsOverwrite(service.Labels, labels))
	service.Annotations = macro.Map(util.MergeStringMapsOverwrite(service.Annotations, annotations))

	// Append provided Selector to already specified Selector in template
	service.Spec.Selector = util.MergeStringMapsOverwrite(service.Spec.Selector, selector)

	// And after the object is ready we can put version label
	model.MakeObjectVersion(&service.ObjectMeta, service)

	return service
}
