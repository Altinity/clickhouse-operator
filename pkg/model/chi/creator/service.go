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
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags"
	"github.com/altinity/clickhouse-operator/pkg/model/k8s"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type ServiceType string

const (
	ServiceCHI        ServiceType = "svc chi"
	ServiceCHICluster ServiceType = "svc chi cluster"
	ServiceCHIShard   ServiceType = "svc chi shard"
	ServiceCHIHost    ServiceType = "svc chi host"
)

func (c *Creator) CreateService(what ServiceType, params ...any) *core.Service {
	switch what {
	case ServiceCHI:
		return c.createServiceCHI()
	case ServiceCHICluster:
		var cluster api.ICluster
		if len(params) > 0 {
			cluster = params[0].(api.ICluster)
		}
		return c.createServiceCluster(cluster)
	case ServiceCHIShard:
		var shard api.IShard
		if len(params) > 0 {
			shard = params[0].(api.IShard)
		}
		return c.createServiceShard(shard)
	case ServiceCHIHost:
		var host *api.Host
		if len(params) > 0 {
			host = params[0].(*api.Host)
		}
		return c.createServiceHost(host)
	default:
		return nil
	}
}

// createServiceCHI creates new core.Service for specified CHI
func (c *Creator) createServiceCHI() *core.Service {
	if template, ok := c.cr.GetRootServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return c.createServiceFromTemplate(
			template,
			c.cr.GetNamespace(),
			namer.Name(namer.NameCHIService, c.cr),
			c.tagger.Label(tags.LabelServiceCHI, c.cr),
			c.tagger.Annotate(tags.AnnotateServiceCHI, c.cr),
			c.tagger.Selector(tags.SelectorCHIScopeReady),
			createOwnerReferences(c.cr),
			namer.Macro(c.cr),
		)
	}

	// Create default Service
	// We do not have .templates.ServiceTemplate specified or it is incorrect
	svc := &core.Service{
		ObjectMeta: meta.ObjectMeta{
			Name:            namer.Name(namer.NameCHIService, c.cr),
			Namespace:       c.cr.GetNamespace(),
			Labels:          namer.Macro(c.cr).Map(c.tagger.Label(tags.LabelServiceCHI, c.cr)),
			Annotations:     namer.Macro(c.cr).Map(c.tagger.Annotate(tags.AnnotateServiceCHI, c.cr)),
			OwnerReferences: createOwnerReferences(c.cr),
		},
		Spec: core.ServiceSpec{
			ClusterIP: model.TemplateDefaultsServiceClusterIP,
			Ports: []core.ServicePort{
				{
					Name:       api.ChDefaultHTTPPortName,
					Protocol:   core.ProtocolTCP,
					Port:       api.ChDefaultHTTPPortNumber,
					TargetPort: intstr.FromString(api.ChDefaultHTTPPortName),
				},
				{
					Name:       api.ChDefaultTCPPortName,
					Protocol:   core.ProtocolTCP,
					Port:       api.ChDefaultTCPPortNumber,
					TargetPort: intstr.FromString(api.ChDefaultTCPPortName),
				},
			},
			Selector: c.tagger.Selector(tags.SelectorCHIScopeReady),
			Type:     core.ServiceTypeClusterIP,
			// ExternalTrafficPolicy: core.ServiceExternalTrafficPolicyTypeLocal, // For core.ServiceTypeLoadBalancer only
		},
	}
	tags.MakeObjectVersion(svc.GetObjectMeta(), svc)
	return svc
}

// createServiceCluster creates new core.Service for specified Cluster
func (c *Creator) createServiceCluster(cluster api.ICluster) *core.Service {
	serviceName := namer.Name(namer.NameClusterService, cluster)
	ownerReferences := createOwnerReferences(c.cr)

	c.a.V(1).F().Info("%s/%s", cluster.GetRuntime().GetAddress().GetNamespace(), serviceName)
	if template, ok := cluster.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return c.createServiceFromTemplate(
			template,
			cluster.GetRuntime().GetAddress().GetNamespace(),
			serviceName,
			c.tagger.Label(tags.LabelServiceCluster, cluster),
			c.tagger.Annotate(tags.AnnotateServiceCluster, cluster),
			c.tagger.Selector(tags.SelectorClusterScopeReady, cluster),
			ownerReferences,
			namer.Macro(cluster),
		)
	}
	// No template specified, no need to create service
	return nil
}

// createServiceShard creates new core.Service for specified Shard
func (c *Creator) createServiceShard(shard api.IShard) *core.Service {
	if template, ok := shard.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return c.createServiceFromTemplate(
			template,
			shard.GetRuntime().GetAddress().GetNamespace(),
			namer.Name(namer.NameShardService, shard),
			c.tagger.Label(tags.LabelServiceShard, shard),
			c.tagger.Annotate(tags.AnnotateServiceShard, shard),
			c.tagger.Selector(tags.SelectorShardScopeReady, shard),
			createOwnerReferences(c.cr),
			namer.Macro(shard),
		)
	}
	// No template specified, no need to create service
	return nil
}

// createServiceHost creates new core.Service for specified host
func (c *Creator) createServiceHost(host *api.Host) *core.Service {
	if template, ok := host.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return c.createServiceFromTemplate(
			template,
			host.Runtime.Address.Namespace,
			namer.Name(namer.NameStatefulSetService, host),
			c.tagger.Label(tags.LabelServiceHost, host),
			c.tagger.Annotate(tags.AnnotateServiceHost, host),
			c.tagger.Selector(tags.SelectorHostScope, host),
			createOwnerReferences(c.cr),
			namer.Macro(host),
		)
	}

	// Create default Service
	// We do not have .templates.ServiceTemplate specified or it is incorrect
	svc := &core.Service{
		ObjectMeta: meta.ObjectMeta{
			Name:            namer.Name(namer.NameStatefulSetService, host),
			Namespace:       host.Runtime.Address.Namespace,
			Labels:          namer.Macro(host).Map(c.tagger.Label(tags.LabelServiceHost, host)),
			Annotations:     namer.Macro(host).Map(c.tagger.Annotate(tags.AnnotateServiceHost, host)),
			OwnerReferences: createOwnerReferences(c.cr),
		},
		Spec: core.ServiceSpec{
			Selector:                 c.tagger.Selector(tags.SelectorHostScope, host),
			ClusterIP:                model.TemplateDefaultsServiceClusterIP,
			Type:                     "ClusterIP",
			PublishNotReadyAddresses: true,
		},
	}
	svcAppendSpecifiedPorts(svc, host)
	tags.MakeObjectVersion(svc.GetObjectMeta(), svc)
	return svc
}

func svcAppendSpecifiedPorts(service *core.Service, host *api.Host) {
	// Walk over all assigned ports of the host and append each port to the list of service's ports
	host.WalkAssignedPorts(
		func(name string, port *api.Int32, protocol core.Protocol) bool {
			// Append assigned port to the list of service's ports
			service.Spec.Ports = append(service.Spec.Ports,
				core.ServicePort{
					Name:       name,
					Protocol:   protocol,
					Port:       port.Value(),
					TargetPort: intstr.FromInt(port.IntValue()),
				},
			)
			// Do not abort, continue iterating
			return false
		},
	)
}

// createServiceFromTemplate create Service from ServiceTemplate and additional info
func (c *Creator) createServiceFromTemplate(
	template *api.ServiceTemplate,
	namespace string,
	name string,
	labels map[string]string,
	annotations map[string]string,
	selector map[string]string,
	ownerReferences []meta.OwnerReference,
	macro *namer.MacrosEngine,
) *core.Service {

	// Verify Ports
	if err := k8s.ServiceSpecVerifyPorts(&template.Spec); err != nil {
		c.a.V(1).F().Warning(fmt.Sprintf("template: %s err: %s", template.Name, err))
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
	tags.MakeObjectVersion(service.GetObjectMeta(), service)

	return service
}
