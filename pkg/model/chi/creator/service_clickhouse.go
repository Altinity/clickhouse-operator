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
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags/annotator"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chi"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags"
)

type ServiceManagerClickHouse struct {
	cr     api.ICustomResource
	tagger iTagger
}

func NewServiceManagerClickHouse() *ServiceManagerClickHouse {
	return &ServiceManagerClickHouse{}
}

func (m *ServiceManagerClickHouse) CreateService(what ServiceType, params ...any) *core.Service {
	switch what {
	case ServiceCHI:
		return m.createServiceCHI()
	case ServiceCHICluster:
		var cluster api.ICluster
		if len(params) > 0 {
			cluster = params[0].(api.ICluster)
			return m.createServiceCluster(cluster)
		}
	case ServiceCHIShard:
		var shard api.IShard
		if len(params) > 0 {
			shard = params[0].(api.IShard)
			return m.createServiceShard(shard)
		}
	case ServiceCHIHost:
		var host *api.Host
		if len(params) > 0 {
			host = params[0].(*api.Host)
			return m.createServiceHost(host)
		}
	}
	panic("unknown service type")
}

func (m *ServiceManagerClickHouse) SetCR(cr api.ICustomResource) {
	m.cr = cr
}
func (m *ServiceManagerClickHouse) SetTagger(tagger iTagger) {
	m.tagger = tagger
}

// createServiceCHI creates new core.Service for specified CHI
func (m *ServiceManagerClickHouse) createServiceCHI() *core.Service {
	if template, ok := m.cr.GetRootServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return createServiceFromTemplate(
			template,
			m.cr.GetNamespace(),
			namer.Name(namer.NameCHIService, m.cr),
			m.tagger.Label(tags.LabelServiceCHI, m.cr),
			m.tagger.Annotate(annotator.AnnotateServiceCR, m.cr),
			m.tagger.Selector(tags.SelectorCHIScopeReady),
			createOwnerReferences(m.cr),
			namer.Macro(m.cr),
		)
	}

	// Create default Service
	// We do not have .templates.ServiceTemplate specified or it is incorrect
	svc := &core.Service{
		ObjectMeta: meta.ObjectMeta{
			Name:            namer.Name(namer.NameCHIService, m.cr),
			Namespace:       m.cr.GetNamespace(),
			Labels:          namer.Macro(m.cr).Map(m.tagger.Label(tags.LabelServiceCHI, m.cr)),
			Annotations:     namer.Macro(m.cr).Map(m.tagger.Annotate(annotator.AnnotateServiceCR, m.cr)),
			OwnerReferences: createOwnerReferences(m.cr),
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
			Selector: m.tagger.Selector(tags.SelectorCHIScopeReady),
			Type:     core.ServiceTypeClusterIP,
			// ExternalTrafficPolicy: core.ServiceExternalTrafficPolicyTypeLocal, // For core.ServiceTypeLoadBalancer only
		},
	}
	tags.MakeObjectVersion(svc.GetObjectMeta(), svc)
	return svc
}

// createServiceCluster creates new core.Service for specified Cluster
func (m *ServiceManagerClickHouse) createServiceCluster(cluster api.ICluster) *core.Service {
	serviceName := namer.Name(namer.NameClusterService, cluster)
	ownerReferences := createOwnerReferences(m.cr)

	if template, ok := cluster.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return createServiceFromTemplate(
			template,
			cluster.GetRuntime().GetAddress().GetNamespace(),
			serviceName,
			m.tagger.Label(tags.LabelServiceCluster, cluster),
			m.tagger.Annotate(annotator.AnnotateServiceCluster, cluster),
			m.tagger.Selector(tags.SelectorClusterScopeReady, cluster),
			ownerReferences,
			namer.Macro(cluster),
		)
	}
	// No template specified, no need to create service
	return nil
}

// createServiceShard creates new core.Service for specified Shard
func (m *ServiceManagerClickHouse) createServiceShard(shard api.IShard) *core.Service {
	if template, ok := shard.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return createServiceFromTemplate(
			template,
			shard.GetRuntime().GetAddress().GetNamespace(),
			namer.Name(namer.NameShardService, shard),
			m.tagger.Label(tags.LabelServiceShard, shard),
			m.tagger.Annotate(annotator.AnnotateServiceShard, shard),
			m.tagger.Selector(tags.SelectorShardScopeReady, shard),
			createOwnerReferences(m.cr),
			namer.Macro(shard),
		)
	}
	// No template specified, no need to create service
	return nil
}

// createServiceHost creates new core.Service for specified host
func (m *ServiceManagerClickHouse) createServiceHost(host *api.Host) *core.Service {
	if template, ok := host.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return createServiceFromTemplate(
			template,
			host.Runtime.Address.Namespace,
			namer.Name(namer.NameStatefulSetService, host),
			m.tagger.Label(tags.LabelServiceHost, host),
			m.tagger.Annotate(annotator.AnnotateServiceHost, host),
			m.tagger.Selector(tags.SelectorHostScope, host),
			createOwnerReferences(m.cr),
			namer.Macro(host),
		)
	}

	// Create default Service
	// We do not have .templates.ServiceTemplate specified or it is incorrect
	svc := &core.Service{
		ObjectMeta: meta.ObjectMeta{
			Name:            namer.Name(namer.NameStatefulSetService, host),
			Namespace:       host.Runtime.Address.Namespace,
			Labels:          namer.Macro(host).Map(m.tagger.Label(tags.LabelServiceHost, host)),
			Annotations:     namer.Macro(host).Map(m.tagger.Annotate(annotator.AnnotateServiceHost, host)),
			OwnerReferences: createOwnerReferences(m.cr),
		},
		Spec: core.ServiceSpec{
			Selector:                 m.tagger.Selector(tags.SelectorHostScope, host),
			ClusterIP:                model.TemplateDefaultsServiceClusterIP,
			Type:                     "ClusterIP",
			PublishNotReadyAddresses: true,
		},
	}
	svcAppendSpecifiedPorts(svc, host)
	tags.MakeObjectVersion(svc.GetObjectMeta(), svc)
	return svc
}
