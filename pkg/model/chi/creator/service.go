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
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/common/creator"
	"github.com/altinity/clickhouse-operator/pkg/model/common/namer/macro"
	commonLabeler "github.com/altinity/clickhouse-operator/pkg/model/common/tags/labeler"
)

const (
	// Default value for ClusterIP service
	TemplateDefaultsServiceClusterIP = "None"
)

type ServiceManager struct {
	cr     api.ICustomResource
	or     interfaces.IOwnerReferencesManager
	tagger interfaces.ITagger
}

func NewServiceManager() *ServiceManager {
	return &ServiceManager{
		or: NewOwnerReferencer(),
	}
}

func (m *ServiceManager) CreateService(what interfaces.ServiceType, params ...any) *core.Service {
	switch what {
	case interfaces.ServiceCR:
		return m.createServiceCR()
	case interfaces.ServiceCluster:
		var cluster api.ICluster
		if len(params) > 0 {
			cluster = params[0].(api.ICluster)
			return m.createServiceCluster(cluster)
		}
	case interfaces.ServiceShard:
		var shard api.IShard
		if len(params) > 0 {
			shard = params[0].(api.IShard)
			return m.createServiceShard(shard)
		}
	case interfaces.ServiceHost:
		var host *api.Host
		if len(params) > 0 {
			host = params[0].(*api.Host)
			return m.createServiceHost(host)
		}
	}
	panic("unknown service type")
}

func (m *ServiceManager) SetCR(cr api.ICustomResource) {
	m.cr = cr
}
func (m *ServiceManager) SetTagger(tagger interfaces.ITagger) {
	m.tagger = tagger
}

// createServiceCR creates new core.Service for specified CR
func (m *ServiceManager) createServiceCR() *core.Service {
	if template, ok := m.cr.GetRootServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return creator.CreateServiceFromTemplate(
			template,
			m.cr.GetNamespace(),
			namer.New().Name(interfaces.NameCRService, m.cr),
			m.tagger.Label(interfaces.LabelServiceCR, m.cr),
			m.tagger.Annotate(interfaces.AnnotateServiceCR, m.cr),
			m.tagger.Selector(interfaces.SelectorCRScopeReady),
			m.or.CreateOwnerReferences(m.cr),
			macro.Macro(m.cr),
		)
	}

	// Create default Service
	// We do not have .templates.ServiceTemplate specified or it is incorrect
	svc := &core.Service{
		ObjectMeta: meta.ObjectMeta{
			Name:            namer.New().Name(interfaces.NameCRService, m.cr),
			Namespace:       m.cr.GetNamespace(),
			Labels:          macro.Macro(m.cr).Map(m.tagger.Label(interfaces.LabelServiceCR, m.cr)),
			Annotations:     macro.Macro(m.cr).Map(m.tagger.Annotate(interfaces.AnnotateServiceCR, m.cr)),
			OwnerReferences: m.or.CreateOwnerReferences(m.cr),
		},
		Spec: core.ServiceSpec{
			ClusterIP: TemplateDefaultsServiceClusterIP,
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
			Selector: m.tagger.Selector(interfaces.SelectorCRScopeReady),
			Type:     core.ServiceTypeClusterIP,
			// ExternalTrafficPolicy: core.ServiceExternalTrafficPolicyTypeLocal, // For core.ServiceTypeLoadBalancer only
		},
	}
	commonLabeler.MakeObjectVersion(svc.GetObjectMeta(), svc)
	return svc
}

// createServiceCluster creates new core.Service for specified Cluster
func (m *ServiceManager) createServiceCluster(cluster api.ICluster) *core.Service {
	serviceName := namer.New().Name(interfaces.NameClusterService, cluster)
	ownerReferences := m.or.CreateOwnerReferences(m.cr)

	if template, ok := cluster.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return creator.CreateServiceFromTemplate(
			template,
			cluster.GetRuntime().GetAddress().GetNamespace(),
			serviceName,
			m.tagger.Label(interfaces.LabelServiceCluster, cluster),
			m.tagger.Annotate(interfaces.AnnotateServiceCluster, cluster),
			m.tagger.Selector(interfaces.SelectorClusterScopeReady, cluster),
			ownerReferences,
			macro.Macro(cluster),
		)
	}
	// No template specified, no need to create service
	return nil
}

// createServiceShard creates new core.Service for specified Shard
func (m *ServiceManager) createServiceShard(shard api.IShard) *core.Service {
	if template, ok := shard.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return creator.CreateServiceFromTemplate(
			template,
			shard.GetRuntime().GetAddress().GetNamespace(),
			namer.New().Name(interfaces.NameShardService, shard),
			m.tagger.Label(interfaces.LabelServiceShard, shard),
			m.tagger.Annotate(interfaces.AnnotateServiceShard, shard),
			m.tagger.Selector(interfaces.SelectorShardScopeReady, shard),
			m.or.CreateOwnerReferences(m.cr),
			macro.Macro(shard),
		)
	}
	// No template specified, no need to create service
	return nil
}

// createServiceHost creates new core.Service for specified host
func (m *ServiceManager) createServiceHost(host *api.Host) *core.Service {
	if template, ok := host.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return creator.CreateServiceFromTemplate(
			template,
			host.GetRuntime().GetAddress().GetNamespace(),
			namer.New().Name(interfaces.NameStatefulSetService, host),
			m.tagger.Label(interfaces.LabelServiceHost, host),
			m.tagger.Annotate(interfaces.AnnotateServiceHost, host),
			m.tagger.Selector(interfaces.SelectorHostScope, host),
			m.or.CreateOwnerReferences(m.cr),
			macro.Macro(host),
		)
	}

	// Create default Service
	// We do not have .templates.ServiceTemplate specified or it is incorrect
	svc := &core.Service{
		ObjectMeta: meta.ObjectMeta{
			Name:            namer.New().Name(interfaces.NameStatefulSetService, host),
			Namespace:       host.GetRuntime().GetAddress().GetNamespace(),
			Labels:          macro.Macro(host).Map(m.tagger.Label(interfaces.LabelServiceHost, host)),
			Annotations:     macro.Macro(host).Map(m.tagger.Annotate(interfaces.AnnotateServiceHost, host)),
			OwnerReferences: m.or.CreateOwnerReferences(m.cr),
		},
		Spec: core.ServiceSpec{
			Selector:                 m.tagger.Selector(interfaces.SelectorHostScope, host),
			ClusterIP:                TemplateDefaultsServiceClusterIP,
			Type:                     "ClusterIP",
			PublishNotReadyAddresses: true,
		},
	}
	creator.SvcAppendSpecifiedPorts(svc, host)
	commonLabeler.MakeObjectVersion(svc.GetObjectMeta(), svc)
	return svc
}
