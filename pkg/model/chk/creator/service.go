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
	"github.com/altinity/clickhouse-operator/pkg/util"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/macro"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/tags/labeler"
	"github.com/altinity/clickhouse-operator/pkg/model/common/creator"
	commonMacro "github.com/altinity/clickhouse-operator/pkg/model/common/macro"
)

const (
	// Default value for ClusterIP service
	TemplateDefaultsServiceClusterIP = "None"
)

type ServiceManager struct {
	cr      chi.ICustomResource
	or      interfaces.IOwnerReferencesManager
	tagger  interfaces.ITagger
	macro   interfaces.IMacro
	namer   interfaces.INameManager
	labeler interfaces.ILabeler
}

func NewServiceManager() *ServiceManager {
	return &ServiceManager{
		or:      NewOwnerReferencer(),
		macro:   commonMacro.New(macro.List),
		namer:   namer.New(),
		labeler: nil,
	}
}

func (m *ServiceManager) CreateService(what interfaces.ServiceType, params ...any) util.Slice[*core.Service] {
	switch what {
	case interfaces.ServiceCR:
		return m.createServiceCR()
	case interfaces.ServiceCluster:
		var cluster chi.ICluster
		if len(params) > 0 {
			cluster = params[0].(chi.ICluster)
			return []*core.Service{m.createServiceCluster(cluster)}
		}
	case interfaces.ServiceShard:
		var shard chi.IShard
		if len(params) > 0 {
			shard = params[0].(chi.IShard)
			return []*core.Service{m.createServiceShard(shard)}
		}
	case interfaces.ServiceHost:
		var host *chi.Host
		if len(params) > 0 {
			host = params[0].(*chi.Host)
			return []*core.Service{m.createServiceHost(host)}
		}
	}
	panic("unknown service type")
}

func (m *ServiceManager) SetCR(cr chi.ICustomResource) {
	m.cr = cr
	m.labeler = labeler.New(cr)
}
func (m *ServiceManager) SetTagger(tagger interfaces.ITagger) {
	m.tagger = tagger
}

// createServiceCR creates new core.Service for specified CR
func (m *ServiceManager) createServiceCR() []*core.Service {
	if m.cr.IsZero() {
		return nil
	}
	if templates, ok := m.cr.GetRootServiceTemplates(); ok {
		var services []*core.Service
		for _, template := range templates {
			services = append(services,
				creator.CreateServiceFromTemplate(
					template,
					m.cr.GetNamespace(),
					m.namer.Name(interfaces.NameCRService, m.cr, template),
					m.tagger.Label(interfaces.LabelServiceCR, m.cr),
					m.tagger.Annotate(interfaces.AnnotateServiceCR, m.cr),
					m.tagger.Selector(interfaces.SelectorCRScopeReady),
					m.or.CreateOwnerReferences(m.cr),
					m.macro.Scope(m.cr),
					m.labeler,
				),
			)
		}
		return services
	}

	// Create default Service
	// We do not have .templates.ServiceTemplate specified or it is incorrect
	svc := &core.Service{
		ObjectMeta: meta.ObjectMeta{
			Name:            m.namer.Name(interfaces.NameCRService, m.cr),
			Namespace:       m.cr.GetNamespace(),
			Labels:          m.macro.Scope(m.cr).Map(m.tagger.Label(interfaces.LabelServiceCR, m.cr)),
			Annotations:     m.macro.Scope(m.cr).Map(m.tagger.Annotate(interfaces.AnnotateServiceCR, m.cr)),
			OwnerReferences: m.or.CreateOwnerReferences(m.cr),
		},
		Spec: core.ServiceSpec{
			ClusterIP: TemplateDefaultsServiceClusterIP,
			Ports: []core.ServicePort{
				{
					Name:       chi.KpDefaultZKPortName,
					Protocol:   core.ProtocolTCP,
					Port:       chi.KpDefaultZKPortNumber,
					TargetPort: intstr.FromString(chi.KpDefaultZKPortName),
				},
				{
					Name:       chi.KpDefaultRaftPortName,
					Protocol:   core.ProtocolTCP,
					Port:       chi.KpDefaultRaftPortNumber,
					TargetPort: intstr.FromString(chi.KpDefaultRaftPortName),
				},
			},
			Selector: m.tagger.Selector(interfaces.SelectorCRScopeReady),
			Type:     core.ServiceTypeClusterIP,
			// ExternalTrafficPolicy: core.ServiceExternalTrafficPolicyTypeLocal, // For core.ServiceTypeLoadBalancer only
		},
	}
	m.labeler.MakeObjectVersion(svc.GetObjectMeta(), svc)

	return []*core.Service{svc}
}

// createServiceCluster creates new core.Service for specified Cluster
func (m *ServiceManager) createServiceCluster(cluster chi.ICluster) *core.Service {
	if cluster.IsZero() {
		return nil
	}

	serviceName := m.namer.Name(interfaces.NameClusterService, cluster)
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
			m.macro.Scope(cluster),
			m.labeler,
		)
	}
	// No template specified, no need to create service
	return nil
}

// createServiceShard creates new core.Service for specified Shard
func (m *ServiceManager) createServiceShard(shard chi.IShard) *core.Service {
	if shard.IsZero() {
		return nil
	}
	if template, ok := shard.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return creator.CreateServiceFromTemplate(
			template,
			shard.GetRuntime().GetAddress().GetNamespace(),
			m.namer.Name(interfaces.NameShardService, shard),
			m.tagger.Label(interfaces.LabelServiceShard, shard),
			m.tagger.Annotate(interfaces.AnnotateServiceShard, shard),
			m.tagger.Selector(interfaces.SelectorShardScopeReady, shard),
			m.or.CreateOwnerReferences(m.cr),
			m.macro.Scope(shard),
			m.labeler,
		)
	}
	// No template specified, no need to create service
	return nil
}

// createServiceHost creates new core.Service for specified host
func (m *ServiceManager) createServiceHost(host *chi.Host) *core.Service {
	if host.IsZero() {
		return nil
	}
	if template, ok := host.GetServiceTemplate(); ok {
		// .templates.ServiceTemplate specified
		return creator.CreateServiceFromTemplate(
			template,
			host.GetRuntime().GetAddress().GetNamespace(),
			m.namer.Name(interfaces.NameStatefulSetService, host),
			m.tagger.Label(interfaces.LabelServiceHost, host),
			m.tagger.Annotate(interfaces.AnnotateServiceHost, host),
			m.tagger.Selector(interfaces.SelectorHostScope, host),
			m.or.CreateOwnerReferences(m.cr),
			m.macro.Scope(host),
			m.labeler,
		)
	}

	// Create default Service
	// We do not have .templates.ServiceTemplate specified or it is incorrect
	svc := &core.Service{
		ObjectMeta: meta.ObjectMeta{
			Name:            m.namer.Name(interfaces.NameStatefulSetService, host),
			Namespace:       host.GetRuntime().GetAddress().GetNamespace(),
			Labels:          m.macro.Scope(host).Map(m.tagger.Label(interfaces.LabelServiceHost, host)),
			Annotations:     m.macro.Scope(host).Map(m.tagger.Annotate(interfaces.AnnotateServiceHost, host)),
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
	m.labeler.MakeObjectVersion(svc.GetObjectMeta(), svc)
	return svc
}
