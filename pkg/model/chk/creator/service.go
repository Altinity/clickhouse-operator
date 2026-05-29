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
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/macro"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/tags/labeler"
	"github.com/altinity/clickhouse-operator/pkg/model/common/creator"
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
		macro:   macro.New(),
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

// createServiceCR creates new core.Service for specified CR.
//
// NOTE: user-supplied ServiceTemplate (spec.templates.serviceTemplate*) bypasses
// the plaintext-port suppression that cluster.insecure=no normally enforces on
// the default Service. If a user pairs `cluster.insecure: no` with a template
// that still declares the legacy zk:2181 port, the Service will advertise that
// port even though the Keeper process binds no plaintext listener — clients
// will route to a dead endpoint. Users opting into TLS-only mode must keep
// their templates aligned with their cluster.insecure/secure settings.
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
			Ports:     buildZKServicePorts(crExposesInsecureZK(m.cr), crExposesSecureZK(m.cr)),
			Selector:  m.tagger.Selector(interfaces.SelectorCRScopeReady),
			Type:      core.ServiceTypeClusterIP,
			// ExternalTrafficPolicy: core.ServiceExternalTrafficPolicyTypeLocal, // For core.ServiceTypeLoadBalancer only
		},
	}
	m.labeler.MakeObjectVersion(svc.GetObjectMeta(), svc)

	return []*core.Service{svc}
}

// buildZKServicePorts is a pure helper that constructs the ZK + Raft port
// list for the CR-scope Service. When any host exposes the secure (TLS) ZK
// port, the zk-secure:2281 port is appended alongside the plaintext zk:2181.
// Raft is always present. Order is stable: insecure → secure → raft.
func buildZKServicePorts(insecure, secure bool) []core.ServicePort {
	ports := make([]core.ServicePort, 0, 3)
	if insecure {
		ports = append(ports, core.ServicePort{
			Name:       chi.KpDefaultZKPortName,
			Protocol:   core.ProtocolTCP,
			Port:       chi.KpDefaultZKPortNumber,
			TargetPort: intstr.FromString(chi.KpDefaultZKPortName),
		})
	}
	if secure {
		ports = append(ports, core.ServicePort{
			Name:       chi.KpDefaultZKSecurePortName,
			Protocol:   core.ProtocolTCP,
			Port:       chi.KpDefaultZKSecurePortNumber,
			TargetPort: intstr.FromString(chi.KpDefaultZKSecurePortName),
		})
	}
	ports = append(ports, core.ServicePort{
		Name:       chi.KpDefaultRaftPortName,
		Protocol:   core.ProtocolTCP,
		Port:       chi.KpDefaultRaftPortNumber,
		TargetPort: intstr.FromString(chi.KpDefaultRaftPortName),
	})
	return ports
}

// crExposesInsecureZK reports whether any host in the CR opts into publishing
// the plaintext Keeper client port (zk:2181) on Kubernetes Services and on
// StatefulSet container ports. A host opts in implicitly via the default
// (host.IsInsecure() returns true unless cluster.insecure or host.insecure is
// explicitly "no"). A CR with no hosts returns true so legacy/empty CRs are
// byte-identical at the Service layer.
//
// When the host opts out the K8s-level exposure is dropped AND the per-host
// XML overlay emits <tcp_port remove="1"/> so the Keeper process binds no
// plaintext listener at all (loopback included). The liveness probe falls
// back to pgrep because upstream 4LW does not work over the secure port.
func crExposesInsecureZK(cr chi.ICustomResource) bool {
	if cr == nil {
		return true
	}
	exposed := false
	hasHosts := false
	cr.WalkHosts(func(h *chi.Host) error {
		hasHosts = true
		if (h != nil) && h.IsInsecure() && h.ZKPort.HasValue() {
			exposed = true
		}
		return nil
	})
	if !hasHosts {
		return true
	}
	return exposed
}

// crExposesSecureZK reports whether any host in the CR has the secure (TLS)
// ZK port set. Returns false when no host is secure-configured — keeps the
// emitted Service byte-identical to legacy behavior for non-adopters.
// Asymmetric with crExposesInsecureZK: a CR with no hosts returns false
// here (secure is opt-in) but true there (insecure is the legacy default).
func crExposesSecureZK(cr chi.ICustomResource) bool {
	if cr == nil {
		return false
	}
	exposed := false
	cr.WalkHosts(func(h *chi.Host) error {
		if h != nil && h.ZKPortSecure.HasValue() {
			exposed = true
		}
		return nil
	})
	return exposed
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
	appendHostExposedPorts(svc, host)
	m.labeler.MakeObjectVersion(svc.GetObjectMeta(), svc)
	return svc
}

// appendHostExposedPorts walks the host's specified ports and emits them onto
// the per-host Service, with one CHK-local rule: the plaintext zk port is
// suppressed when the host opts out (`host.IsInsecure() == false`). The
// matching per-host XML overlay emits <tcp_port remove="1"/> so the Keeper
// process binds no plaintext listener at all; liveness probe falls back to
// pgrep. Other ports (zk-secure, raft) flow through unchanged.
func appendHostExposedPorts(svc *core.Service, host *chi.Host) {
	host.WalkSpecifiedPorts(func(name string, port *types.Int32, protocol core.Protocol) bool {
		if (name == chi.KpDefaultZKPortName) && !host.IsInsecure() {
			return false
		}
		svc.Spec.Ports = append(svc.Spec.Ports, core.ServicePort{
			Name:       name,
			Protocol:   protocol,
			Port:       port.Value(),
			TargetPort: intstr.FromInt(port.IntValue()),
		})
		return false
	})
}

// appendHostExposedContainerPorts mirrors appendHostExposedPorts but emits onto
// a Container's port list (StatefulSet pod template). Suppresses the plaintext
// zk containerPort when the host opts out of insecure exposure; the per-host
// XML overlay correspondingly emits <tcp_port remove="1"/> so the Keeper
// process binds no plaintext listener.
func appendHostExposedContainerPorts(container *core.Container, host *chi.Host) {
	host.WalkSpecifiedPorts(func(name string, port *types.Int32, protocol core.Protocol) bool {
		if (name == chi.KpDefaultZKPortName) && !host.IsInsecure() {
			return false
		}
		container.Ports = append(container.Ports, core.ContainerPort{
			Name:          name,
			ContainerPort: port.Value(),
			Protocol:      protocol,
		})
		return false
	})
}
