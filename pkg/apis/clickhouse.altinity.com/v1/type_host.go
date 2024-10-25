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

package v1

import (
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"

	"github.com/altinity/clickhouse-operator/pkg/apis/swversion"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Host defines host (a data replica within a shard) of .spec.configuration.clusters[n].shards[m]
type Host struct {
	Name         string `json:"name,omitempty" yaml:"name,omitempty"`
	HostSecure   `json:",inline" yaml:",inline"`
	HostPorts    `json:",inline" yaml:",inline"`
	HostSettings `json:",inline" yaml:",inline"`
	Templates    *TemplatesList `json:"templates,omitempty"           yaml:"templates,omitempty"`

	Runtime HostRuntime `json:"-" yaml:"-"`
}

type HostSecure struct {
	Insecure *types.StringBool `json:"insecure,omitempty"            yaml:"insecure,omitempty"`
	Secure   *types.StringBool `json:"secure,omitempty"              yaml:"secure,omitempty"`
}

type HostPorts struct {
	// DEPRECATED - to be removed soon
	Port *types.Int32 `json:"port,omitempty"  yaml:"port,omitempty"`

	TCPPort             *types.Int32 `json:"tcpPort,omitempty"             yaml:"tcpPort,omitempty"`
	TLSPort             *types.Int32 `json:"tlsPort,omitempty"             yaml:"tlsPort,omitempty"`
	HTTPPort            *types.Int32 `json:"httpPort,omitempty"            yaml:"httpPort,omitempty"`
	HTTPSPort           *types.Int32 `json:"httpsPort,omitempty"           yaml:"httpsPort,omitempty"`
	InterserverHTTPPort *types.Int32 `json:"interserverHTTPPort,omitempty" yaml:"interserverHTTPPort,omitempty"`
	ZKPort              *types.Int32 `json:"zkPort,omitempty"              yaml:"zkPort,omitempty"`
	RaftPort            *types.Int32 `json:"raftPort,omitempty"            yaml:"raftPort,omitempty"`
}

type HostSettings struct {
	Settings *Settings `json:"settings,omitempty"            yaml:"settings,omitempty"`
	Files    *Settings `json:"files,omitempty"               yaml:"files,omitempty"`
}

type HostRuntime struct {
	// Internal data
	Address             HostAddress                `json:"-" yaml:"-"`
	Version             *swversion.SoftWareVersion `json:"-" yaml:"-"`
	reconcileAttributes *HostReconcileAttributes   `json:"-" yaml:"-" testdiff:"ignore"`
	replicas            *types.Int32               `json:"-" yaml:"-"`
	hasData             bool                       `json:"-" yaml:"-"`

	// CurStatefulSet is a current stateful set, fetched from k8s
	CurStatefulSet *apps.StatefulSet `json:"-" yaml:"-" testdiff:"ignore"`
	// DesiredStatefulSet is a desired stateful set - reconcile target
	DesiredStatefulSet *apps.StatefulSet `json:"-" yaml:"-" testdiff:"ignore"`

	cr ICustomResource `json:"-" yaml:"-" testdiff:"ignore"`
}

func (r *HostRuntime) GetAddress() IHostAddress {
	return &r.Address
}

func (r *HostRuntime) SetCR(cr ICustomResource) {
	r.cr = cr
}

func (r *HostRuntime) GetCR() ICustomResource {
	return r.cr.(ICustomResource)
}

func (host *Host) GetRuntime() IHostRuntime {
	return &host.Runtime
}

// GetReconcileAttributes is an ensurer getter
func (host *Host) GetReconcileAttributes() *HostReconcileAttributes {
	if host == nil {
		return nil
	}
	if host.Runtime.reconcileAttributes == nil {
		host.Runtime.reconcileAttributes = NewHostReconcileAttributes()
	}
	return host.Runtime.reconcileAttributes
}

// InheritSettingsFrom inherits settings from specified shard and replica
func (host *Host) InheritSettingsFrom(shard IShard, replica IReplica) {
	if (shard != nil) && shard.HasSettings() {
		host.Settings = host.Settings.MergeFrom(shard.GetSettings())
	}

	if (replica != nil) && replica.HasSettings() {
		host.Settings = host.Settings.MergeFrom(replica.GetSettings())
	}
}

// InheritFilesFrom inherits files from specified shard and replica
func (host *Host) InheritFilesFrom(shard IShard, replica IReplica) {
	if (shard != nil) && shard.HasFiles() {
		host.Files = host.Files.MergeFrom(shard.GetFiles())
	}

	if (replica != nil) && replica.HasFiles() {
		host.Files = host.Files.MergeFrom(replica.GetFiles())
	}
}

// InheritTemplatesFrom inherits templates from specified shard, replica or template
func (host *Host) InheritTemplatesFrom(sources ...any) {
	for _, source := range sources {
		switch typed := source.(type) {
		case IShard:
			shard := typed
			if shard.HasTemplates() {
				host.Templates = host.Templates.MergeFrom(shard.GetTemplates(), MergeTypeFillEmptyValues)
			}
		case IReplica:
			replica := typed
			if replica.HasTemplates() {
				host.Templates = host.Templates.MergeFrom(replica.GetTemplates(), MergeTypeFillEmptyValues)
			}
		case *HostTemplate:
			template := typed
			if template != nil {
				host.Templates = host.Templates.MergeFrom(template.Spec.Templates, MergeTypeFillEmptyValues)
			}
		}
	}

	host.Templates.HandleDeprecatedFields()
}

// MergeFrom merges from specified host
func (host *Host) MergeFrom(from *Host) {
	if (host == nil) || (from == nil) {
		return
	}

	host.Insecure = host.Insecure.MergeFrom(from.Insecure)
	host.Secure = host.Secure.MergeFrom(from.Secure)

	if !host.TCPPort.HasValue() {
		host.TCPPort.MergeFrom(from.TCPPort)
	}
	if !host.TLSPort.HasValue() {
		host.TLSPort.MergeFrom(from.TLSPort)
	}
	if !host.HTTPPort.HasValue() {
		host.HTTPPort.MergeFrom(from.HTTPPort)
	}
	if !host.HTTPSPort.HasValue() {
		host.HTTPSPort.MergeFrom(from.HTTPSPort)
	}
	if !host.InterserverHTTPPort.HasValue() {
		host.InterserverHTTPPort.MergeFrom(from.InterserverHTTPPort)
	}
	if !host.ZKPort.HasValue() {
		host.ZKPort.MergeFrom(from.ZKPort)
	}
	if !host.RaftPort.HasValue() {
		host.RaftPort.MergeFrom(from.RaftPort)
	}

	host.Templates = host.Templates.MergeFrom(from.Templates, MergeTypeFillEmptyValues)
	host.Templates.HandleDeprecatedFields()
}

// GetHostTemplate gets host template
func (host *Host) GetHostTemplate() (*HostTemplate, bool) {
	if !host.Templates.HasHostTemplate() {
		return nil, false
	}
	name := host.Templates.GetHostTemplate()
	return host.GetCR().GetHostTemplate(name)
}

// GetPodTemplate gets pod template
func (host *Host) GetPodTemplate() (*PodTemplate, bool) {
	if !host.Templates.HasPodTemplate() {
		return nil, false
	}
	name := host.Templates.GetPodTemplate()
	return host.GetCR().GetPodTemplate(name)
}

// GetServiceTemplate gets service template
func (host *Host) GetServiceTemplate() (*ServiceTemplate, bool) {
	if !host.Templates.HasReplicaServiceTemplate() {
		return nil, false
	}
	name := host.Templates.GetReplicaServiceTemplate()
	return host.GetCR().GetServiceTemplate(name)
}

// GetStatefulSetReplicasNum gets stateful set replica num
func (host *Host) GetStatefulSetReplicasNum(shutdown bool) *int32 {
	var num int32 = 0
	switch {
	case shutdown:
		num = 0
	case host.IsStopped():
		num = 0
	case host.Runtime.replicas.HasValue():
		num = host.Runtime.replicas.Value()
	default:
		num = 1
	}
	return &num
}

// GetSettings gets settings
func (host *Host) GetSettings() *Settings {
	return host.Settings
}

// GetZookeeper gets zookeeper
func (host *Host) GetZookeeper() *ZookeeperConfig {
	cluster := host.GetCluster()
	return cluster.GetZookeeper()
}

// GetName gets name
func (host *Host) GetName() string {
	if host == nil {
		return "host-is-nil"
	}
	return host.Name
}

// GetCR gets CHI
func (host *Host) GetCR() ICustomResource {
	return host.GetRuntime().GetCR()
}

// HasCR checks whether host has CHI
func (host *Host) HasCR() bool {
	return host.GetCR() != nil
}

func (host *Host) SetCR(chi ICustomResource) {
	host.GetRuntime().SetCR(chi)
}

// GetCluster gets cluster
func (host *Host) GetCluster() ICluster {
	// Host has to have filled Address
	return host.GetCR().FindCluster(host.Runtime.Address.ClusterName)
}

// GetShard gets shard
func (host *Host) GetShard() IShard {
	// Host has to have filled Address
	return host.GetCR().FindShard(host.Runtime.Address.ClusterName, host.Runtime.Address.ShardName)
}

// GetAncestor gets ancestor of a host
func (host *Host) GetAncestor() *Host {
	if !host.HasAncestorCR() {
		return nil
	}
	return host.GetAncestorCR().FindHost(
		host.Runtime.Address.ClusterName,
		host.Runtime.Address.ShardName,
		host.Runtime.Address.HostName,
	)
}

// HasAncestor checks whether host has an ancestor
func (host *Host) HasAncestor() bool {
	return host.GetAncestor() != nil
}

// GetAncestorCR gets ancestor of a host
func (host *Host) GetAncestorCR() ICustomResource {
	return host.GetCR().GetAncestor()
}

// HasAncestorCR checks whether host has an ancestor
func (host *Host) HasAncestorCR() bool {
	return host.GetAncestorCR().IsNonZero()
}

// WalkVolumeClaimTemplates walks VolumeClaimTemplate(s)
func (host *Host) WalkVolumeClaimTemplates(f func(template *VolumeClaimTemplate)) {
	host.GetCR().WalkVolumeClaimTemplates(f)
}

// IsStopped checks whether host is stopped
func (host *Host) IsStopped() bool {
	return host.GetCR().IsStopped()
}

// IsInNewCluster checks whether host is in a new cluster
// TODO unify with model HostIsNewOne
func (host *Host) IsInNewCluster() bool {
	return !host.HasAncestor() && (host.GetCR().IEnsureStatus().GetHostsCount() == host.GetCR().IEnsureStatus().GetHostsAddedCount())
}

// WhichStatefulSet specifies which StatefulSet we are going to process in host functions
type WhichStatefulSet string

const (
	// CurStatefulSet specifies current StatefulSet to be processed
	CurStatefulSet WhichStatefulSet = "cur"
	// DesiredStatefulSet specifies desired StatefulSet to be processed
	DesiredStatefulSet WhichStatefulSet = "desired"
)

// CurStatefulSet checks whether WhichStatefulSet is a current one
func (w WhichStatefulSet) CurStatefulSet() bool {
	return w == CurStatefulSet
}

// DesiredStatefulSet checks whether WhichStatefulSet is a desired one
func (w WhichStatefulSet) DesiredStatefulSet() bool {
	return w == DesiredStatefulSet
}

// WalkVolumeMounts walks VolumeMount(s)
func (host *Host) WalkVolumeMounts(which WhichStatefulSet, f func(volumeMount *core.VolumeMount)) {
	if host == nil {
		return
	}

	var sts *apps.StatefulSet
	switch {
	case which.DesiredStatefulSet():
		if !host.HasDesiredStatefulSet() {
			return
		}
		sts = host.Runtime.DesiredStatefulSet
	case which.CurStatefulSet():
		if !host.HasCurStatefulSet() {
			return
		}
		sts = host.Runtime.CurStatefulSet
	default:
		return
	}

	// TODO ensure sts.Spec.Template.Spec.Containers

	for i := range sts.Spec.Template.Spec.Containers {
		container := &sts.Spec.Template.Spec.Containers[i]
		for j := range container.VolumeMounts {
			volumeMount := &container.VolumeMounts[j]
			f(volumeMount)
		}
	}
}

// GetVolumeMount gets VolumeMount by the name
//func (host *Host) GetVolumeMount(volumeMountName string) (vm *corev1.VolumeMount, ok bool) {
//	host.WalkVolumeMounts(func(volumeMount *corev1.VolumeMount) {
//		if volumeMount.Name == volumeMountName {
//			vm = volumeMount
//			ok = true
//		}
//	})
//	return
//}

// IsSecure checks whether the host requires secure communication
func (host *Host) IsSecure() bool {
	if host == nil {
		return false
	}

	// Personal host settings take priority
	if host.Secure.HasValue() {
		return host.Secure.Value()
	}

	// No personal value - fallback to cluster value
	if host.GetCluster().GetSecure().HasValue() {
		return host.GetCluster().GetSecure().Value()
	}

	// No cluster value - host should not expose secure
	return false
}

// IsInsecure checks whether the host requires insecure communication
func (host *Host) IsInsecure() bool {
	if host == nil {
		return false
	}

	// Personal host settings take priority
	if host.Insecure.HasValue() {
		return host.Insecure.Value()
	}

	// No personal value - fallback to cluster value
	if host.GetCluster().GetInsecure().HasValue() {
		return host.GetCluster().GetInsecure().Value()
	}

	// No cluster value - host should expose insecure
	return true
}

// IsFirst checks whether the host is the first host of the whole CHI
func (host *Host) IsFirst() bool {
	if host == nil {
		return false
	}

	return host.Runtime.Address.CHIScopeIndex == 0
}

// IsFirst checks whether the host is the last host of the whole CHI
func (host *Host) IsLast() bool {
	if host == nil {
		return false
	}

	return host.Runtime.Address.CHIScopeIndex == (host.GetCR().HostsCount() - 1)
}

// HasCurStatefulSet checks whether host has CurStatefulSet
func (host *Host) HasCurStatefulSet() bool {
	if host == nil {
		return false
	}

	return host.Runtime.CurStatefulSet != nil
}

// HasDesiredStatefulSet checks whether host has DesiredStatefulSet
func (host *Host) HasDesiredStatefulSet() bool {
	if host == nil {
		return false
	}

	return host.Runtime.DesiredStatefulSet != nil
}

const (
	ChDefaultPortName   = "port"
	ChDefaultPortNumber = int32(9000)

	// ClickHouse open ports names and values
	ChDefaultTCPPortName               = "tcp"
	ChDefaultTCPPortNumber             = int32(9000)
	ChDefaultTLSPortName               = "secureclient"
	ChDefaultTLSPortNumber             = int32(9440)
	ChDefaultHTTPPortName              = "http"
	ChDefaultHTTPPortNumber            = int32(8123)
	ChDefaultHTTPSPortName             = "https"
	ChDefaultHTTPSPortNumber           = int32(8443)
	ChDefaultInterserverHTTPPortName   = "interserver"
	ChDefaultInterserverHTTPPortNumber = int32(9009)

	// Keeper open ports names and values
	KpDefaultZKPortName     = "zk"
	KpDefaultZKPortNumber   = int32(2181)
	KpDefaultRaftPortName   = "raft"
	KpDefaultRaftPortNumber = int32(9444)
)

func (host *Host) WalkPorts(f func(name string, port *types.Int32, protocol core.Protocol) bool) {
	if host == nil {
		return
	}
	if f(ChDefaultPortName, host.Port, core.ProtocolTCP) {
		return
	}
	if f(ChDefaultTCPPortName, host.TCPPort, core.ProtocolTCP) {
		return
	}
	if f(ChDefaultTLSPortName, host.TLSPort, core.ProtocolTCP) {
		return
	}
	if f(ChDefaultHTTPPortName, host.HTTPPort, core.ProtocolTCP) {
		return
	}
	if f(ChDefaultHTTPSPortName, host.HTTPSPort, core.ProtocolTCP) {
		return
	}
	if f(ChDefaultInterserverHTTPPortName, host.InterserverHTTPPort, core.ProtocolTCP) {
		return
	}
	if f(KpDefaultZKPortName, host.ZKPort, core.ProtocolTCP) {
		return
	}
	if f(KpDefaultRaftPortName, host.RaftPort, core.ProtocolTCP) {
		return
	}
}

func (host *Host) WalkSpecifiedPorts(f func(name string, port *types.Int32, protocol core.Protocol) bool) {
	host.WalkPorts(
		func(_name string, _port *types.Int32, _protocol core.Protocol) bool {
			if _port.HasValue() {
				// Port is explicitly specified - call provided function on it
				return f(_name, _port, _protocol)
			}
			// Do not break, continue iterating
			return false
		},
	)
}

func (host *Host) AppendSpecifiedPortsToContainer(container *core.Container) {
	// Walk over all assigned ports of the host and append each port to the list of container's ports
	host.WalkSpecifiedPorts(
		func(name string, port *types.Int32, protocol core.Protocol) bool {
			// Append assigned port to the list of container's ports
			container.Ports = append(container.Ports,
				core.ContainerPort{
					Name:          name,
					ContainerPort: port.Value(),
					Protocol:      protocol,
				},
			)
			// Do not abort, continue iterating
			return false
		},
	)
}

func (host *Host) HasListedTablesCreated(name string) bool {
	return util.InArray(
		name,
		host.GetCR().IEnsureStatus().GetHostsWithTablesCreated(),
	)
}

func (host *Host) HasData() bool {
	if host == nil {
		return false
	}
	return host.Runtime.hasData
}

func (host *Host) SetHasData(hasData bool) {
	if host == nil {
		return
	}
	host.Runtime.hasData = hasData
}

func (host *Host) IsZero() bool {
	return host == nil
}

func (host *Host) IsNonZero() bool {
	return host != nil
}
