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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

const (
	// PortMayBeAssignedLaterOrLeftUnused value means that port
	// is not assigned yet and is expected to be assigned later.
	PortMayBeAssignedLaterOrLeftUnused = int32(0)
)

// ChiHost defines host (a data replica within a shard) of .spec.configuration.clusters[n].shards[m]
type ChiHost struct {
	Name string `json:"name,omitempty" yaml:"name,omitempty"`
	// DEPRECATED - to be removed soon
	Port int32 `json:"port,omitempty"                yaml:"port,omitempty"`

	Insecure            *StringBool       `json:"insecure,omitempty"            yaml:"insecure,omitempty"`
	Secure              *StringBool       `json:"secure,omitempty"              yaml:"secure,omitempty"`
	TCPPort             int32             `json:"tcpPort,omitempty"             yaml:"tcpPort,omitempty"`
	TLSPort             int32             `json:"tlsPort,omitempty"             yaml:"tlsPort,omitempty"`
	HTTPPort            int32             `json:"httpPort,omitempty"            yaml:"httpPort,omitempty"`
	HTTPSPort           int32             `json:"httpsPort,omitempty"           yaml:"httpsPort,omitempty"`
	InterserverHTTPPort int32             `json:"interserverHTTPPort,omitempty" yaml:"interserverHTTPPort,omitempty"`
	Settings            *Settings         `json:"settings,omitempty"            yaml:"settings,omitempty"`
	Files               *Settings         `json:"files,omitempty"               yaml:"files,omitempty"`
	Templates           *ChiTemplateNames `json:"templates,omitempty"           yaml:"templates,omitempty"`

	// Internal data
	Address             ChiHostAddress              `json:"-" yaml:"-"`
	Config              ChiHostConfig               `json:"-" yaml:"-"`
	reconcileAttributes *ChiHostReconcileAttributes `json:"-" yaml:"-" testdiff:"ignore"`
	// StatefulSet is a stateful set which is being worked with by the host.
	// It can be desired stateful set when host is being created or current stateful set.
	// Ex.: polling sts after creation.
	StatefulSet *appsv1.StatefulSet `json:"-" yaml:"-" testdiff:"ignore"`
	// CurStatefulSet is a current stateful set, fetched from k8s
	CurStatefulSet *appsv1.StatefulSet `json:"-" yaml:"-" testdiff:"ignore"`
	// DesiredStatefulSet is a desired stateful set - reconcile target
	DesiredStatefulSet *appsv1.StatefulSet     `json:"-" yaml:"-" testdiff:"ignore"`
	CHI                *ClickHouseInstallation `json:"-" yaml:"-" testdiff:"ignore"`
}

// GetReconcileAttributes is an ensurer getter
func (host *ChiHost) GetReconcileAttributes() *ChiHostReconcileAttributes {
	if host == nil {
		return nil
	}
	if host.reconcileAttributes == nil {
		host.reconcileAttributes = NewChiHostReconcileAttributes()
	}
	return host.reconcileAttributes
}

// InheritSettingsFrom inherits settings from specified shard and replica
func (host *ChiHost) InheritSettingsFrom(shard *ChiShard, replica *ChiReplica) {
	if shard != nil {
		host.Settings = host.Settings.MergeFrom(shard.Settings)
	}

	if replica != nil {
		host.Settings = host.Settings.MergeFrom(replica.Settings)
	}
}

// InheritFilesFrom inherits files from specified shard and replica
func (host *ChiHost) InheritFilesFrom(shard *ChiShard, replica *ChiReplica) {
	if shard != nil {
		host.Files = host.Files.MergeFrom(shard.Files)
	}

	if replica != nil {
		host.Files = host.Files.MergeFrom(replica.Files)
	}
}

// InheritTemplatesFrom inherits templates from specified shard and replica
func (host *ChiHost) InheritTemplatesFrom(shard *ChiShard, replica *ChiReplica, template *ChiHostTemplate) {
	if shard != nil {
		host.Templates = host.Templates.MergeFrom(shard.Templates, MergeTypeFillEmptyValues)
	}

	if replica != nil {
		host.Templates = host.Templates.MergeFrom(replica.Templates, MergeTypeFillEmptyValues)
	}

	if template != nil {
		host.Templates = host.Templates.MergeFrom(template.Spec.Templates, MergeTypeFillEmptyValues)
	}

	host.Templates.HandleDeprecatedFields()
}

func isUnassigned(port int32) bool {
	return port == PortMayBeAssignedLaterOrLeftUnused
}

// MergeFrom merges from specified host
func (host *ChiHost) MergeFrom(from *ChiHost) {
	if (host == nil) || (from == nil) {
		return
	}
	if isUnassigned(host.Port) {
		host.Port = from.Port
	}

	host.Insecure = host.Insecure.MergeFrom(from.Insecure)
	host.Secure = host.Secure.MergeFrom(from.Secure)
	if isUnassigned(host.TCPPort) {
		host.TCPPort = from.TCPPort
	}
	if isUnassigned(host.TLSPort) {
		host.TLSPort = from.TLSPort
	}
	if isUnassigned(host.HTTPPort) {
		host.HTTPPort = from.HTTPPort
	}
	if isUnassigned(host.HTTPSPort) {
		host.HTTPSPort = from.HTTPSPort
	}
	if isUnassigned(host.InterserverHTTPPort) {
		host.InterserverHTTPPort = from.InterserverHTTPPort
	}
	host.Templates = host.Templates.MergeFrom(from.Templates, MergeTypeFillEmptyValues)
	host.Templates.HandleDeprecatedFields()
}

// GetHostTemplate gets host template
func (host *ChiHost) GetHostTemplate() (*ChiHostTemplate, bool) {
	if !host.Templates.HasHostTemplate() {
		return nil, false
	}
	name := host.Templates.GetHostTemplate()
	return host.CHI.GetHostTemplate(name)
}

// GetPodTemplate gets pod template
func (host *ChiHost) GetPodTemplate() (*ChiPodTemplate, bool) {
	if !host.Templates.HasPodTemplate() {
		return nil, false
	}
	name := host.Templates.GetPodTemplate()
	return host.CHI.GetPodTemplate(name)
}

// GetServiceTemplate gets service template
func (host *ChiHost) GetServiceTemplate() (*ChiServiceTemplate, bool) {
	if !host.Templates.HasReplicaServiceTemplate() {
		return nil, false
	}
	name := host.Templates.GetReplicaServiceTemplate()
	return host.CHI.GetServiceTemplate(name)
}

// GetStatefulSetReplicasNum gets stateful set replica num
func (host *ChiHost) GetStatefulSetReplicasNum(shutdown bool) *int32 {
	var num int32 = 0
	switch {
	case shutdown:
		num = 0
	case host.CHI.IsStopped():
		num = 0
	default:
		num = 1
	}
	return &num
}

// GetSettings gets settings
func (host *ChiHost) GetSettings() *Settings {
	return host.Settings
}

// GetZookeeper gets zookeeper
func (host *ChiHost) GetZookeeper() *ChiZookeeperConfig {
	cluster := host.GetCluster()
	return cluster.Zookeeper
}

// GetName gets name
func (host *ChiHost) GetName() string {
	if host == nil {
		return "host-is-nil"
	}
	return host.Name
}

// GetCHI gets CHI
func (host *ChiHost) GetCHI() *ClickHouseInstallation {
	if host == nil {
		return nil
	}
	return host.CHI
}

// HasCHI checks whether host has CHI
func (host *ChiHost) HasCHI() bool {
	return host.GetCHI() != nil
}

// GetCluster gets cluster
func (host *ChiHost) GetCluster() *Cluster {
	// Host has to have filled Address
	return host.GetCHI().FindCluster(host.Address.ClusterName)
}

// GetShard gets shard
func (host *ChiHost) GetShard() *ChiShard {
	// Host has to have filled Address
	return host.GetCHI().FindShard(host.Address.ClusterName, host.Address.ShardName)
}

// GetAncestor gets ancestor of a host
func (host *ChiHost) GetAncestor() *ChiHost {
	return host.GetCHI().GetAncestor().FindHost(host.Address.ClusterName, host.Address.ShardName, host.Address.HostName)
}

// HasAncestor checks whether host has an ancestor
func (host *ChiHost) HasAncestor() bool {
	return host.GetAncestor() != nil
}

// GetAncestorCHI gets ancestor of a host
func (host *ChiHost) GetAncestorCHI() *ClickHouseInstallation {
	return host.GetCHI().GetAncestor()
}

// HasAncestorCHI checks whether host has an ancestor
func (host *ChiHost) HasAncestorCHI() bool {
	return host.GetAncestorCHI() != nil
}

// WalkVolumeClaimTemplates walks VolumeClaimTemplate(s)
func (host *ChiHost) WalkVolumeClaimTemplates(f func(template *ChiVolumeClaimTemplate)) {
	host.GetCHI().WalkVolumeClaimTemplates(f)
}

// WalkVolumeMounts walks VolumeMount(s)
func (host *ChiHost) WalkVolumeMounts(f func(volumeMount *corev1.VolumeMount)) {
	if host == nil {
		return
	}
	if host.StatefulSet == nil {
		return
	}

	for i := range host.StatefulSet.Spec.Template.Spec.Containers {
		container := &host.StatefulSet.Spec.Template.Spec.Containers[i]
		for j := range container.VolumeMounts {
			volumeMount := &container.VolumeMounts[j]
			f(volumeMount)
		}
	}
}

// GetVolumeMount gets VolumeMount by the name
func (host *ChiHost) GetVolumeMount(volumeMountName string) (vm *corev1.VolumeMount, ok bool) {
	host.WalkVolumeMounts(func(volumeMount *corev1.VolumeMount) {
		if volumeMount.Name == volumeMountName {
			vm = volumeMount
			ok = true
		}
	})
	return
}

// IsSecure checks whether the host requires secure communication
func (host *ChiHost) IsSecure() bool {
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
func (host *ChiHost) IsInsecure() bool {
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
func (host *ChiHost) IsFirst() bool {
	if host == nil {
		return false
	}

	return host.Address.CHIScopeIndex == 0
}
