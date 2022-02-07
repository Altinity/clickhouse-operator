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

// ChiHost defines host (a data replica within a shard) of .spec.configuration.clusters[n].shards[m]
type ChiHost struct {
	Name string `json:"name,omitempty" yaml:"name,omitempty"`
	// DEPRECATED - to be removed soon
	Port                int32             `json:"port,omitempty"                yaml:"port,omitempty"`
	TCPPort             int32             `json:"tcpPort,omitempty"             yaml:"tcpPort,omitempty"`
	HTTPPort            int32             `json:"httpPort,omitempty"            yaml:"httpPort,omitempty"`
	InterserverHTTPPort int32             `json:"interserverHTTPPort,omitempty" yaml:"interserverHTTPPort,omitempty"`
	Settings            *Settings         `json:"settings,omitempty"            yaml:"settings,omitempty"`
	Files               *Settings         `json:"files,omitempty"               yaml:"files,omitempty"`
	Templates           *ChiTemplateNames `json:"templates,omitempty"           yaml:"templates,omitempty"`

	// Internal data
	Address             ChiHostAddress             `json:"-" yaml:"-"`
	Config              ChiHostConfig              `json:"-" yaml:"-"`
	ReconcileAttributes ChiHostReconcileAttributes `json:"-" yaml:"-" testdiff:"ignore"`
	StatefulSet         *appsv1.StatefulSet        `json:"-" yaml:"-" testdiff:"ignore"`
	CurStatefulSet      *appsv1.StatefulSet        `json:"-" yaml:"-" testdiff:"ignore"`
	DesiredStatefulSet  *appsv1.StatefulSet        `json:"-" yaml:"-" testdiff:"ignore"`
	CHI                 *ClickHouseInstallation    `json:"-" yaml:"-" testdiff:"ignore"`
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

// MergeFrom merges from specified host
func (host *ChiHost) MergeFrom(from *ChiHost) {
	if (host == nil) || (from == nil) {
		return
	}

	if host.Port == 0 {
		host.Port = from.Port
	}
	if host.TCPPort == 0 {
		host.TCPPort = from.TCPPort
	}
	if host.HTTPPort == 0 {
		host.HTTPPort = from.HTTPPort
	}
	if host.InterserverHTTPPort == 0 {
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

// GetCluster gets cluster
func (host *ChiHost) GetCluster() *ChiCluster {
	// Host has to have filled Address
	return host.GetCHI().FindCluster(host.Address.ClusterName)
}

// GetShard gets shard
func (host *ChiHost) GetShard() *ChiShard {
	// Host has to have filled Address
	return host.GetCHI().FindShard(host.Address.ClusterName, host.Address.ShardName)
}

// CanDeleteAllPVCs checks whether all PVCs can be deleted
func (host *ChiHost) CanDeleteAllPVCs() bool {
	canDeleteAllPVCs := true
	host.CHI.WalkVolumeClaimTemplates(func(template *ChiVolumeClaimTemplate) {
		if template.PVCReclaimPolicy == PVCReclaimPolicyRetain {
			// At least one template wants to keep its PVC
			canDeleteAllPVCs = false
		}
	})

	return canDeleteAllPVCs
}

// WalkVolumeClaimTemplates walks VolumeClaimTemplate(s)
func (host *ChiHost) WalkVolumeClaimTemplates(f func(template *ChiVolumeClaimTemplate)) {
	host.CHI.WalkVolumeClaimTemplates(f)
}

// WalkVolumeMounts walks VolumeMount(s)
func (host *ChiHost) WalkVolumeMounts(f func(volumeMount *corev1.VolumeMount)) {
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
