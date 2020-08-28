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
	"k8s.io/api/core/v1"

	"github.com/altinity/clickhouse-operator/pkg/util"
)

func (host *ChiHost) InheritSettingsFrom(shard *ChiShard, replica *ChiReplica) {
	if shard != nil {
		(&host.Settings).MergeFrom(shard.Settings)
	}

	if replica != nil {
		(&host.Settings).MergeFrom(replica.Settings)
	}
}

func (host *ChiHost) InheritFilesFrom(shard *ChiShard, replica *ChiReplica) {
	if shard != nil {
		(&host.Files).MergeFrom(shard.Files)
	}

	if replica != nil {
		(&host.Files).MergeFrom(replica.Files)
	}
}

func (host *ChiHost) InheritTemplatesFrom(shard *ChiShard, replica *ChiReplica, template *ChiHostTemplate) {
	if shard != nil {
		(&host.Templates).MergeFrom(&shard.Templates, MergeTypeFillEmptyValues)
	}

	if replica != nil {
		(&host.Templates).MergeFrom(&replica.Templates, MergeTypeFillEmptyValues)
	}

	if template != nil {
		(&host.Templates).MergeFrom(&template.Spec.Templates, MergeTypeFillEmptyValues)
	}
	(&host.Templates).HandleDeprecatedFields()
}

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
	(&host.Templates).MergeFrom(&from.Templates, MergeTypeFillEmptyValues)
	(&host.Templates).HandleDeprecatedFields()
}

func (host *ChiHost) GetHostTemplate() (*ChiHostTemplate, bool) {
	name := host.Templates.HostTemplate
	template, ok := host.CHI.GetHostTemplate(name)
	return template, ok
}

func (host *ChiHost) GetPodTemplate() (*ChiPodTemplate, bool) {
	name := host.Templates.PodTemplate
	template, ok := host.CHI.GetPodTemplate(name)
	return template, ok
}

func (host *ChiHost) GetServiceTemplate() (*ChiServiceTemplate, bool) {
	name := host.Templates.ReplicaServiceTemplate
	template, ok := host.CHI.GetServiceTemplate(name)
	return template, ok
}

func (host *ChiHost) GetReplicasNum() int32 {
	if util.IsStringBoolTrue(host.CHI.Spec.Stop) {
		return 0
	} else {
		return 1
	}
}

func (host *ChiHost) GetSettings() Settings {
	return host.Settings
}

func (host *ChiHost) GetZookeeper() *ChiZookeeperConfig {
	cluster := host.GetCluster()
	return &cluster.Zookeeper
}

func (host *ChiHost) GetCHI() *ClickHouseInstallation {
	return host.CHI
}

func (host *ChiHost) GetCluster() *ChiCluster {
	// Host has to have filled Address
	for index := range host.CHI.Spec.Configuration.Clusters {
		cluster := &host.CHI.Spec.Configuration.Clusters[index]
		if host.Address.ClusterName == cluster.Name {
			return cluster
		}
	}

	// This should not happen, actually

	return nil
}

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

func (host *ChiHost) WalkVolumeClaimTemplates(f func(template *ChiVolumeClaimTemplate)) {
	host.CHI.WalkVolumeClaimTemplates(f)
}

func (host *ChiHost) WalkVolumeMounts(f func(volumeMount *v1.VolumeMount)) {
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

// GetAnnotations returns chi annotations and excludes
func (host *ChiHost) GetAnnotations() map[string]string {
	annotations := make(map[string]string, 0)
	for key, value := range host.CHI.Annotations {
		if isAnnotationToBeSkipped(key) {
			continue
		}
		annotations[key] = value
	}
	return annotations
}

// isAnnotationToBeSkipped checks whether an annotation be skipped
func isAnnotationToBeSkipped(annotation string) bool {
	switch annotation {
	case "kubectl.kubernetes.io/last-applied-configuration":
		return true
	}
	return false
}
