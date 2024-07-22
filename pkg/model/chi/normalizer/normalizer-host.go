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

package normalizer

import (
	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	commonCreator "github.com/altinity/clickhouse-operator/pkg/model/common/creator"
	commonNamer "github.com/altinity/clickhouse-operator/pkg/model/common/namer"
)

func (n *Normalizer) hostApplyHostTemplateSpecifiedOrDefault(host *api.Host) {
	hostTemplate := n.hostGetHostTemplate(host)
	hostApplyHostTemplate(host, hostTemplate)
}

// hostGetHostTemplate gets Host Template to be used to normalize Host
func (n *Normalizer) hostGetHostTemplate(host *api.Host) *api.HostTemplate {
	// Which host template would be used - either explicitly defined in or a default one
	hostTemplate, ok := host.GetHostTemplate()
	if ok {
		// Host explicitly references known HostTemplate
		log.V(2).M(host).F().Info("host: %s uses custom hostTemplate %s", host.Name, hostTemplate.Name)
		return hostTemplate
	}

	// Host references either no template or UNKNOWN HostTemplate, thus will use a default one
	// However, with the default template there is a nuance - hostNetwork requires different default host template

	// Check hostNetwork case at first
	if podTemplate, ok := host.GetPodTemplate(); ok {
		if podTemplate.Spec.HostNetwork {
			// HostNetwork
			hostTemplate = commonCreator.CreateHostTemplate(interfaces.HostTemplateHostNetwork, n.namer.Name(interfaces.NameHostTemplate, host))
		}
	}

	// In case hostTemplate still is not picked - use default one
	if hostTemplate == nil {
		hostTemplate = commonCreator.CreateHostTemplate(interfaces.HostTemplateCommon, n.namer.Name(interfaces.NameHostTemplate, host))
	}

	log.V(3).M(host).F().Info("host: %s use default hostTemplate", host.Name)

	return hostTemplate
}

// hostApplyHostTemplate
func hostApplyHostTemplate(host *api.Host, template *api.HostTemplate) {
	if host.GetName() == "" {
		host.Name = template.Spec.Name
	}

	host.Insecure = host.Insecure.MergeFrom(template.Spec.Insecure)
	host.Secure = host.Secure.MergeFrom(template.Spec.Secure)

	for _, portDistribution := range template.PortDistribution {
		switch portDistribution.Type {
		case deployment.PortDistributionUnspecified:
			if !host.TCPPort.HasValue() {
				host.TCPPort = template.Spec.TCPPort
			}
			if !host.TLSPort.HasValue() {
				host.TLSPort = template.Spec.TLSPort
			}
			if !host.HTTPPort.HasValue() {
				host.HTTPPort = template.Spec.HTTPPort
			}
			if !host.HTTPSPort.HasValue() {
				host.HTTPSPort = template.Spec.HTTPSPort
			}
			if !host.InterserverHTTPPort.HasValue() {
				host.InterserverHTTPPort = template.Spec.InterserverHTTPPort
			}
		case deployment.PortDistributionClusterScopeIndex:
			if !host.TCPPort.HasValue() {
				base := api.ChDefaultTCPPortNumber
				if template.Spec.TCPPort.HasValue() {
					base = template.Spec.TCPPort.Value()
				}
				host.TCPPort = api.NewInt32(base + int32(host.Runtime.Address.ClusterScopeIndex))
			}
			if !host.TLSPort.HasValue() {
				base := api.ChDefaultTLSPortNumber
				if template.Spec.TLSPort.HasValue() {
					base = template.Spec.TLSPort.Value()
				}
				host.TLSPort = api.NewInt32(base + int32(host.Runtime.Address.ClusterScopeIndex))
			}
			if !host.HTTPPort.HasValue() {
				base := api.ChDefaultHTTPPortNumber
				if template.Spec.HTTPPort.HasValue() {
					base = template.Spec.HTTPPort.Value()
				}
				host.HTTPPort = api.NewInt32(base + int32(host.Runtime.Address.ClusterScopeIndex))
			}
			if !host.HTTPSPort.HasValue() {
				base := api.ChDefaultHTTPSPortNumber
				if template.Spec.HTTPSPort.HasValue() {
					base = template.Spec.HTTPSPort.Value()
				}
				host.HTTPSPort = api.NewInt32(base + int32(host.Runtime.Address.ClusterScopeIndex))
			}
			if !host.InterserverHTTPPort.HasValue() {
				base := api.ChDefaultInterserverHTTPPortNumber
				if template.Spec.InterserverHTTPPort.HasValue() {
					base = template.Spec.InterserverHTTPPort.Value()
				}
				host.InterserverHTTPPort = api.NewInt32(base + int32(host.Runtime.Address.ClusterScopeIndex))
			}
		}
	}

	hostApplyPortsFromSettings(host)

	host.InheritTemplatesFrom(nil, nil, template)
}

// hostApplyPortsFromSettings
func hostApplyPortsFromSettings(host *api.Host) {
	// Use host personal settings at first
	hostEnsurePortValuesFromSettings(host, host.GetSettings(), false)
	// Fallback to common settings
	hostEnsurePortValuesFromSettings(host, host.GetCR().GetSpec().Configuration.Settings, true)
}

// hostEnsurePortValuesFromSettings fetches port spec from settings, if any provided
func hostEnsurePortValuesFromSettings(host *api.Host, settings *api.Settings, final bool) {
	//
	// 1. Setup fallback/default ports
	//
	// For intermittent (non-final) setup fallback values should be from "MustBeAssignedLater" family,
	// because this is not final setup (just intermittent) and all these ports may be overwritten later
	var (
		fallbackTCPPort             *api.Int32
		fallbackTLSPort             *api.Int32
		fallbackHTTPPort            *api.Int32
		fallbackHTTPSPort           *api.Int32
		fallbackInterserverHTTPPort *api.Int32
	)

	// On the other hand, for final setup we need to assign real numbers to ports
	if final {
		if host.IsInsecure() {
			fallbackTCPPort = api.NewInt32(api.ChDefaultTCPPortNumber)
			fallbackHTTPPort = api.NewInt32(api.ChDefaultHTTPPortNumber)
		}
		if host.IsSecure() {
			fallbackTLSPort = api.NewInt32(api.ChDefaultTLSPortNumber)
			fallbackHTTPSPort = api.NewInt32(api.ChDefaultHTTPSPortNumber)
		}
		fallbackInterserverHTTPPort = api.NewInt32(api.ChDefaultInterserverHTTPPortNumber)
	}

	//
	// 2. Setup ports
	//
	host.TCPPort = api.EnsurePortValue(host.TCPPort, settings.GetTCPPort(), fallbackTCPPort)
	host.TLSPort = api.EnsurePortValue(host.TLSPort, settings.GetTCPPortSecure(), fallbackTLSPort)
	host.HTTPPort = api.EnsurePortValue(host.HTTPPort, settings.GetHTTPPort(), fallbackHTTPPort)
	host.HTTPSPort = api.EnsurePortValue(host.HTTPSPort, settings.GetHTTPSPort(), fallbackHTTPSPort)
	host.InterserverHTTPPort = api.EnsurePortValue(host.InterserverHTTPPort, settings.GetInterserverHTTPPort(), fallbackInterserverHTTPPort)
}

// createHostsField
func createHostsField(cluster *api.Cluster) {
	cluster.Layout.HostsField = api.NewHostsField(cluster.Layout.ShardsCount, cluster.Layout.ReplicasCount)

	// Need to migrate hosts from Shards and Replicas into HostsField
	hostMergeFunc := func(shard, replica int, host *api.Host) error {
		if curHost := cluster.Layout.HostsField.Get(shard, replica); curHost == nil {
			cluster.Layout.HostsField.Set(shard, replica, host)
		} else {
			curHost.MergeFrom(host)
		}
		return nil
	}

	cluster.WalkHostsByShards(hostMergeFunc)
	cluster.WalkHostsByReplicas(hostMergeFunc)
}

// normalizeHost normalizes a host/replica
func (n *Normalizer) normalizeHost(
	host *api.Host,
	shard *api.ChiShard,
	replica *api.ChiReplica,
	cluster *api.Cluster,
	shardIndex int,
	replicaIndex int,
) {

	n.normalizeHostName(host, shard, shardIndex, replica, replicaIndex)
	// Inherit from either Shard or Replica
	var s *api.ChiShard
	var r *api.ChiReplica
	if cluster.IsShardSpecified() {
		s = shard
	} else {
		r = replica
	}
	host.InheritSettingsFrom(s, r)
	host.Settings = n.normalizeConfigurationSettings(host.Settings)
	host.InheritFilesFrom(s, r)
	host.Files = n.normalizeConfigurationFiles(host.Files)
	host.InheritTemplatesFrom(s, r, nil)
}

// normalizeHostName normalizes host's name
func (n *Normalizer) normalizeHostName(
	host *api.Host,
	shard *api.ChiShard,
	shardIndex int,
	replica *api.ChiReplica,
	replicaIndex int,
) {
	if (len(host.GetName()) > 0) && !commonNamer.IsAutoGeneratedHostName(host.GetName(), host, shard, shardIndex, replica, replicaIndex) {
		// Has explicitly specified name already
		return
	}

	host.Name = n.namer.Name(interfaces.NameHost, host, shard, shardIndex, replica, replicaIndex)
}
