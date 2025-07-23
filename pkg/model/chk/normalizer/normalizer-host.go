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
	core "k8s.io/api/core/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/common/creator"
	"github.com/altinity/clickhouse-operator/pkg/model/common/namer"
)

func (n *Normalizer) hostApplyHostTemplateSpecifiedOrDefault(host *chi.Host) {
	hostTemplate := n.hostGetHostTemplate(host)
	hostApplyHostTemplate(host, hostTemplate)
}

// hostGetHostTemplate gets Host Template to be used to normalize Host
func (n *Normalizer) hostGetHostTemplate(host *chi.Host) *chi.HostTemplate {
	// Which host template would be used - either explicitly defined in or a default one
	if hostTemplate, ok := host.GetHostTemplate(); ok {
		// Host explicitly references known HostTemplate
		log.V(2).M(host).F().Info("host: %s uses custom hostTemplate: %s", host.Name, hostTemplate.Name)
		return hostTemplate
	}

	// Host references either no template or an UNKNOWN HostTemplate, thus will use a default host template.
	// However, with the default host template there is a nuance - hostNetwork requires different default host template.

	// Check hostNetwork case at first
	if podTemplate, ok := host.GetPodTemplate(); ok {
		if podTemplate.Spec.HostNetwork {
			// HostNetwork
			log.V(3).M(host).F().Info("host: %s uses default hostTemplate for HostNetwork", host.Name)
			return creator.CreateHostTemplate(interfaces.HostTemplateHostNetwork, n.namer.Name(interfaces.NameHostTemplate, host))
		}
	}

	// Pick default host template
	log.V(3).M(host).F().Info("host: %s uses default hostTemplate", host.Name)
	return creator.CreateHostTemplate(interfaces.HostTemplateCommon, n.namer.Name(interfaces.NameHostTemplate, host))
}

// hostApplyHostTemplate
func hostApplyHostTemplate(host *chi.Host, template *chi.HostTemplate) {
	if !host.HasName() {
		host.SetName(template.Spec.Name)
		log.V(3).M(host).F().Info("host has no name specified thus assigning name from Spec: %s", host.GetName())
	}

	host.Insecure = host.Insecure.MergeFrom(template.Spec.Insecure)
	host.Secure = host.Secure.MergeFrom(template.Spec.Secure)

	hostApplyHostTemplatePortDistribution(host, template)
	hostApplyPortsFromSettings(host)

	host.InheritTemplatesFrom(template)
}

func hostApplyHostTemplatePortDistribution(host *chi.Host, template *chi.HostTemplate) {
	for _, portDistribution := range template.PortDistribution {
		switch portDistribution.Type {
		case deployment.PortDistributionUnspecified:
			if !host.ZKPort.HasValue() {
				host.ZKPort = template.Spec.ZKPort
			}
			if !host.RaftPort.HasValue() {
				host.RaftPort = template.Spec.RaftPort
			}
		case deployment.PortDistributionClusterScopeIndex:
			if !host.ZKPort.HasValue() {
				base := chi.KpDefaultZKPortNumber
				if template.Spec.ZKPort.HasValue() {
					base = template.Spec.ZKPort.Value()
				}
				host.ZKPort = types.NewInt32(base + int32(host.Runtime.Address.ClusterScopeIndex))
			}
			if !host.RaftPort.HasValue() {
				base := chi.KpDefaultRaftPortNumber
				if template.Spec.RaftPort.HasValue() {
					base = template.Spec.RaftPort.Value()
				}
				host.RaftPort = types.NewInt32(base + int32(host.Runtime.Address.ClusterScopeIndex))
			}
		}
	}
}

// hostApplyPortsFromSettings
func hostApplyPortsFromSettings(host *chi.Host) {
	// Use host personal settings at first
	hostEnsurePortValuesFromSettings(host, host.GetSettings(), false)
	// Fallback to common settings
	hostEnsurePortValuesFromSettings(host, host.GetCR().GetSpec().GetConfiguration().GetSettings(), true)
}

// hostEnsurePortValuesFromSettings fetches port spec from settings, if any provided
func hostEnsurePortValuesFromSettings(host *chi.Host, settings *chi.Settings, final bool) {
	//
	// 1. Setup fallback/default ports
	//
	// For intermittent (non-final) setup fallback values should be from "MustBeAssignedLater" family,
	// because this is not final setup (just intermittent) and all these ports may be overwritten later
	var (
		fallbackZKPort   *types.Int32
		fallbackRaftPort *types.Int32
	)

	// On the other hand, for final setup we need to assign real numbers to ports
	if final {
		fallbackZKPort = types.NewInt32(chi.KpDefaultZKPortNumber)
		fallbackRaftPort = types.NewInt32(chi.KpDefaultRaftPortNumber)
	}

	//
	// 2. Setup ports
	//
	host.ZKPort = types.EnsurePortValue(host.ZKPort, settings.GetZKPort(), fallbackZKPort)
	host.RaftPort = types.EnsurePortValue(host.RaftPort, settings.GetRaftPort(), fallbackRaftPort)
}

// createHostsField
func createHostsField(cluster *chk.Cluster) {
	// Create HostsField of required size
	cluster.Layout.HostsField = chi.NewHostsField(cluster.Layout.ShardsCount, cluster.Layout.ReplicasCount)

	//
	// Migrate hosts from Shards and Replicas into HostsField.
	// Hosts which are explicitly specified in Shards and Replicas are migrated into HostsField for further use
	//
	hostMigrationFunc := func(shard, replica int, host *chi.Host) error {
		if curHost := cluster.Layout.HostsField.Get(shard, replica); curHost == nil {
			cluster.Layout.HostsField.Set(shard, replica, host)
		} else {
			curHost.MergeFrom(host)
		}
		return nil
	}

	// Run host migration func on all hosts specified in shards and replicas - migrate specified hosts into hosts field
	cluster.WalkHostsByShards(hostMigrationFunc)
	cluster.WalkHostsByReplicas(hostMigrationFunc)
}

// normalizeHostStage1 normalizes a host
func (n *Normalizer) normalizeHostStage1(
	host *chi.Host,
	shard chi.IShard,
	replica chi.IReplica,
	cluster chi.ICluster,
	shardIndex int,
	replicaIndex int,
) {
	n.normalizeHostName(host, shard, shardIndex, replica, replicaIndex)
}

// normalizeHostStage2 normalizes a host
func (n *Normalizer) normalizeHostStage2(
	host *chi.Host,
	shard chi.IShard,
	replica chi.IReplica,
	cluster chi.ICluster,
	shardIndex int,
	replicaIndex int,
) {
	// Inherit from either Shard or Replica - use one of them as a source
	src := cluster.SelectSettingsSourceFrom(shard, replica)
	log.V(2).M(src).F().Info("will be used as source for host: %s", host.GetName())

	host.InheritSettingsFrom(src)
	host.Settings = n.normalizeConfigurationSettings(host.Settings)
	host.InheritFilesFrom(src)
	host.Files = n.normalizeConfigurationFiles(host.Files)
	host.InheritTemplatesFrom(src)

	n.normalizeHostEnvVars()
}

func (n *Normalizer) normalizeHostEnvVars() {
	log.V(2).F().Info("going to add env var CLICKHOUSE_DATA_DIR")
	n.req.AppendAdditionalEnvVar(
		core.EnvVar{
			Name:  "CLICKHOUSE_DATA_DIR",
			Value: "/var/lib/clickhouse-keeper",
		},
	)
}

// normalizeHostName normalizes host's name
func (n *Normalizer) normalizeHostName(
	host *chi.Host,
	shard chi.IShard,
	shardIndex int,
	replica chi.IReplica,
	replicaIndex int,
) {
	hasHostName := len(host.GetName()) > 0
	explicitlySpecifiedHostName := !namer.IsAutoGeneratedHostName(host.GetName(), host, shard, shardIndex, replica, replicaIndex)
	if hasHostName && explicitlySpecifiedHostName {
		// Has explicitly specified name already, normalization is not required
		return
	}

	// Create host name
	host.Name = n.namer.Name(interfaces.NameHost, host, shard, shardIndex, replica, replicaIndex)
}
