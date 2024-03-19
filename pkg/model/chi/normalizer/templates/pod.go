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

package templates

import (
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	model "github.com/altinity/clickhouse-operator/pkg/model/chi"
)

// NormalizePodTemplate normalizes .spec.templates.podTemplates
func NormalizePodTemplate(replicasCount int, template *api.ChiPodTemplate) {
	// Name

	// Zone
	switch {
	case len(template.Zone.Values) == 0:
		// In case no values specified - no key is reasonable
		template.Zone.Key = ""
	case template.Zone.Key == "":
		// We have values specified, but no key
		// Use default zone key in this case
		template.Zone.Key = core.LabelTopologyZone
	default:
		// We have both key and value(s) specified explicitly
	}

	// PodDistribution
	for i := range template.PodDistribution {
		if additionalPodDistributions := normalizePodDistribution(replicasCount, &template.PodDistribution[i]); additionalPodDistributions != nil {
			template.PodDistribution = append(template.PodDistribution, additionalPodDistributions...)
		}
	}

	// Spec
	template.Spec.Affinity = model.MergeAffinity(template.Spec.Affinity, model.NewAffinity(template))

	// In case we have hostNetwork specified, we need to have ClusterFirstWithHostNet DNS policy, because of
	// https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/#pod-s-dns-policy
	// which tells:  For Pods running with hostNetwork, you should explicitly set its DNS policy “ClusterFirstWithHostNet”.
	if template.Spec.HostNetwork {
		template.Spec.DNSPolicy = core.DNSClusterFirstWithHostNet
	}
}

const defaultTopologyKey = core.LabelHostname

func normalizePodDistribution(replicasCount int, podDistribution *api.ChiPodDistribution) []api.ChiPodDistribution {
	// Ensure topology key
	if podDistribution.TopologyKey == "" {
		podDistribution.TopologyKey = defaultTopologyKey
	}

	switch podDistribution.Type {
	case
		deployment.PodDistributionUnspecified,
		// AntiAffinity section
		deployment.PodDistributionClickHouseAntiAffinity,
		deployment.PodDistributionShardAntiAffinity,
		deployment.PodDistributionReplicaAntiAffinity:
		// PodDistribution is known
		if podDistribution.Scope == "" {
			podDistribution.Scope = deployment.PodDistributionScopeCluster
		}
		return nil
	case
		deployment.PodDistributionAnotherNamespaceAntiAffinity,
		deployment.PodDistributionAnotherClickHouseInstallationAntiAffinity,
		deployment.PodDistributionAnotherClusterAntiAffinity:
		// PodDistribution is known
		return nil
	case
		deployment.PodDistributionMaxNumberPerNode:
		// PodDistribution is known
		if podDistribution.Number < 0 {
			podDistribution.Number = 0
		}
		return nil
	case
		// Affinity section
		deployment.PodDistributionNamespaceAffinity,
		deployment.PodDistributionClickHouseInstallationAffinity,
		deployment.PodDistributionClusterAffinity,
		deployment.PodDistributionShardAffinity,
		deployment.PodDistributionReplicaAffinity,
		deployment.PodDistributionPreviousTailAffinity:
		// PodDistribution is known
		return nil

	case deployment.PodDistributionCircularReplication:
		// PodDistribution is known
		// PodDistributionCircularReplication is a shortcut to simplify complex set of other distributions
		// All shortcuts have to be expanded

		if podDistribution.Scope == "" {
			podDistribution.Scope = deployment.PodDistributionScopeCluster
		}

		// Expand shortcut
		return []api.ChiPodDistribution{
			{
				Type:  deployment.PodDistributionShardAntiAffinity,
				Scope: podDistribution.Scope,
			},
			{
				Type:  deployment.PodDistributionReplicaAntiAffinity,
				Scope: podDistribution.Scope,
			},
			{
				Type:   deployment.PodDistributionMaxNumberPerNode,
				Scope:  podDistribution.Scope,
				Number: replicasCount,
			},

			{
				Type: deployment.PodDistributionPreviousTailAffinity,
			},

			{
				Type: deployment.PodDistributionNamespaceAffinity,
			},
			{
				Type: deployment.PodDistributionClickHouseInstallationAffinity,
			},
			{
				Type: deployment.PodDistributionClusterAffinity,
			},
		}
	}

	// PodDistribution is not known
	podDistribution.Type = deployment.PodDistributionUnspecified
	return nil
}
