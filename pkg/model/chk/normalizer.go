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

package chk

import (
	"strings"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/chi"
)

// NormalizerContext specifies CHI-related normalization context
type NormalizerContext struct {
	// start specifies start CHK from which normalization has started
	start *apiChk.ClickHouseKeeperInstallation
	// chk specifies current CHK being normalized
	chk *apiChk.ClickHouseKeeperInstallation
	// options specifies normalization options
	options *NormalizerOptions
}

// NewNormalizerContext creates new NormalizerContext
func NewNormalizerContext(options *NormalizerOptions) *NormalizerContext {
	return &NormalizerContext{
		options: options,
	}
}

// NormalizerOptions specifies normalization options
type NormalizerOptions struct {
	// WithDefaultCluster specifies whether to insert default cluster in case no cluster specified
	WithDefaultCluster bool
}

// NewNormalizerOptions creates new NormalizerOptions
func NewNormalizerOptions() *NormalizerOptions {
	return &NormalizerOptions{
		WithDefaultCluster: true,
	}
}

// Normalizer specifies structures normalizer
type Normalizer struct {
	ctx *NormalizerContext
}

// NewNormalizer creates new normalizer
func NewNormalizer() *Normalizer {
	return &Normalizer{}
}

func newCHK() *apiChk.ClickHouseKeeperInstallation {
	return &apiChk.ClickHouseKeeperInstallation{
		TypeMeta: meta.TypeMeta{
			Kind:       apiChk.ClickHouseKeeperInstallationCRDResourceKind,
			APIVersion: apiChk.SchemeGroupVersion.String(),
		},
	}
}

// CreateTemplatedCHK produces ready-to-use CHK object
func (n *Normalizer) CreateTemplatedCHK(
	chk *apiChk.ClickHouseKeeperInstallation,
	options *NormalizerOptions,
) (*apiChk.ClickHouseKeeperInstallation, error) {
	// New CHI starts with new context
	n.ctx = NewNormalizerContext(options)

	if chk == nil {
		// No CHK specified - meaning we are building over provided 'empty' CHK with no clusters inside
		chk = newCHK()
		n.ctx.options.WithDefaultCluster = false
	} else {
		// Even in case having CHI provided, we need to insert default cluster in case no clusters specified
		n.ctx.options.WithDefaultCluster = true
	}

	n.ctx.chk = newCHK()

	n.ctx.chk.MergeFrom(chk, apiChi.MergeTypeOverrideByNonEmptyValues)

	return n.normalize()
}

// normalize normalizes whole CHI.
// Returns normalized CHI
func (n *Normalizer) normalize() (*apiChk.ClickHouseKeeperInstallation, error) {
	// Walk over ChiSpec datatype fields
	n.ctx.chk.Spec.Configuration = n.normalizeConfiguration(n.ctx.chk.Spec.Configuration)
	n.ctx.chk.Spec.Templates = n.normalizeTemplates(n.ctx.chk.Spec.Templates)
	// UseTemplates already done

	n.fillStatus()

	return n.ctx.chk, nil
}

// fillStatus fills .status section of a CHI with values based on current CHI
func (n *Normalizer) fillStatus() {
	//endpoint := CreateCHIServiceFQDN(n.ctx.chi)
	//pods := make([]string, 0)
	//fqdns := make([]string, 0)
	//n.ctx.chi.WalkHosts(func(host *apiChi.ChiHost) error {
	//	pods = append(pods, CreatePodName(host))
	//	fqdns = append(fqdns, CreateFQDN(host))
	//	return nil
	//})
	//ip, _ := chop.Get().ConfigManager.GetRuntimeParam(apiChi.OPERATOR_POD_IP)
	//n.ctx.chi.FillStatus(endpoint, pods, fqdns, ip)
}

// normalizeNamespaceDomainPattern normalizes .spec.namespaceDomainPattern
func (n *Normalizer) normalizeNamespaceDomainPattern(namespaceDomainPattern string) string {
	if strings.Count(namespaceDomainPattern, "%s") > 1 {
		return ""
	}
	return namespaceDomainPattern
}

// normalizeConfiguration normalizes .spec.configuration
func (n *Normalizer) normalizeConfiguration(conf *apiChk.ChkConfiguration) *apiChk.ChkConfiguration {
	// Ensure configuration
	if conf == nil {
		conf = apiChk.NewConfiguration()
	}
	conf.Settings = n.normalizeConfigurationSettings(conf.Settings)
	conf.Clusters = n.normalizeClusters(conf.Clusters)
	return conf
}

// normalizeTemplates normalizes .spec.templates
func (n *Normalizer) normalizeTemplates(templates *apiChk.ChkTemplates) *apiChk.ChkTemplates {
	if templates == nil {
		//templates = apiChi.NewChiTemplates()
		return nil
	}

	for i := range templates.PodTemplates {
		podTemplate := &templates.PodTemplates[i]
		n.normalizePodTemplate(podTemplate)
	}

	for i := range templates.VolumeClaimTemplates {
		vcTemplate := &templates.VolumeClaimTemplates[i]
		n.normalizeVolumeClaimTemplate(vcTemplate)
	}

	for i := range templates.ServiceTemplates {
		serviceTemplate := &templates.ServiceTemplates[i]
		n.normalizeServiceTemplate(serviceTemplate)
	}

	return templates
}

// normalizePodTemplate normalizes .spec.templates.podTemplates
func (n *Normalizer) normalizePodTemplate(template *apiChi.ChiPodTemplate) {
	// Name

	// Zone
	if len(template.Zone.Values) == 0 {
		// In case no values specified - no key is reasonable
		template.Zone.Key = ""
	} else if template.Zone.Key == "" {
		// We have values specified, but no key
		// Use default zone key in this case
		template.Zone.Key = core.LabelTopologyZone
	} else {
		// We have both key and value(s) specified explicitly
	}

	// PodDistribution
	for i := range template.PodDistribution {
		if additionalPoDistributions := n.normalizePodDistribution(&template.PodDistribution[i]); additionalPoDistributions != nil {
			template.PodDistribution = append(template.PodDistribution, additionalPoDistributions...)
		}
	}

	// Spec
	template.Spec.Affinity = chi.MergeAffinity(template.Spec.Affinity, chi.NewAffinity(template))

	// In case we have hostNetwork specified, we need to have ClusterFirstWithHostNet DNS policy, because of
	// https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/#pod-s-dns-policy
	// which tells:  For Pods running with hostNetwork, you should explicitly set its DNS policy “ClusterFirstWithHostNet”.
	if template.Spec.HostNetwork {
		template.Spec.DNSPolicy = core.DNSClusterFirstWithHostNet
	}

	// Introduce PodTemplate into Index
	n.ctx.chk.Spec.Templates.EnsurePodTemplatesIndex().Set(template.Name, template)
}

const defaultTopologyKey = core.LabelHostname

func (n *Normalizer) normalizePodDistribution(podDistribution *apiChi.ChiPodDistribution) []apiChi.ChiPodDistribution {
	if podDistribution.TopologyKey == "" {
		podDistribution.TopologyKey = defaultTopologyKey
	}
	switch podDistribution.Type {
	case
		apiChi.PodDistributionUnspecified,
		// AntiAffinity section
		apiChi.PodDistributionClickHouseAntiAffinity,
		apiChi.PodDistributionShardAntiAffinity,
		apiChi.PodDistributionReplicaAntiAffinity:
		// PodDistribution is known
		if podDistribution.Scope == "" {
			podDistribution.Scope = apiChi.PodDistributionScopeCluster
		}
		return nil
	case
		apiChi.PodDistributionAnotherNamespaceAntiAffinity,
		apiChi.PodDistributionAnotherClickHouseInstallationAntiAffinity,
		apiChi.PodDistributionAnotherClusterAntiAffinity:
		// PodDistribution is known
		return nil
	case
		apiChi.PodDistributionMaxNumberPerNode:
		// PodDistribution is known
		if podDistribution.Number < 0 {
			podDistribution.Number = 0
		}
		return nil
	case
		// Affinity section
		apiChi.PodDistributionNamespaceAffinity,
		apiChi.PodDistributionClickHouseInstallationAffinity,
		apiChi.PodDistributionClusterAffinity,
		apiChi.PodDistributionShardAffinity,
		apiChi.PodDistributionReplicaAffinity,
		apiChi.PodDistributionPreviousTailAffinity:
		// PodDistribution is known
		return nil

	case apiChi.PodDistributionCircularReplication:
		// PodDistribution is known
		// PodDistributionCircularReplication is a shortcut to simplify complex set of other distributions
		// All shortcuts have to be expanded

		if podDistribution.Scope == "" {
			podDistribution.Scope = apiChi.PodDistributionScopeCluster
		}

		// TODO need to support multi-cluster
		cluster := n.ctx.chk.Spec.Configuration.Clusters[0]

		// Expand shortcut
		return []apiChi.ChiPodDistribution{
			{
				Type:  apiChi.PodDistributionShardAntiAffinity,
				Scope: podDistribution.Scope,
			},
			{
				Type:  apiChi.PodDistributionReplicaAntiAffinity,
				Scope: podDistribution.Scope,
			},
			{
				Type:   apiChi.PodDistributionMaxNumberPerNode,
				Scope:  podDistribution.Scope,
				Number: cluster.Layout.ReplicasCount,
			},

			{
				Type: apiChi.PodDistributionPreviousTailAffinity,
			},

			{
				Type: apiChi.PodDistributionNamespaceAffinity,
			},
			{
				Type: apiChi.PodDistributionClickHouseInstallationAffinity,
			},
			{
				Type: apiChi.PodDistributionClusterAffinity,
			},
		}
	}

	// PodDistribution is not known
	podDistribution.Type = apiChi.PodDistributionUnspecified
	return nil
}

// normalizeVolumeClaimTemplate normalizes .spec.templates.volumeClaimTemplates
func (n *Normalizer) normalizeVolumeClaimTemplate(template *apiChi.ChiVolumeClaimTemplate) {
	// Check name
	// Skip for now

	// StorageManagement
	n.normalizeStorageManagement(&template.StorageManagement)

	// Check Spec
	// Skip for now

	// Introduce VolumeClaimTemplate into Index
	n.ctx.chk.Spec.Templates.EnsureVolumeClaimTemplatesIndex().Set(template.Name, template)
}

// normalizeStorageManagement normalizes StorageManagement
func (n *Normalizer) normalizeStorageManagement(storage *apiChi.StorageManagement) {
	// Check PVCProvisioner
	if !storage.PVCProvisioner.IsValid() {
		storage.PVCProvisioner = apiChi.PVCProvisionerUnspecified
	}

	// Check PVCReclaimPolicy
	if !storage.PVCReclaimPolicy.IsValid() {
		storage.PVCReclaimPolicy = apiChi.PVCReclaimPolicyUnspecified
	}
}

// normalizeServiceTemplate normalizes .spec.templates.serviceTemplates
func (n *Normalizer) normalizeServiceTemplate(template *apiChi.ChiServiceTemplate) {
	// Check name
	// Check GenerateName
	// Check ObjectMeta
	// Check Spec

	// Introduce ServiceClaimTemplate into Index
	n.ctx.chk.Spec.Templates.EnsureServiceTemplatesIndex().Set(template.Name, template)
}

// normalizeClusters normalizes clusters
func (n *Normalizer) normalizeClusters(clusters []*apiChk.ChkCluster) []*apiChk.ChkCluster {
	// We need to have at least one cluster available
	clusters = n.ensureClusters(clusters)

	// Normalize all clusters
	for i := range clusters {
		clusters[i] = n.normalizeCluster(clusters[i])
	}

	return clusters
}

// newDefaultCluster
func (n *Normalizer) newDefaultCluster() *apiChk.ChkCluster {
	return &apiChk.ChkCluster{
		Name: "cluster",
	}
}

// ensureClusters
func (n *Normalizer) ensureClusters(clusters []*apiChk.ChkCluster) []*apiChk.ChkCluster {
	if len(clusters) > 0 {
		return clusters
	}

	if n.ctx.options.WithDefaultCluster {
		return []*apiChk.ChkCluster{
			n.newDefaultCluster(),
		}
	}

	return []*apiChk.ChkCluster{}
}

// normalizeConfigurationSettings normalizes .spec.configuration.settings
func (n *Normalizer) normalizeConfigurationSettings(settings *apiChi.Settings) *apiChi.Settings {
	return settings.
		Ensure().
		MergeFrom(defaultKeeperSettings(n.ctx.chk.Spec.GetPath())).
		Normalize()
}

// normalizeCluster normalizes cluster and returns deployments usage counters for this cluster
func (n *Normalizer) normalizeCluster(cluster *apiChk.ChkCluster) *apiChk.ChkCluster {
	// Ensure cluster
	if cluster == nil {
		cluster = n.newDefaultCluster()
	}

	// Ensure layout
	if cluster.Layout == nil {
		cluster.Layout = apiChk.NewChkClusterLayout()
	}
	cluster.Layout = n.normalizeClusterLayoutShardsCountAndReplicasCount(cluster.Layout)

	return cluster
}

// normalizeClusterLayoutShardsCountAndReplicasCount ensures at least 1 shard and 1 replica counters
func (n *Normalizer) normalizeClusterLayoutShardsCountAndReplicasCount(layout *apiChk.ChkClusterLayout) *apiChk.ChkClusterLayout {
	// Ensure layout
	if layout == nil {
		layout = apiChk.NewChkClusterLayout()
	}

	// Layout.ShardsCount and
	// Layout.ReplicasCount must represent max number of shards and replicas requested respectively

	// Deal with ReplicasCount
	if layout.ReplicasCount == 0 {
		// No ReplicasCount specified - need to figure out

		// We need to have at least one Replica
		layout.ReplicasCount = 1
	}

	// Deal with ReplicasCount
	if layout.ReplicasCount > 7 {
		// Too big ReplicasCount specified - need to trim

		// We need to have at max 7 Replicas
		layout.ReplicasCount = 7
	}

	return layout
}
