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

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer"
	templatesNormalizer "github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer/templates"
)

// NormalizerContext specifies CHI-related normalization context
type NormalizerContext struct {
	// chk specifies current CHK being normalized
	chk *apiChk.ClickHouseKeeperInstallation
	// options specifies normalization options
	options *normalizer.Options
}

// NewNormalizerContext creates new NormalizerContext
func NewNormalizerContext(options *normalizer.Options) *NormalizerContext {
	return &NormalizerContext{
		options: options,
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
	options *normalizer.Options,
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
func (n *Normalizer) normalizeTemplates(templates *apiChi.ChiTemplates) *apiChi.ChiTemplates {
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
func (n *Normalizer) normalizePodTemplate(template *apiChi.PodTemplate) {
	// TODO need to support multi-cluster
	replicasCount := 1
	if len(n.ctx.chk.Spec.Configuration.Clusters) > 0 {
		replicasCount = n.ctx.chk.Spec.Configuration.Clusters[0].Layout.ReplicasCount
	}
	templatesNormalizer.NormalizePodTemplate(replicasCount, template)
	// Introduce PodTemplate into Index
	n.ctx.chk.Spec.Templates.EnsurePodTemplatesIndex().Set(template.Name, template)
}

// normalizeVolumeClaimTemplate normalizes .spec.templates.volumeClaimTemplates
func (n *Normalizer) normalizeVolumeClaimTemplate(template *apiChi.VolumeClaimTemplate) {
	templatesNormalizer.NormalizeVolumeClaimTemplate(template)
	// Introduce VolumeClaimTemplate into Index
	n.ctx.chk.Spec.Templates.EnsureVolumeClaimTemplatesIndex().Set(template.Name, template)
}

// normalizeServiceTemplate normalizes .spec.templates.serviceTemplates
func (n *Normalizer) normalizeServiceTemplate(template *apiChi.ServiceTemplate) {
	templatesNormalizer.NormalizeServiceTemplate(template)
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
