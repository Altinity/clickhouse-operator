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
	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	templatesNormalizer "github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer/templates"
	"github.com/altinity/clickhouse-operator/pkg/model/chk/config"
	commonCreator "github.com/altinity/clickhouse-operator/pkg/model/common/creator"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer/templates"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

// Normalizer specifies structures normalizer
type Normalizer struct {
	ctx *Context
}

// NewNormalizer creates new normalizer
func NewNormalizer() *Normalizer {
	return &Normalizer{}
}

// CreateTemplatedCHK produces ready-to-use CHK object
func (n *Normalizer) CreateTemplatedCHK(subj *apiChk.ClickHouseKeeperInstallation, options *normalizer.Options) (
	*apiChk.ClickHouseKeeperInstallation,
	error,
) {
	// Normalization starts with a new context
	n.buildContext(options)
	// Ensure normalization subject presence
	subj = n.ensureSubject(subj)
	// Build target from all templates and subject
	n.buildTargetFromTemplates(subj)
	// And launch normalization of the whole stack
	return n.normalizeTarget()
}

func (n *Normalizer) buildContext(options *normalizer.Options) {
	n.ctx = NewContext(options)
}

func (n *Normalizer) buildTargetFromTemplates(subj *apiChk.ClickHouseKeeperInstallation) {
	// Create new target that will be populated with data during normalization process
	n.ctx.SetTarget(n.createTarget())

	// At this moment we have target available - is either newly created or a system-wide template

	// Apply templates - both auto and explicitly requested - on top of target
	//n.applyTemplatesOnTarget(subj)

	// After all templates applied, place provided 'subject' on top of the whole stack (target)
	n.ctx.GetTarget().MergeFrom(subj, apiChi.MergeTypeOverrideByNonEmptyValues)
}

func (n *Normalizer) applyTemplatesOnTarget(subj templatesNormalizer.TemplateSubject) {
	//for _, template := range templatesNormalizer.ApplyTemplates(n.ctx.GetTarget(), subj) {
	//	n.ctx.GetTarget().EnsureStatus().PushUsedTemplate(template)
	//}
}

func (n *Normalizer) newSubject() *apiChk.ClickHouseKeeperInstallation {
	return managers.CreateCustomResource(managers.CustomResourceCHK).(*apiChk.ClickHouseKeeperInstallation)
}

func (n *Normalizer) ensureSubject(subj *apiChk.ClickHouseKeeperInstallation) *apiChk.ClickHouseKeeperInstallation {
	switch {
	case subj == nil:
		// No subject specified - meaning we are normalizing non-existing subject and it should have no clusters inside
		// Need to create subject
		n.ctx.Options().WithDefaultCluster = false
		return n.newSubject()
	default:
		// Subject specified - meaning we are normalizing existing subject and we need to ensure default cluster presence
		n.ctx.Options().WithDefaultCluster = true
		return subj
	}
}

func (n *Normalizer) GetTargetTemplate() *apiChk.ClickHouseKeeperInstallation {
	//return chop.Config().Template.CHI.Runtime.Template
	return nil
}

func (n *Normalizer) HasTargetTemplate() bool {
	return n.GetTargetTemplate() != nil
}

func (n *Normalizer) createTarget() *apiChk.ClickHouseKeeperInstallation {
	//if n.HasTargetTemplate() {
	//	// Template specified - start with template
	//	return n.GetTargetTemplate().DeepCopy()
	//} else {
	//	// No template specified - start with clear page
	return n.newSubject()
	//}
}

// normalizeTarget normalizes target
func (n *Normalizer) normalizeTarget() (*apiChk.ClickHouseKeeperInstallation, error) {
	n.normalizeSpec()
	n.finalize()
	n.fillStatus()

	return n.ctx.GetTarget(), nil
}

func (n *Normalizer) normalizeSpec() {
	// Walk over ChiSpec datatype fields
	n.ctx.GetTarget().GetSpec().Configuration = n.normalizeConfiguration(n.ctx.chk.Spec.Configuration)
	n.ctx.GetTarget().GetSpec().Templates = n.normalizeTemplates(n.ctx.chk.Spec.Templates)
	// UseTemplates already done
}

// finalize performs some finalization tasks, which should be done after CHI is normalized
func (n *Normalizer) finalize() {
	//n.ctx.GetTarget().Fill()
	//n.ctx.GetTarget().WalkHosts(func(host *api.Host) error {
	//	hostTemplate := n.hostGetHostTemplate(host)
	//	hostApplyHostTemplate(host, hostTemplate)
	//	return nil
	//})
	//n.fillCHIAddressInfo()
}

// fillStatus fills .status section of a CHI with values based on current CHI
func (n *Normalizer) fillStatus() {
	//endpoint := CreateCHIServiceFQDN(n.ctx.chi)
	//pods := make([]string, 0)
	//fqdns := make([]string, 0)
	//n.ctx.chi.WalkHosts(func(host *apiChi.Host) error {
	//	pods = append(pods, CreatePodName(host))
	//	fqdns = append(fqdns, CreateFQDN(host))
	//	return nil
	//})
	//ip, _ := chop.Get().ConfigManager.GetRuntimeParam(apiChi.OPERATOR_POD_IP)
	//n.ctx.chi.FillStatus(endpoint, pods, fqdns, ip)
}

// normalizeNamespaceDomainPattern normalizes .spec.namespaceDomainPattern
//func (n *Normalizer) normalizeNamespaceDomainPattern(namespaceDomainPattern string) string {
//	if strings.Count(namespaceDomainPattern, "%s") > 1 {
//		return ""
//	}
//	return namespaceDomainPattern
//}

// normalizeConfiguration normalizes .spec.configuration
func (n *Normalizer) normalizeConfiguration(conf *apiChk.ChkConfiguration) *apiChk.ChkConfiguration {
	if conf == nil {
		conf = apiChk.NewConfiguration()
	}
	conf.Settings = n.normalizeConfigurationSettings(conf.Settings)
	conf.Clusters = n.normalizeClusters(conf.Clusters)
	return conf
}

// normalizeTemplates normalizes .spec.templates
func (n *Normalizer) normalizeTemplates(templates *apiChi.Templates) *apiChi.Templates {
	if templates == nil {
		return nil
	}

	n.normalizeHostTemplates(templates)
	n.normalizePodTemplates(templates)
	n.normalizeVolumeClaimTemplates(templates)
	n.normalizeServiceTemplates(templates)
	return templates
}

func (n *Normalizer) normalizeHostTemplates(templates *apiChi.Templates) {
	for i := range templates.HostTemplates {
		n.normalizeHostTemplate(&templates.HostTemplates[i])
	}
}

func (n *Normalizer) normalizePodTemplates(templates *apiChi.Templates) {
	for i := range templates.PodTemplates {
		n.normalizePodTemplate(&templates.PodTemplates[i])
	}
}

func (n *Normalizer) normalizeVolumeClaimTemplates(templates *apiChi.Templates) {
	for i := range templates.VolumeClaimTemplates {
		n.normalizeVolumeClaimTemplate(&templates.VolumeClaimTemplates[i])
	}
}

func (n *Normalizer) normalizeServiceTemplates(templates *apiChi.Templates) {
	for i := range templates.ServiceTemplates {
		n.normalizeServiceTemplate(&templates.ServiceTemplates[i])
	}
}

// normalizeHostTemplate normalizes .spec.templates.hostTemplates
func (n *Normalizer) normalizeHostTemplate(template *apiChi.HostTemplate) {
	templates.NormalizeHostTemplate(template)
	// Introduce HostTemplate into Index
	n.ctx.GetTarget().GetSpec().Templates.EnsureHostTemplatesIndex().Set(template.Name, template)
}

// normalizePodTemplate normalizes .spec.templates.podTemplates
func (n *Normalizer) normalizePodTemplate(template *apiChi.PodTemplate) {
	// TODO need to support multi-cluster
	replicasCount := 1
	if len(n.ctx.chk.Spec.Configuration.Clusters) > 0 {
		replicasCount = n.ctx.chk.Spec.Configuration.Clusters[0].Layout.ReplicasCount
	}
	templates.NormalizePodTemplate(replicasCount, template)
	// Introduce PodTemplate into Index
	n.ctx.chk.Spec.Templates.EnsurePodTemplatesIndex().Set(template.Name, template)
}

// normalizeVolumeClaimTemplate normalizes .spec.templates.volumeClaimTemplates
func (n *Normalizer) normalizeVolumeClaimTemplate(template *apiChi.VolumeClaimTemplate) {
	templates.NormalizeVolumeClaimTemplate(template)
	// Introduce VolumeClaimTemplate into Index
	n.ctx.chk.Spec.Templates.EnsureVolumeClaimTemplatesIndex().Set(template.Name, template)
}

// normalizeServiceTemplate normalizes .spec.templates.serviceTemplates
func (n *Normalizer) normalizeServiceTemplate(template *apiChi.ServiceTemplate) {
	templates.NormalizeServiceTemplate(template)
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

// ensureClusters
func (n *Normalizer) ensureClusters(clusters []*apiChk.ChkCluster) []*apiChk.ChkCluster {
	// May be we have cluster(s) available
	if len(clusters) > 0 {
		return clusters
	}

	// In case no clusters available, we may want to create a default one
	if n.ctx.options.WithDefaultCluster {
		return []*apiChk.ChkCluster{
			commonCreator.CreateCluster(interfaces.ClusterCHKDefault).(*apiChk.ChkCluster),
		}
	}

	// Nope, no clusters expected
	return nil
}

// normalizeConfigurationSettings normalizes .spec.configuration.settings
func (n *Normalizer) normalizeConfigurationSettings(settings *apiChi.Settings) *apiChi.Settings {
	return settings.
		Ensure().
		MergeFrom(config.DefaultSettings(n.ctx.chk.Spec.GetPath())).
		Normalize()
}

// normalizeCluster normalizes cluster and returns deployments usage counters for this cluster
func (n *Normalizer) normalizeCluster(cluster *apiChk.ChkCluster) *apiChk.ChkCluster {
	// Ensure cluster
	if cluster == nil {
		cluster = commonCreator.CreateCluster(interfaces.ClusterCHKDefault).(*apiChk.ChkCluster)
	}

	// Ensure layout
	if cluster.Layout == nil {
		cluster.Layout = apiChi.NewClusterLayout()
	}
	cluster.Layout = n.normalizeClusterLayoutShardsCountAndReplicasCount(cluster.Layout)

	return cluster
}

// normalizeClusterLayoutShardsCountAndReplicasCount ensures at least 1 shard and 1 replica counters
func (n *Normalizer) normalizeClusterLayoutShardsCountAndReplicasCount(layout *apiChi.ClusterLayout) *apiChi.ClusterLayout {
	// Ensure layout
	if layout == nil {
		layout = apiChi.NewClusterLayout()
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
