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

package model

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"gopkg.in/d4l3k/messagediff.v1"
	"k8s.io/api/core/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chiV1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Normalizer
type Normalizer struct {
	chop *chop.CHOp
	chi  *chiV1.ClickHouseInstallation
	// Whether should insert default cluster if no cluster specified
	withDefaultCluster bool
}

// NewNormalizer
func NewNormalizer(chop *chop.CHOp) *Normalizer {
	return &Normalizer{
		chop: chop,
	}
}

// CreateTemplatedCHI produces ready-to-use CHI object
func (n *Normalizer) CreateTemplatedCHI(chi *chiV1.ClickHouseInstallation) (*chiV1.ClickHouseInstallation, error) {
	if chi == nil {
		// No CHI specified - meaning we are building 'empty' CHI with no clusters inside
		chi = new(chiV1.ClickHouseInstallation)
		n.withDefaultCluster = false
	} else {
		// Insert default cluster in case no clusters specified in this CHI
		n.withDefaultCluster = true
	}

	// What base should be used to create CHI
	if n.chop.Config().CHITemplate == nil {
		// No template specified - start with clear page
		n.chi = new(chiV1.ClickHouseInstallation)
	} else {
		// Template specified - start with template
		n.chi = n.chop.Config().CHITemplate.DeepCopy()
	}

	// At this moment n.chi is either empty CHI or a system-wide template
	// We need to apply templates

	// Apply CHOP-specified templates
	// TODO

	// Apply CHI-specified templates

	var useTemplates []chiV1.ChiUseTemplate

	if autoTemplates := n.chop.Config().FindAutoTemplates(); len(autoTemplates) > 0 {
		log.V(2).M(chi).F().Info("Found auto-templates num: %d", len(autoTemplates))
		for _, template := range autoTemplates {
			log.V(3).M(chi).F().Info("Adding auto-template to merge list: %s/%s ", template.Name, template.Namespace)
			useTemplates = append(useTemplates, chiV1.ChiUseTemplate{
				Name:      template.Name,
				Namespace: template.Namespace,
				UseType:   useTypeMerge,
			})
		}
	}

	if len(chi.Spec.UseTemplates) > 0 {
		useTemplates = append(useTemplates, chi.Spec.UseTemplates...)
	}

	// UseTemplates must contain reasonable data, thus has to be normalized
	if len(useTemplates) > 0 {
		n.normalizeUseTemplates(useTemplates)
	}

	for i := range useTemplates {
		useTemplate := &useTemplates[i]
		if template := n.chop.Config().FindTemplate(useTemplate, chi.Namespace); template == nil {
			log.V(1).M(chi).A().Warning("UNABLE to find template %s/%s referenced in useTemplates. Skip it.", useTemplate.Namespace, useTemplate.Name)
		} else {
			(&n.chi.Spec).MergeFrom(&template.Spec, chiV1.MergeTypeOverrideByNonEmptyValues)
			log.V(2).M(chi).F().Info("Merge template %s/%s referenced in useTemplates", useTemplate.Namespace, useTemplate.Name)
		}
	}

	// After all templates applied, place provided CHI on top of the whole stack
	n.chi.MergeFrom(chi, chiV1.MergeTypeOverrideByNonEmptyValues)

	return n.normalize(nil)
}

// NormalizeCHI normalizes CHI.
// Returns normalized CHI
func (n *Normalizer) normalize(chi *chiV1.ClickHouseInstallation) (*chiV1.ClickHouseInstallation, error) {
	if chi != nil {
		n.chi = chi
	}

	// Walk over ChiSpec datatype fields
	n.chi.Spec.UseTemplates = n.normalizeUseTemplates(n.chi.Spec.UseTemplates)
	n.chi.Spec.Stop = n.normalizeStop(n.chi.Spec.Stop)
	n.chi.Spec.NamespaceDomainPattern = n.normalizeNamespaceDomainPattern(n.chi.Spec.NamespaceDomainPattern)
	n.chi.Spec.Templating = n.normalizeTemplating(n.chi.Spec.Templating)
	n.chi.Spec.Reconciling = n.normalizeReconciling(n.chi.Spec.Reconciling)
	n.chi.Spec.Defaults = n.normalizeDefaults(n.chi.Spec.Defaults)
	n.chi.Spec.Configuration = n.normalizeConfiguration(n.chi.Spec.Configuration)
	n.chi.Spec.Templates = n.normalizeTemplates(n.chi.Spec.Templates)
	// UseTemplates already done

	n.finalizeCHI()
	n.fillStatus()

	return n.chi, nil
}

// finalizeCHI performs some finalization tasks, which should be done after CHI is normalized
func (n *Normalizer) finalizeCHI() {
	n.chi.FillSelfCalculatedAddressInfo()
	n.chi.FillCHIPointer()
	n.chi.WalkHosts(func(host *chiV1.ChiHost) error {
		hostTemplate := n.getHostTemplate(host)
		hostApplyHostTemplate(host, hostTemplate)
		return nil
	})
	n.fillCHIAddressInfo()
	n.chi.WalkHosts(func(host *chiV1.ChiHost) error {
		return n.calcFingerprints(host)
	})
}

func (n *Normalizer) fillCHIAddressInfo() {
	n.chi.WalkHostsFullPath(0, 0, func(
		chi *chiV1.ClickHouseInstallation,

		chiScopeIndex int,
		chiScopeCycleSize int,
		chiScopeCycleIndex int,
		chiScopeCycleOffset int,

		clusterScopeIndex int,
		clusterScopeCycleSize int,
		clusterScopeCycleIndex int,
		clusterScopeCycleOffset int,

		clusterIndex int,
		cluster *chiV1.ChiCluster,

		shardIndex int,
		shard *chiV1.ChiShard,

		replicaIndex int,
		replica *chiV1.ChiReplica,

		host *chiV1.ChiHost,
	) error {
		host.Address.StatefulSet = CreateStatefulSetName(host)

		return nil
	})
}

// getHostTemplate gets Host Template to be used to normalize Host
func (n *Normalizer) getHostTemplate(host *chiV1.ChiHost) *chiV1.ChiHostTemplate {
	statefulSetName := CreateStatefulSetName(host)

	// Which host template would be used - either explicitly defined in or a default one
	hostTemplate, ok := host.GetHostTemplate()
	if ok {
		// Host references known HostTemplate
		log.V(2).M(host).F().Info("StatefulSet %s uses custom hostTemplate %s", statefulSetName, hostTemplate.Name)
		return hostTemplate
	}

	// Host references UNKNOWN HostTemplate, will use default one
	// However, with default template there is a nuance - hostNetwork requires different default host template

	// Check hostNetwork case at first
	podTemplate, ok := host.GetPodTemplate()
	if ok {
		if podTemplate.Spec.HostNetwork {
			// HostNetwork
			hostTemplate = newDefaultHostTemplateForHostNetwork(statefulSetName)
		}
	}

	// In case hostTemplate still is not assigned - use default one
	if hostTemplate == nil {
		hostTemplate = newDefaultHostTemplate(statefulSetName)
	}

	log.V(3).M(host).F().Info("StatefulSet %s use default hostTemplate", statefulSetName)

	return hostTemplate
}

// hostApplyHostTemplate
func hostApplyHostTemplate(host *chiV1.ChiHost, template *chiV1.ChiHostTemplate) {
	if host.Name == "" {
		host.Name = template.Spec.Name
	}

	for _, portDistribution := range template.PortDistribution {
		switch portDistribution.Type {
		case chiV1.PortDistributionUnspecified:
			if host.TCPPort == chPortNumberMustBeAssignedLater {
				host.TCPPort = template.Spec.TCPPort
			}
			if host.HTTPPort == chPortNumberMustBeAssignedLater {
				host.HTTPPort = template.Spec.HTTPPort
			}
			if host.InterserverHTTPPort == chPortNumberMustBeAssignedLater {
				host.InterserverHTTPPort = template.Spec.InterserverHTTPPort
			}
		case chiV1.PortDistributionClusterScopeIndex:
			if host.TCPPort == chPortNumberMustBeAssignedLater {
				base := chDefaultTCPPortNumber
				if template.Spec.TCPPort != chPortNumberMustBeAssignedLater {
					base = template.Spec.TCPPort
				}
				host.TCPPort = base + int32(host.Address.ClusterScopeIndex)
			}
			if host.HTTPPort == chPortNumberMustBeAssignedLater {
				base := chDefaultHTTPPortNumber
				if template.Spec.HTTPPort != chPortNumberMustBeAssignedLater {
					base = template.Spec.HTTPPort
				}
				host.HTTPPort = base + int32(host.Address.ClusterScopeIndex)
			}
			if host.InterserverHTTPPort == chPortNumberMustBeAssignedLater {
				base := chDefaultInterserverHTTPPortNumber
				if template.Spec.InterserverHTTPPort != chPortNumberMustBeAssignedLater {
					base = template.Spec.InterserverHTTPPort
				}
				host.InterserverHTTPPort = base + int32(host.Address.ClusterScopeIndex)
			}
		}
	}

	hostApplyPortsFromSettings(host)

	host.InheritTemplatesFrom(nil, nil, template)
}

// hostApplyPortsFromSettings
func hostApplyPortsFromSettings(host *chiV1.ChiHost) {
	settings := host.GetSettings()
	ensurePortValue(&host.TCPPort, settings.GetTCPPort(), chDefaultTCPPortNumber)
	ensurePortValue(&host.HTTPPort, settings.GetHTTPPort(), chDefaultHTTPPortNumber)
	ensurePortValue(&host.InterserverHTTPPort, settings.GetInterserverHTTPPort(), chDefaultInterserverHTTPPortNumber)
}

// ensurePortValue
func ensurePortValue(port *int32, settings, _default int32) {
	if *port != chPortNumberMustBeAssignedLater {
		// Port has a value already
		return
	}

	// Port has no value, let's assign value from settings

	if settings != chPortNumberMustBeAssignedLater {
		// Settings gas a value, use it
		*port = settings
		return
	}

	// Port has no value, settings has no value, fallback to default value
	*port = _default
}

// fillStatus fills .status section of a CHI with values based on current CHI
func (n *Normalizer) fillStatus() {
	endpoint := CreateCHIServiceFQDN(n.chi)
	pods := make([]string, 0)
	fqdns := make([]string, 0)
	n.chi.WalkHosts(func(host *chiV1.ChiHost) error {
		pods = append(pods, CreatePodName(host))
		fqdns = append(fqdns, CreatePodFQDN(host))
		return nil
	})
	// Spam normalized config in high-verbose modes only
	normalized := false
	if v, err := n.chop.Config().GetLogLevel(); (err == nil) && (v >= 3) {
		normalized = true
	}
	n.chi.FillStatus(endpoint, pods, fqdns, normalized)
}

// normalizeStop normalizes .spec.stop
func (n *Normalizer) normalizeStop(stop string) string {
	if util.IsStringBool(stop) {
		// It is bool, use as it is
		return stop
	}

	// In case it is unknown value - just use set it to false
	return util.StringBoolFalseLowercase
}

// normalizeNamespaceDomainPattern normalizes .spec.namespaceDomainPattern
func (n *Normalizer) normalizeNamespaceDomainPattern(namespaceDomainPattern string) string {
	if strings.Count(namespaceDomainPattern, "%s") > 1 {
		return ""
	}
	return namespaceDomainPattern
}

// normalizeDefaults normalizes .spec.defaults
func (n *Normalizer) normalizeDefaults(defaults *chiV1.ChiDefaults) *chiV1.ChiDefaults {
	if defaults == nil {
		defaults = chiV1.NewChiDefaults()
	}
	// Set defaults for CHI object properties
	defaults.ReplicasUseFQDN = util.CastStringBoolToStringTrueFalse(defaults.ReplicasUseFQDN, false)
	// Ensure field
	if defaults.DistributedDDL == nil {
		defaults.DistributedDDL = chiV1.NewChiDistributedDDL()
	}
	// Ensure field
	if defaults.Templates == nil {
		//defaults.Templates = chiV1.NewChiTemplateNames()
	}
	defaults.Templates.HandleDeprecatedFields()
	return defaults
}

// normalizeConfiguration normalizes .spec.configuration
func (n *Normalizer) normalizeConfiguration(conf *chiV1.Configuration) *chiV1.Configuration {
	if conf == nil {
		conf = chiV1.NewConfiguration()
	}
	conf.Zookeeper = n.normalizeConfigurationZookeeper(conf.Zookeeper)
	conf.Users = n.normalizeConfigurationUsers(conf.Users)
	conf.Profiles = n.normalizeConfigurationProfiles(conf.Profiles)
	conf.Quotas = n.normalizeConfigurationQuotas(conf.Quotas)
	conf.Settings = n.normalizeConfigurationSettings(conf.Settings)
	conf.Files = n.normalizeConfigurationFiles(conf.Files)
	conf.Clusters = n.normalizeClusters(conf.Clusters)
	return conf
}

// normalizeTemplates normalizes .spec.templates
func (n *Normalizer) normalizeTemplates(templates *chiV1.ChiTemplates) *chiV1.ChiTemplates {
	if templates == nil {
		templates = chiV1.NewChiTemplates()
	}

	for i := range templates.HostTemplates {
		hostTemplate := &templates.HostTemplates[i]
		n.normalizeHostTemplate(hostTemplate)
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

// normalizeTemplating normalizes .spec.templating
func (n *Normalizer) normalizeTemplating(templating *chiV1.ChiTemplating) *chiV1.ChiTemplating {
	if templating == nil {
		templating = chiV1.NewChiTemplating()
	}
	switch strings.ToLower(templating.GetPolicy()) {
	case
		chiV1.TemplatingPolicyManual,
		chiV1.TemplatingPolicyAuto:
		templating.SetPolicy(strings.ToLower(templating.GetPolicy()))
	default:
		templating.SetPolicy(strings.ToLower(chiV1.TemplatingPolicyManual))
	}
	return templating
}

// normalizeReconciling normalizes .spec.reconciling
func (n *Normalizer) normalizeReconciling(reconciling *chiV1.ChiReconciling) *chiV1.ChiReconciling {
	if reconciling == nil {
		reconciling = chiV1.NewChiReconciling()
	}
	switch strings.ToLower(reconciling.GetPolicy()) {
	case
		chiV1.ReconcilingPolicyWait,
		chiV1.ReconcilingPolicyNoWait:
		reconciling.SetPolicy(strings.ToLower(reconciling.GetPolicy()))
	default:
		reconciling.SetPolicy(strings.ToLower(chiV1.ReconcilingPolicyUnspecified))
	}
	return reconciling
}

// normalizeHostTemplate normalizes .spec.templates.hostTemplates
func (n *Normalizer) normalizeHostTemplate(template *chiV1.ChiHostTemplate) {
	// Name

	// PortDistribution

	if template.PortDistribution == nil {
		// In case no PortDistribution provided - setup default one
		template.PortDistribution = []chiV1.ChiPortDistribution{
			{Type: chiV1.PortDistributionUnspecified},
		}
	}
	// Normalize PortDistribution
	for i := range template.PortDistribution {
		portDistribution := &template.PortDistribution[i]
		switch portDistribution.Type {
		case
			chiV1.PortDistributionUnspecified,
			chiV1.PortDistributionClusterScopeIndex:
			// distribution is known
		default:
			// distribution is not known
			portDistribution.Type = chiV1.PortDistributionUnspecified
		}
	}

	// Spec
	n.normalizeHostTemplateSpec(&template.Spec)

	// Introduce HostTemplate into Index
	// Ensure map is in place
	if n.chi.Spec.Templates.HostTemplatesIndex == nil {
		n.chi.Spec.Templates.HostTemplatesIndex = make(map[string]*chiV1.ChiHostTemplate)
	}

	n.chi.Spec.Templates.HostTemplatesIndex[template.Name] = template
}

// normalizePodTemplate normalizes .spec.templates.podTemplates
func (n *Normalizer) normalizePodTemplate(template *chiV1.ChiPodTemplate) {
	// Name

	// Zone
	if len(template.Zone.Values) == 0 {
		// In case no values specified - no key is reasonable
		template.Zone.Key = ""
	} else if template.Zone.Key == "" {
		// We have values specified, but no key
		// Use default zone key in this case
		template.Zone.Key = "failure-domain.beta.kubernetes.io/zone"
	} else {
		// We have both key and value(s) specified explicitly
	}

	// Distribution
	if template.Distribution == chiV1.PodDistributionOnePerHost {
		// Known distribution, all is fine
	} else {
		// Default Pod Distribution
		template.Distribution = chiV1.PodDistributionUnspecified
	}

	// PodDistribution
	for i := range template.PodDistribution {
		podDistribution := &template.PodDistribution[i]
		switch podDistribution.Type {
		case
			chiV1.PodDistributionUnspecified,

			// AntiAffinity section
			chiV1.PodDistributionClickHouseAntiAffinity,
			chiV1.PodDistributionShardAntiAffinity,
			chiV1.PodDistributionReplicaAntiAffinity:
			if podDistribution.Scope == "" {
				podDistribution.Scope = chiV1.PodDistributionScopeCluster
			}
		case
			chiV1.PodDistributionAnotherNamespaceAntiAffinity,
			chiV1.PodDistributionAnotherClickHouseInstallationAntiAffinity,
			chiV1.PodDistributionAnotherClusterAntiAffinity:
			// PodDistribution is known
		case
			chiV1.PodDistributionMaxNumberPerNode:
			// PodDistribution is known
			if podDistribution.Number < 0 {
				podDistribution.Number = 0
			}
		case
			// Affinity section
			chiV1.PodDistributionNamespaceAffinity,
			chiV1.PodDistributionClickHouseInstallationAffinity,
			chiV1.PodDistributionClusterAffinity,
			chiV1.PodDistributionShardAffinity,
			chiV1.PodDistributionReplicaAffinity,
			chiV1.PodDistributionPreviousTailAffinity:
			// PodDistribution is known

		case chiV1.PodDistributionCircularReplication:
			// Shortcut section
			// All shortcuts have to be expanded

			// PodDistribution is known

			if podDistribution.Scope == "" {
				podDistribution.Scope = chiV1.PodDistributionScopeCluster
			}

			// TODO need to support multi-cluster
			cluster := n.chi.Spec.Configuration.Clusters[0]

			template.PodDistribution = append(template.PodDistribution, chiV1.ChiPodDistribution{
				Type:  chiV1.PodDistributionShardAntiAffinity,
				Scope: podDistribution.Scope,
			})
			template.PodDistribution = append(template.PodDistribution, chiV1.ChiPodDistribution{
				Type:  chiV1.PodDistributionReplicaAntiAffinity,
				Scope: podDistribution.Scope,
			})
			template.PodDistribution = append(template.PodDistribution, chiV1.ChiPodDistribution{
				Type:   chiV1.PodDistributionMaxNumberPerNode,
				Scope:  podDistribution.Scope,
				Number: cluster.Layout.ReplicasCount,
			})

			template.PodDistribution = append(template.PodDistribution, chiV1.ChiPodDistribution{
				Type: chiV1.PodDistributionPreviousTailAffinity,
			})

			template.PodDistribution = append(template.PodDistribution, chiV1.ChiPodDistribution{
				Type: chiV1.PodDistributionNamespaceAffinity,
			})
			template.PodDistribution = append(template.PodDistribution, chiV1.ChiPodDistribution{
				Type: chiV1.PodDistributionClickHouseInstallationAffinity,
			})
			template.PodDistribution = append(template.PodDistribution, chiV1.ChiPodDistribution{
				Type: chiV1.PodDistributionClusterAffinity,
			})

		default:
			// PodDistribution is not known
			podDistribution.Type = chiV1.PodDistributionUnspecified
		}
	}

	// Spec
	template.Spec.Affinity = n.mergeAffinity(template.Spec.Affinity, n.newAffinity(template))

	// In case we have hostNetwork specified, we need to have ClusterFirstWithHostNet DNS policy, because of
	// https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/#pod-s-dns-policy
	// which tells:  For Pods running with hostNetwork, you should explicitly set its DNS policy “ClusterFirstWithHostNet”.
	if template.Spec.HostNetwork {
		template.Spec.DNSPolicy = v1.DNSClusterFirstWithHostNet
	}

	// Introduce PodTemplate into Index
	// Ensure map is in place
	if n.chi.Spec.Templates.PodTemplatesIndex == nil {
		n.chi.Spec.Templates.PodTemplatesIndex = make(map[string]*chiV1.ChiPodTemplate)
	}

	n.chi.Spec.Templates.PodTemplatesIndex[template.Name] = template
}

// newAffinity
func (n *Normalizer) newAffinity(template *chiV1.ChiPodTemplate) *v1.Affinity {
	nodeAffinity := n.newNodeAffinity(template)
	podAffinity := n.newPodAffinity(template)
	podAntiAffinity := n.newPodAntiAffinity(template)

	if (nodeAffinity == nil) && (podAffinity == nil) && (podAntiAffinity == nil) {
		// Neither Affinity nor AntiAffinity specified
		return nil
	}

	return &v1.Affinity{
		NodeAffinity:    nodeAffinity,
		PodAffinity:     podAffinity,
		PodAntiAffinity: podAntiAffinity,
	}
}

// mergeAffinity
func (n *Normalizer) mergeAffinity(dst *v1.Affinity, src *v1.Affinity) *v1.Affinity {
	if src == nil {
		// Nothing to merge from
		return dst
	}

	if dst == nil {
		// No receiver, allocate new one
		dst = &v1.Affinity{}
	}

	dst.NodeAffinity = n.mergeNodeAffinity(dst.NodeAffinity, src.NodeAffinity)
	dst.PodAffinity = n.mergePodAffinity(dst.PodAffinity, src.PodAffinity)
	dst.PodAntiAffinity = n.mergePodAntiAffinity(dst.PodAntiAffinity, src.PodAntiAffinity)

	return dst
}

// newNodeAffinity
func (n *Normalizer) newNodeAffinity(template *chiV1.ChiPodTemplate) *v1.NodeAffinity {
	if template.Zone.Key == "" {
		return nil
	}

	return &v1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
			NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					// A list of node selector requirements by node's labels.
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      template.Zone.Key,
							Operator: v1.NodeSelectorOpIn,
							Values:   template.Zone.Values,
						},
					},
					// A list of node selector requirements by node's fields.
					//MatchFields: []v1.NodeSelectorRequirement{
					//	v1.NodeSelectorRequirement{},
					//},
				},
			},
		},

		PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{},
	}
}

// mergeNodeAffinity
func (n *Normalizer) mergeNodeAffinity(dst *v1.NodeAffinity, src *v1.NodeAffinity) *v1.NodeAffinity {
	if src == nil {
		// Nothing to merge from
		return dst
	}

	if dst == nil {
		// No receiver, allocate new one
		dst = &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{},
			},
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{},
		}
	}

	// Merge NodeSelectors
	for i := range src.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		s := &src.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[i]
		equal := false
		for j := range dst.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
			d := &dst.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[j]
			if _, equal = messagediff.DeepDiff(*s, *d); equal {
				break
			}
		}
		if !equal {
			dst.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = append(
				dst.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms,
				src.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[i],
			)
		}
	}

	// Merge PreferredSchedulingTerm
	for i := range src.PreferredDuringSchedulingIgnoredDuringExecution {
		s := &src.PreferredDuringSchedulingIgnoredDuringExecution[i]
		equal := false
		for j := range dst.PreferredDuringSchedulingIgnoredDuringExecution {
			d := &dst.PreferredDuringSchedulingIgnoredDuringExecution[j]
			if _, equal = messagediff.DeepDiff(*s, *d); equal {
				break
			}
		}
		if !equal {
			dst.PreferredDuringSchedulingIgnoredDuringExecution = append(
				dst.PreferredDuringSchedulingIgnoredDuringExecution,
				src.PreferredDuringSchedulingIgnoredDuringExecution[i],
			)
		}
	}

	return dst
}

// newPodAffinity
func (n *Normalizer) newPodAffinity(template *chiV1.ChiPodTemplate) *v1.PodAffinity {
	podAffinity := &v1.PodAffinity{}

	for i := range template.PodDistribution {
		podDistribution := &template.PodDistribution[i]
		switch podDistribution.Type {
		case chiV1.PodDistributionNamespaceAffinity:
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = n.addWeightedPodAffinityTermWithMatchLabels(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				1,
				map[string]string{
					LabelNamespace: macrosNamespace,
				},
			)
		case chiV1.PodDistributionClickHouseInstallationAffinity:
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = n.addWeightedPodAffinityTermWithMatchLabels(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				1,
				map[string]string{
					LabelCHIName: macrosChiName,
				},
			)
		case chiV1.PodDistributionClusterAffinity:
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = n.addWeightedPodAffinityTermWithMatchLabels(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				1,
				map[string]string{
					LabelClusterName: macrosClusterName,
				},
			)
		case chiV1.PodDistributionShardAffinity:
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = n.addWeightedPodAffinityTermWithMatchLabels(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				1,
				map[string]string{
					LabelShardName: macrosShardName,
				},
			)
		case chiV1.PodDistributionReplicaAffinity:
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = n.addWeightedPodAffinityTermWithMatchLabels(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				1,
				map[string]string{
					LabelReplicaName: macrosReplicaName,
				},
			)
		case chiV1.PodDistributionPreviousTailAffinity:
			// Newer k8s insists on Required for this Affinity
			podAffinity.RequiredDuringSchedulingIgnoredDuringExecution = n.addPodAffinityTermWithMatchLabels(
				podAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				map[string]string{
					LabelClusterScopeIndex: macrosClusterScopeCycleHeadPointsToPreviousCycleTail,
				},
			)
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = n.addWeightedPodAffinityTermWithMatchLabels(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				1,
				map[string]string{
					LabelClusterScopeIndex: macrosClusterScopeCycleHeadPointsToPreviousCycleTail,
				},
			)
		}
	}

	if len(podAffinity.PreferredDuringSchedulingIgnoredDuringExecution) > 0 {
		// Has something to return
		return podAffinity
	}

	return nil
}

// mergePodAffinity
func (n *Normalizer) mergePodAffinity(dst *v1.PodAffinity, src *v1.PodAffinity) *v1.PodAffinity {
	if src == nil {
		// Nothing to merge from
		return dst
	}

	if dst == nil {
		// No receiver, allocate new one
		dst = &v1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution:  []v1.PodAffinityTerm{},
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{},
		}
	}

	// Merge PodAffinityTerm
	for i := range src.RequiredDuringSchedulingIgnoredDuringExecution {
		s := &src.RequiredDuringSchedulingIgnoredDuringExecution[i]
		equal := false
		for j := range dst.RequiredDuringSchedulingIgnoredDuringExecution {
			d := &dst.RequiredDuringSchedulingIgnoredDuringExecution[j]
			if _, equal = messagediff.DeepDiff(*s, *d); equal {
				break
			}
		}
		if !equal {
			dst.RequiredDuringSchedulingIgnoredDuringExecution = append(
				dst.RequiredDuringSchedulingIgnoredDuringExecution,
				src.RequiredDuringSchedulingIgnoredDuringExecution[i],
			)
		}
	}

	// Merge WeightedPodAffinityTerm
	for i := range src.PreferredDuringSchedulingIgnoredDuringExecution {
		s := &src.PreferredDuringSchedulingIgnoredDuringExecution[i]
		equal := false
		for j := range dst.PreferredDuringSchedulingIgnoredDuringExecution {
			d := &dst.PreferredDuringSchedulingIgnoredDuringExecution[j]
			if _, equal = messagediff.DeepDiff(*s, *d); equal {
				break
			}
		}
		if !equal {
			dst.PreferredDuringSchedulingIgnoredDuringExecution = append(
				dst.PreferredDuringSchedulingIgnoredDuringExecution,
				src.PreferredDuringSchedulingIgnoredDuringExecution[i],
			)
		}
	}

	return dst
}

// newMatchLabels
func (n *Normalizer) newMatchLabels(
	podDistribution *chiV1.ChiPodDistribution,
	matchLabels map[string]string,
) map[string]string {
	var scopeLabels map[string]string

	switch podDistribution.Scope {
	case chiV1.PodDistributionScopeShard:
		scopeLabels = map[string]string{
			LabelNamespace:   macrosNamespace,
			LabelCHIName:     macrosChiName,
			LabelClusterName: macrosClusterName,
			LabelShardName:   macrosShardName,
		}
	case chiV1.PodDistributionScopeReplica:
		scopeLabels = map[string]string{
			LabelNamespace:   macrosNamespace,
			LabelCHIName:     macrosChiName,
			LabelClusterName: macrosClusterName,
			LabelReplicaName: macrosReplicaName,
		}
	case chiV1.PodDistributionScopeCluster:
		scopeLabels = map[string]string{
			LabelNamespace:   macrosNamespace,
			LabelCHIName:     macrosChiName,
			LabelClusterName: macrosClusterName,
		}
	case chiV1.PodDistributionScopeClickHouseInstallation:
		scopeLabels = map[string]string{
			LabelNamespace: macrosNamespace,
			LabelCHIName:   macrosChiName,
		}
	case chiV1.PodDistributionScopeNamespace:
		scopeLabels = map[string]string{
			LabelNamespace: macrosNamespace,
		}
	case chiV1.PodDistributionScopeGlobal:
		scopeLabels = map[string]string{}
	}

	return util.MergeStringMapsOverwrite(matchLabels, scopeLabels)
}

// newPodAntiAffinity
func (n *Normalizer) newPodAntiAffinity(template *chiV1.ChiPodTemplate) *v1.PodAntiAffinity {
	podAntiAffinity := &v1.PodAntiAffinity{}

	// Distribution
	// DEPRECATED
	if template.Distribution == chiV1.PodDistributionOnePerHost {
		podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = n.addPodAffinityTermWithMatchLabels(
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
			map[string]string{
				LabelAppName: LabelAppValue,
			},
		)
	}

	// PodDistribution
	for i := range template.PodDistribution {
		podDistribution := &template.PodDistribution[i]
		switch podDistribution.Type {
		case chiV1.PodDistributionClickHouseAntiAffinity:
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = n.addPodAffinityTermWithMatchLabels(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				n.newMatchLabels(
					podDistribution,
					map[string]string{
						LabelAppName: LabelAppValue,
					},
				),
			)
		case chiV1.PodDistributionMaxNumberPerNode:
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = n.addPodAffinityTermWithMatchLabels(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				n.newMatchLabels(
					podDistribution,
					map[string]string{
						LabelClusterScopeCycleIndex: macrosClusterScopeCycleIndex,
					},
				),
			)
		case chiV1.PodDistributionShardAntiAffinity:
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = n.addPodAffinityTermWithMatchLabels(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				n.newMatchLabels(
					podDistribution,
					map[string]string{
						LabelShardName: macrosShardName,
					},
				),
			)
		case chiV1.PodDistributionReplicaAntiAffinity:
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = n.addPodAffinityTermWithMatchLabels(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				n.newMatchLabels(
					podDistribution,
					map[string]string{
						LabelReplicaName: macrosReplicaName,
					},
				),
			)
		case chiV1.PodDistributionAnotherNamespaceAntiAffinity:
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = n.addPodAffinityTermWithMatchExpressions(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				[]metaV1.LabelSelectorRequirement{
					{
						Key:      LabelNamespace,
						Operator: metaV1.LabelSelectorOpNotIn,
						Values: []string{
							macrosNamespace,
						},
					},
				},
			)
		case chiV1.PodDistributionAnotherClickHouseInstallationAntiAffinity:
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = n.addPodAffinityTermWithMatchExpressions(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				[]metaV1.LabelSelectorRequirement{
					{
						Key:      LabelCHIName,
						Operator: metaV1.LabelSelectorOpNotIn,
						Values: []string{
							macrosChiName,
						},
					},
				},
			)
		case chiV1.PodDistributionAnotherClusterAntiAffinity:
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = n.addPodAffinityTermWithMatchExpressions(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				[]metaV1.LabelSelectorRequirement{
					{
						Key:      LabelClusterName,
						Operator: metaV1.LabelSelectorOpNotIn,
						Values: []string{
							macrosClusterName,
						},
					},
				},
			)
		}
	}

	if len(podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) > 0 {
		// Has something to return
		return podAntiAffinity
	}

	return nil
}

// mergePodAntiAffinity
func (n *Normalizer) mergePodAntiAffinity(dst *v1.PodAntiAffinity, src *v1.PodAntiAffinity) *v1.PodAntiAffinity {
	if src == nil {
		// Nothing to merge from
		return dst
	}

	if dst == nil {
		// No receiver, allocate new one
		dst = &v1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution:  []v1.PodAffinityTerm{},
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{},
		}
	}

	// Merge PodAffinityTerm
	for i := range src.RequiredDuringSchedulingIgnoredDuringExecution {
		s := &src.RequiredDuringSchedulingIgnoredDuringExecution[i]
		equal := false
		for j := range dst.RequiredDuringSchedulingIgnoredDuringExecution {
			d := &dst.RequiredDuringSchedulingIgnoredDuringExecution[j]
			if _, equal = messagediff.DeepDiff(*s, *d); equal {
				break
			}
		}
		if !equal {
			dst.RequiredDuringSchedulingIgnoredDuringExecution = append(
				dst.RequiredDuringSchedulingIgnoredDuringExecution,
				src.RequiredDuringSchedulingIgnoredDuringExecution[i],
			)
		}
	}

	// Merge WeightedPodAffinityTerm
	for i := range src.PreferredDuringSchedulingIgnoredDuringExecution {
		s := &src.PreferredDuringSchedulingIgnoredDuringExecution[i]
		equal := false
		for j := range dst.PreferredDuringSchedulingIgnoredDuringExecution {
			d := &dst.PreferredDuringSchedulingIgnoredDuringExecution[j]
			if _, equal = messagediff.DeepDiff(*s, *d); equal {
				break
			}
		}
		if !equal {
			dst.PreferredDuringSchedulingIgnoredDuringExecution = append(
				dst.PreferredDuringSchedulingIgnoredDuringExecution,
				src.PreferredDuringSchedulingIgnoredDuringExecution[i],
			)
		}
	}

	return dst
}

// addPodAffinityTermWithMatchLabels
func (n *Normalizer) addPodAffinityTermWithMatchLabels(terms []v1.PodAffinityTerm, matchLabels map[string]string) []v1.PodAffinityTerm {
	return append(terms,
		v1.PodAffinityTerm{
			LabelSelector: &metaV1.LabelSelector{
				// A list of node selector requirements by node's labels.
				//MatchLabels: map[string]string{
				//	LabelClusterScopeCycleIndex: macrosClusterScopeCycleIndex,
				//},
				MatchLabels: matchLabels,
				// Switch to MatchLabels
				//MatchExpressions: []metaV1.LabelSelectorRequirement{
				//	{
				//		Key:      LabelAppName,
				//		Operator: metaV1.LabelSelectorOpIn,
				//		Values: []string{
				//			LabelAppValue,
				//		},
				//	},
				//},
			},
			TopologyKey: "kubernetes.io/hostname",
		},
	)
}

// addPodAffinityTermWithMatchExpressions
func (n *Normalizer) addPodAffinityTermWithMatchExpressions(terms []v1.PodAffinityTerm, matchExpressions []metaV1.LabelSelectorRequirement) []v1.PodAffinityTerm {
	return append(terms,
		v1.PodAffinityTerm{
			LabelSelector: &metaV1.LabelSelector{
				// A list of node selector requirements by node's labels.
				//MatchLabels: map[string]string{
				//	LabelClusterScopeCycleIndex: macrosClusterScopeCycleIndex,
				//},
				//MatchExpressions: []metaV1.LabelSelectorRequirement{
				//	{
				//		Key:      LabelAppName,
				//		Operator: metaV1.LabelSelectorOpIn,
				//		Values: []string{
				//			LabelAppValue,
				//		},
				//	},
				//},
				MatchExpressions: matchExpressions,
			},
			TopologyKey: "kubernetes.io/hostname",
		},
	)
}

// addWeightedPodAffinityTermWithMatchLabels
func (n *Normalizer) addWeightedPodAffinityTermWithMatchLabels(
	terms []v1.WeightedPodAffinityTerm,
	weight int32,
	matchLabels map[string]string,
) []v1.WeightedPodAffinityTerm {
	return append(terms,
		v1.WeightedPodAffinityTerm{
			Weight: weight,
			PodAffinityTerm: v1.PodAffinityTerm{
				LabelSelector: &metaV1.LabelSelector{
					// A list of node selector requirements by node's labels.
					//MatchLabels: map[string]string{
					//	LabelClusterScopeCycleIndex: macrosClusterScopeCycleIndex,
					//},
					MatchLabels: matchLabels,
					// Switch to MatchLabels
					//MatchExpressions: []metaV1.LabelSelectorRequirement{
					//	{
					//		Key:      LabelAppName,
					//		Operator: metaV1.LabelSelectorOpIn,
					//		Values: []string{
					//			LabelAppValue,
					//		},
					//	},
					//},
				},
				TopologyKey: "kubernetes.io/hostname",
			},
		},
	)
}

// normalizeVolumeClaimTemplate normalizes .spec.templates.volumeClaimTemplates
func (n *Normalizer) normalizeVolumeClaimTemplate(template *chiV1.ChiVolumeClaimTemplate) {
	// Check name
	// Check PVCReclaimPolicy
	if !template.PVCReclaimPolicy.IsValid() {
		template.PVCReclaimPolicy = chiV1.PVCReclaimPolicyDelete
	}
	// Check Spec

	// Ensure map is in place
	if n.chi.Spec.Templates.VolumeClaimTemplatesIndex == nil {
		n.chi.Spec.Templates.VolumeClaimTemplatesIndex = make(map[string]*chiV1.ChiVolumeClaimTemplate)
	}
	n.chi.Spec.Templates.VolumeClaimTemplatesIndex[template.Name] = template
}

// normalizeServiceTemplate normalizes .spec.templates.serviceTemplates
func (n *Normalizer) normalizeServiceTemplate(template *chiV1.ChiServiceTemplate) {
	// Check name
	// Check GenerateName
	// Check ObjectMeta
	// Check Spec

	// Ensure map is in place
	if n.chi.Spec.Templates.ServiceTemplatesIndex == nil {
		n.chi.Spec.Templates.ServiceTemplatesIndex = make(map[string]*chiV1.ChiServiceTemplate)
	}
	n.chi.Spec.Templates.ServiceTemplatesIndex[template.Name] = template
}

// normalizeUseTemplates normalizes .spec.useTemplates
func (n *Normalizer) normalizeUseTemplates(useTemplates []chiV1.ChiUseTemplate) []chiV1.ChiUseTemplate {
	for i := range useTemplates {
		useTemplate := &useTemplates[i]
		n.normalizeUseTemplate(useTemplate)
	}
	return useTemplates
}

// normalizeUseTemplate normalizes ChiUseTemplate
func (n *Normalizer) normalizeUseTemplate(useTemplate *chiV1.ChiUseTemplate) {
	// Check Name
	if useTemplate.Name == "" {
		// This is strange
	}

	// Check Namespace
	if useTemplate.Namespace == "" {
		// So far do nothing with empty namespace
	}

	// Ensure UseType
	switch useTemplate.UseType {
	case useTypeMerge:
		// Known use type, all is fine, do nothing
	default:
		// Unknown - use default value
		useTemplate.UseType = useTypeMerge
	}
}

// normalizeClusters normalizes clusters
func (n *Normalizer) normalizeClusters(clusters []*chiV1.ChiCluster) []*chiV1.ChiCluster {
	// We need to have at least one cluster available
	clusters = n.ensureCluster(clusters)

	// Normalize all clusters
	for i := range clusters {
		clusters[i] = n.normalizeCluster(clusters[i])
	}

	return clusters
}

// newDefaultCluster
func (n *Normalizer) newDefaultCluster() *chiV1.ChiCluster {
	return &chiV1.ChiCluster{
		Name: "cluster",
	}
}

// ensureCluster
func (n *Normalizer) ensureCluster(clusters []*chiV1.ChiCluster) []*chiV1.ChiCluster {
	if len(clusters) > 0 {
		return clusters
	}

	if n.withDefaultCluster {
		return []*chiV1.ChiCluster{n.newDefaultCluster()}
	}

	return []*chiV1.ChiCluster{}
}

// calcFingerprints calculates fingerprints for ClickHouse configuration data
func (n *Normalizer) calcFingerprints(host *chiV1.ChiHost) error {
	zk := host.GetZookeeper()
	host.Config.ZookeeperFingerprint = util.Fingerprint(zk)

	global := n.chi.Spec.Configuration.Settings.AsSortedSliceOfStrings()
	local := host.Settings.AsSortedSliceOfStrings()
	host.Config.SettingsFingerprint = util.Fingerprint(
		fmt.Sprintf("%s%s",
			util.Fingerprint(global),
			util.Fingerprint(local),
		),
	)
	host.Config.FilesFingerprint = util.Fingerprint(
		fmt.Sprintf("%s%s",
			util.Fingerprint(
				n.chi.Spec.Configuration.Files.Filter(
					nil,
					[]chiV1.SettingsSection{chiV1.SectionUsers},
					true,
				).AsSortedSliceOfStrings(),
			),
			util.Fingerprint(
				host.Files.Filter(
					nil,
					[]chiV1.SettingsSection{chiV1.SectionUsers},
					true,
				).AsSortedSliceOfStrings(),
			),
		),
	)

	return nil
}

// normalizeConfigurationZookeeper normalizes .spec.configuration.zookeeper
func (n *Normalizer) normalizeConfigurationZookeeper(zk *chiV1.ChiZookeeperConfig) *chiV1.ChiZookeeperConfig {
	if zk == nil {
		return nil
	}

	// In case no ZK port specified - assign default
	for i := range zk.Nodes {
		// Convenience wrapper
		node := &zk.Nodes[i]
		if node.Port == 0 {
			node.Port = zkDefaultPort
		}
	}

	// In case no ZK root specified - assign '/clickhouse/{namespace}/{chi name}'
	//if zk.Root == "" {
	//	zk.Root = fmt.Sprintf(zkDefaultRootTemplate, n.chi.Namespace, n.chi.Name)
	//}

	return zk
}

// normalizeConfigurationUsers normalizes .spec.configuration.users
func (n *Normalizer) normalizeConfigurationUsers(users *chiV1.Settings) *chiV1.Settings {
	if users == nil {
		users = chiV1.NewSettings()
	}
	users.Normalize()

	// Extract username from path
	usernameMap := make(map[string]bool)
	users.Walk(func(path string, _ *chiV1.Setting) {
		// Split 'admin/password'
		tags := strings.Split(path, "/")

		// Basic sanity check - need to have at least "username/something" pair
		if len(tags) < 2 {
			// Skip incorrect entry
			return
		}

		username := tags[0]
		usernameMap[username] = true
	})

	// Ensure "must have" sections are in place, which are:
	// 1. user/profile
	// 2. user/quota
	// 3. user/networks/ip and user/networks/host_regexp defaults to the installation pods
	// 4. user/password_sha256_hex

	// "default" user is required in order to secure host_regexp
	usernameMap["default"] = true
	for username := range usernameMap {
		// Ensure 'user/profile' section
		users.SetIfNotExists(username+"/profile", chiV1.NewSettingScalar(n.chop.Config().CHConfigUserDefaultProfile))
		// Ensure 'user/quota' section
		users.SetIfNotExists(username+"/quota", chiV1.NewSettingScalar(n.chop.Config().CHConfigUserDefaultQuota))
		// Ensure 'user/networks/ip' section
		users.SetIfNotExists(username+"/networks/ip", chiV1.NewSettingVector(n.chop.Config().CHConfigUserDefaultNetworksIP))
		// Ensure 'user/networks/host_regexp' section
		users.SetIfNotExists(username+"/networks/host_regexp", chiV1.NewSettingScalar(CreatePodRegexp(n.chi, n.chop.Config().CHConfigNetworksHostRegexpTemplate)))

		var pass = ""
		if users.Has(username + "/password") {
			pass = users.Get(username + "/password").String()
		} else if username != "default" {
			pass = n.chop.Config().CHConfigUserDefaultPassword
		}

		hasPasswordSHA256 := users.Has(username + "/password_sha256_hex")
		hasPasswordDoubleSHA1 := users.Has(username + "/password_double_sha1_hex")

		// if SHA256 or double SHA1 are not set, initialize SHA256 from the password
		if !hasPasswordSHA256 && !hasPasswordDoubleSHA1 && (pass != "") {
			passSHA256 := sha256.Sum256([]byte(pass))
			users.Set(username+"/password_sha256_hex", chiV1.NewSettingScalar(hex.EncodeToString(passSHA256[:])))
			hasPasswordSHA256 = true
		}

		if hasPasswordSHA256 {
			// ClickHouse does not start if both password and sha256 are defined
			if username == "default" {
				// Set remove password flag for default user that is empty in stock ClickHouse users.xml
				// TODO fix it
				users.Set(username+"/password", chiV1.NewSettingScalar("_removed_"))
			} else {
				users.Delete(username + "/password")
			}
		}
	}
	return users
}

// normalizeConfigurationProfiles normalizes .spec.configuration.profiles
func (n *Normalizer) normalizeConfigurationProfiles(profiles *chiV1.Settings) *chiV1.Settings {
	if profiles == nil {
		//profiles = chiV1.NewSettings()
		return nil
	}
	profiles.Normalize()
	return profiles
}

// normalizeConfigurationQuotas normalizes .spec.configuration.quotas
func (n *Normalizer) normalizeConfigurationQuotas(quotas *chiV1.Settings) *chiV1.Settings {
	if quotas == nil {
		//quotas = chiV1.NewSettings()
		return nil
	}
	quotas.Normalize()
	return quotas
}

// normalizeConfigurationSettings normalizes .spec.configuration.settings
func (n *Normalizer) normalizeConfigurationSettings(settings *chiV1.Settings) *chiV1.Settings {
	if settings == nil {
		//settings = chiV1.NewSettings()
		return nil
	}
	settings.Normalize()
	return settings
}

// normalizeConfigurationFiles normalizes .spec.configuration.files
func (n *Normalizer) normalizeConfigurationFiles(files *chiV1.Settings) *chiV1.Settings {
	if files == nil {
		//files = chiV1.NewSettings()
		return nil
	}
	files.Normalize()
	return files
}

// normalizeCluster normalizes cluster and returns deployments usage counters for this cluster
func (n *Normalizer) normalizeCluster(cluster *chiV1.ChiCluster) *chiV1.ChiCluster {
	if cluster == nil {
		cluster = n.newDefaultCluster()
	}

	// Inherit from .spec.configuration.zookeeper
	cluster.InheritZookeeperFrom(n.chi)
	// Inherit from .spec.configuration.files
	cluster.InheritFilesFrom(n.chi)
	// Inherit from .spec.defaults
	cluster.InheritTemplatesFrom(n.chi)

	cluster.Zookeeper = n.normalizeConfigurationZookeeper(cluster.Zookeeper)
	cluster.Settings = n.normalizeConfigurationSettings(cluster.Settings)
	cluster.Files = n.normalizeConfigurationFiles(cluster.Files)

	if cluster.Layout == nil {
		cluster.Layout = chiV1.NewChiClusterLayout()
	}
	cluster.FillShardReplicaSpecified()
	cluster.Layout = n.normalizeClusterLayoutShardsCountAndReplicasCount(cluster.Layout)
	n.ensureClusterLayoutShards(cluster.Layout)
	n.ensureClusterLayoutReplicas(cluster.Layout)

	n.createHostsField(cluster)

	// Loop over all shards and replicas inside shards and fill structure
	cluster.WalkShards(func(index int, shard *chiV1.ChiShard) error {
		n.normalizeShard(shard, cluster, index)
		return nil
	})

	cluster.WalkReplicas(func(index int, replica *chiV1.ChiReplica) error {
		n.normalizeReplica(replica, cluster, index)
		return nil
	})

	cluster.Layout.HostsField.WalkHosts(func(shard, replica int, host *chiV1.ChiHost) error {
		n.normalizeHost(host, cluster.GetShard(shard), cluster.GetReplica(replica), cluster, shard, replica)
		return nil
	})

	return cluster
}

// createHostsField
func (n *Normalizer) createHostsField(cluster *chiV1.ChiCluster) {
	cluster.Layout.HostsField = chiV1.NewHostsField(cluster.Layout.ShardsCount, cluster.Layout.ReplicasCount)

	// Need to migrate hosts from Shards and Replicas into HostsField
	hostMergeFunc := func(shard, replica int, host *chiV1.ChiHost) error {
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

// normalizeClusterLayoutShardsCountAndReplicasCount ensures at least 1 shard and 1 replica counters
func (n *Normalizer) normalizeClusterLayoutShardsCountAndReplicasCount(layout *chiV1.ChiClusterLayout) *chiV1.ChiClusterLayout {
	if layout == nil {
		layout = chiV1.NewChiClusterLayout()
	}

	// Layout.ShardsCount and
	// Layout.ReplicasCount must represent max number of shards and replicas requested respectively

	// Deal with ShardsCount
	if layout.ShardsCount == 0 {
		// No ShardsCount specified - need to figure out

		// We need to have at least one Shard
		layout.ShardsCount = 1

		// Let's look for explicitly specified Shards in Layout.Shards
		if len(layout.Shards) > layout.ShardsCount {
			// We have some Shards specified explicitly
			layout.ShardsCount = len(layout.Shards)
		}

		// Let's look for explicitly specified Shards in Layout.Replicas
		for i := range layout.Replicas {
			replica := &layout.Replicas[i]

			if replica.ShardsCount > layout.ShardsCount {
				// We have Shards number specified explicitly in this replica
				layout.ShardsCount = replica.ShardsCount
			}

			if len(replica.Hosts) > layout.ShardsCount {
				// We have some Shards specified explicitly
				layout.ShardsCount = len(replica.Hosts)
			}
		}
	}

	// Deal with ReplicasCount
	if layout.ReplicasCount == 0 {
		// No ReplicasCount specified - need to figure out

		// We need to have at least one Replica
		layout.ReplicasCount = 1

		// Let's look for explicitly specified Replicas in Layout.Shards
		for i := range layout.Shards {
			shard := &layout.Shards[i]

			if shard.ReplicasCount > layout.ReplicasCount {
				// We have Replicas number specified explicitly in this shard
				layout.ReplicasCount = shard.ReplicasCount
			}

			if len(shard.Hosts) > layout.ReplicasCount {
				// We have some Replicas specified explicitly
				layout.ReplicasCount = len(shard.Hosts)
			}
		}

		// Let's look for explicitly specified Replicas in Layout.Replicas
		if len(layout.Replicas) > layout.ReplicasCount {
			// We have some Replicas specified explicitly
			layout.ReplicasCount = len(layout.Replicas)
		}
	}

	return layout
}

// ensureClusterLayoutShards ensures slice layout.Shards is in place
func (n *Normalizer) ensureClusterLayoutShards(layout *chiV1.ChiClusterLayout) {
	// Disposition of shards in slice would be
	// [explicitly specified shards 0..N, N+1..layout.ShardsCount-1 empty slots for to-be-filled shards]

	// Some (may be all) shards specified, need to append space for unspecified shards
	// TODO may be there is better way to append N slots to a slice
	for len(layout.Shards) < layout.ShardsCount {
		layout.Shards = append(layout.Shards, chiV1.ChiShard{})
	}
}

// ensureClusterLayoutReplicas ensures slice layout.Replicas is in place
func (n *Normalizer) ensureClusterLayoutReplicas(layout *chiV1.ChiClusterLayout) {
	// Disposition of replicas in slice would be
	// [explicitly specified replicas 0..N, N+1..layout.ReplicasCount-1 empty slots for to-be-filled replicas]

	// Some (may be all) replicas specified, need to append space for unspecified replicas
	// TODO may be there is better way to append N slots to a slice
	for len(layout.Replicas) < layout.ReplicasCount {
		layout.Replicas = append(layout.Replicas, chiV1.ChiReplica{})
	}
}

// normalizeShard normalizes a shard - walks over all fields
func (n *Normalizer) normalizeShard(shard *chiV1.ChiShard, cluster *chiV1.ChiCluster, shardIndex int) {
	n.normalizeShardName(shard, shardIndex)
	n.normalizeShardWeight(shard)
	// For each shard of this normalized cluster inherit from cluster
	shard.InheritSettingsFrom(cluster)
	shard.Settings = n.normalizeConfigurationSettings(shard.Settings)
	shard.InheritFilesFrom(cluster)
	shard.Files = n.normalizeConfigurationSettings(shard.Files)
	shard.InheritTemplatesFrom(cluster)
	// Normalize Replicas
	n.normalizeShardReplicasCount(shard, cluster.Layout.ReplicasCount)
	n.normalizeShardHosts(shard, cluster, shardIndex)
	// Internal replication uses ReplicasCount thus it has to be normalized after shard ReplicaCount normalized
	n.normalizeShardInternalReplication(shard)
}

// normalizeReplica normalizes a replica - walks over all fields
func (n *Normalizer) normalizeReplica(replica *chiV1.ChiReplica, cluster *chiV1.ChiCluster, replicaIndex int) {
	n.normalizeReplicaName(replica, replicaIndex)
	// For each replica of this normalized cluster inherit from cluster
	replica.InheritSettingsFrom(cluster)
	replica.Settings = n.normalizeConfigurationSettings(replica.Settings)
	replica.InheritFilesFrom(cluster)
	replica.Files = n.normalizeConfigurationSettings(replica.Files)
	replica.InheritTemplatesFrom(cluster)
	// Normalize Shards
	n.normalizeReplicaShardsCount(replica, cluster.Layout.ShardsCount)
	n.normalizeReplicaHosts(replica, cluster, replicaIndex)
}

// normalizeShardReplicasCount ensures shard.ReplicasCount filled properly
func (n *Normalizer) normalizeShardReplicasCount(shard *chiV1.ChiShard, layoutReplicasCount int) {
	if shard.ReplicasCount > 0 {
		// Shard has explicitly specified number of replicas
		return
	}

	// Here we have shard.ReplicasCount = 0, meaning that
	// shard does not have explicitly specified number of replicas - need to fill it

	// Look for explicitly specified Replicas first
	if len(shard.Hosts) > 0 {
		// We have Replicas specified as a slice and no other replicas count provided,
		// this means we have explicitly specified replicas only and exact ReplicasCount is known
		shard.ReplicasCount = len(shard.Hosts)
		return
	}

	// No shard.ReplicasCount specified, no replicas explicitly provided, so we have to
	// use ReplicasCount from layout
	shard.ReplicasCount = layoutReplicasCount
}

// normalizeReplicaShardsCount ensures replica.ShardsCount filled properly
func (n *Normalizer) normalizeReplicaShardsCount(replica *chiV1.ChiReplica, layoutShardsCount int) {
	if replica.ShardsCount > 0 {
		// Replica has explicitly specified number of shards
		return
	}

	// Here we have replica.ShardsCount = 0, meaning that
	// replica does not have explicitly specified number of shards - need to fill it

	// Look for explicitly specified Shards first
	if len(replica.Hosts) > 0 {
		// We have Shards specified as a slice and no other shards count provided,
		// this means we have explicitly specified shards only and exact ShardsCount is known
		replica.ShardsCount = len(replica.Hosts)
		return
	}

	// No replica.ShardsCount specified, no shards explicitly provided, so we have to
	// use ShardsCount from layout
	replica.ShardsCount = layoutShardsCount
}

// normalizeShardName normalizes shard name
func (n *Normalizer) normalizeShardName(shard *chiV1.ChiShard, index int) {
	if (len(shard.Name) > 0) && !IsAutoGeneratedShardName(shard.Name, shard, index) {
		// Has explicitly specified name already
		return
	}

	shard.Name = CreateShardName(shard, index)
}

// normalizeReplicaName normalizes replica name
func (n *Normalizer) normalizeReplicaName(replica *chiV1.ChiReplica, index int) {
	if (len(replica.Name) > 0) && !IsAutoGeneratedReplicaName(replica.Name, replica, index) {
		// Has explicitly specified name already
		return
	}

	replica.Name = CreateReplicaName(replica, index)
}

// normalizeShardName normalizes shard weight
func (n *Normalizer) normalizeShardWeight(shard *chiV1.ChiShard) {
}

// normalizeShardHosts normalizes all replicas of specified shard
func (n *Normalizer) normalizeShardHosts(shard *chiV1.ChiShard, cluster *chiV1.ChiCluster, shardIndex int) {
	// Use hosts from HostsField
	shard.Hosts = nil
	for len(shard.Hosts) < shard.ReplicasCount {
		// We still have some assumed hosts in this shard - let's add it as replicaIndex
		replicaIndex := len(shard.Hosts)
		// Check whether we have this host in HostsField
		host := cluster.GetOrCreateHost(shardIndex, replicaIndex)
		shard.Hosts = append(shard.Hosts, host)
	}
}

// normalizeReplicaHosts normalizes all replicas of specified shard
func (n *Normalizer) normalizeReplicaHosts(replica *chiV1.ChiReplica, cluster *chiV1.ChiCluster, replicaIndex int) {
	// Use hosts from HostsField
	replica.Hosts = nil
	for len(replica.Hosts) < replica.ShardsCount {
		// We still have some assumed hosts in this replica - let's add it as shardIndex
		shardIndex := len(replica.Hosts)
		// Check whether we have this host in HostsField
		host := cluster.Layout.HostsField.GetOrCreate(shardIndex, replicaIndex)
		replica.Hosts = append(replica.Hosts, host)
	}
}

// normalizeHost normalizes a host/replica
func (n *Normalizer) normalizeHost(
	host *chiV1.ChiHost,
	shard *chiV1.ChiShard,
	replica *chiV1.ChiReplica,
	cluster *chiV1.ChiCluster,
	shardIndex int,
	replicaIndex int,
) {
	n.normalizeHostName(host, shard, shardIndex, replica, replicaIndex)
	n.normalizeHostPorts(host)
	// Inherit from either Shard or Replica
	var s *chiV1.ChiShard
	var r *chiV1.ChiReplica
	if cluster.IsShardSpecified() {
		s = shard
	} else {
		r = replica
	}
	host.InheritSettingsFrom(s, r)
	host.Settings = n.normalizeConfigurationSettings(host.Settings)
	host.InheritFilesFrom(s, r)
	host.Files = n.normalizeConfigurationSettings(host.Files)
	host.InheritTemplatesFrom(s, r, nil)
}

// normalizeHostTemplateSpec is the same as normalizeHost but for a template
func (n *Normalizer) normalizeHostTemplateSpec(host *chiV1.ChiHost) {
	n.normalizeHostPorts(host)
}

// normalizeHostName normalizes host's name
func (n *Normalizer) normalizeHostName(
	host *chiV1.ChiHost,
	shard *chiV1.ChiShard,
	shardIndex int,
	replica *chiV1.ChiReplica,
	replicaIndex int,
) {
	if (len(host.Name) > 0) && !IsAutoGeneratedHostName(host.Name, host, shard, shardIndex, replica, replicaIndex) {
		// Has explicitly specified name already
		return
	}

	host.Name = CreateHostName(host, shard, shardIndex, replica, replicaIndex)
}

// normalizeHostPorts ensures chiV1.ChiReplica.Port is reasonable
func (n *Normalizer) normalizeHostPorts(host *chiV1.ChiHost) {
	// Deprecated
	if (host.Port <= 0) || (host.Port >= 65535) {
		host.Port = chPortNumberMustBeAssignedLater
	}

	if (host.TCPPort <= 0) || (host.TCPPort >= 65535) {
		host.TCPPort = chPortNumberMustBeAssignedLater
	}

	if (host.HTTPPort <= 0) || (host.HTTPPort >= 65535) {
		host.HTTPPort = chPortNumberMustBeAssignedLater
	}

	if (host.InterserverHTTPPort <= 0) || (host.InterserverHTTPPort >= 65535) {
		host.InterserverHTTPPort = chPortNumberMustBeAssignedLater
	}
}

// normalizeShardInternalReplication ensures reasonable values in
// .spec.configuration.clusters.layout.shards.internalReplication
func (n *Normalizer) normalizeShardInternalReplication(shard *chiV1.ChiShard) {
	// Shards with replicas are expected to have internal replication on by default
	defaultInternalReplication := false
	if shard.ReplicasCount > 1 {
		defaultInternalReplication = true
	}
	shard.InternalReplication = util.CastStringBoolToStringTrueFalse(shard.InternalReplication, defaultInternalReplication)
}
