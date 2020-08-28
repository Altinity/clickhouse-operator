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

	log "github.com/golang/glog"
	// log "k8s.io/klog"

	"gopkg.in/d4l3k/messagediff.v1"
	"k8s.io/api/core/v1"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Normalizer
type Normalizer struct {
	chop *chop.CHOp
	chi  *chiv1.ClickHouseInstallation
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
func (n *Normalizer) CreateTemplatedCHI(chi *chiv1.ClickHouseInstallation, withDefaultCluster bool) (*chiv1.ClickHouseInstallation, error) {
	// Whether should insert default cluster if no cluster specified
	n.withDefaultCluster = withDefaultCluster

	// What base should be used to create CHI
	if n.chop.Config().CHITemplate == nil {
		// No template specified - start with clear page
		n.chi = new(chiv1.ClickHouseInstallation)
	} else {
		// Template specified - start with template
		n.chi = n.chop.Config().CHITemplate.DeepCopy()
	}

	// At this moment n.chi is either empty CHI or a system-wide template
	// We need to apply templates

	// Apply CHOP-specified templates
	// TODO

	// Apply CHI-specified templates

	var useTemplates []chiv1.ChiUseTemplate
	if len(chi.Spec.UseTemplates) > 0 {
		useTemplates = make([]chiv1.ChiUseTemplate, len(chi.Spec.UseTemplates))
		copy(useTemplates, chi.Spec.UseTemplates)

		// UseTemplates must contain reasonable data, thus has to be normalized
		n.normalizeUseTemplates(&useTemplates)
	}

	for i := range useTemplates {
		useTemplate := &useTemplates[i]
		if template := n.chop.Config().FindTemplate(useTemplate, chi.Namespace); template == nil {
			log.V(1).Infof("UNABLE to find template %s/%s referenced in useTemplates. Skip it.", useTemplate.Namespace, useTemplate.Name)
		} else {
			(&n.chi.Spec).MergeFrom(&template.Spec, chiv1.MergeTypeOverrideByNonEmptyValues)
			log.V(2).Infof("Merge template %s/%s referenced in useTemplates", useTemplate.Namespace, useTemplate.Name)
		}
	}

	// After all templates applied, place provided CHI on top of the whole stack
	n.chi.MergeFrom(chi, chiv1.MergeTypeOverrideByNonEmptyValues)

	return n.NormalizeCHI(nil)
}

// NormalizeCHI normalizes CHI.
// Returns normalized CHI
func (n *Normalizer) NormalizeCHI(chi *chiv1.ClickHouseInstallation) (*chiv1.ClickHouseInstallation, error) {
	if chi != nil {
		n.chi = chi
	}

	// Walk over ChiSpec datatype fields
	n.normalizeUseTemplates(&n.chi.Spec.UseTemplates)
	n.normalizeStop(&n.chi.Spec.Stop)
	n.normalizeDefaults(&n.chi.Spec.Defaults)
	n.normalizeConfiguration(&n.chi.Spec.Configuration)
	n.normalizeTemplates(&n.chi.Spec.Templates)

	n.finalizeCHI()
	n.fillStatus()

	return n.chi, nil
}

// finalizeCHI performs some finalization tasks, which should be done after CHI is normalized
func (n *Normalizer) finalizeCHI() {
	n.chi.FillAddressInfo()
	n.chi.FillCHIPointer()
	n.chi.WalkHosts(func(host *chiv1.ChiHost) error {
		hostTemplate := n.getHostTemplate(host)
		hostApplyHostTemplate(host, hostTemplate)
		return nil
	})
	n.chi.WalkHosts(func(host *chiv1.ChiHost) error {
		return n.calcFingerprints(host)
	})
}

// getHostTemplate gets Host Template to be used to normalize Host
func (n *Normalizer) getHostTemplate(host *chiv1.ChiHost) *chiv1.ChiHostTemplate {
	statefulSetName := CreateStatefulSetName(host)

	// Which host template would be used - either explicitly defined in or a default one
	hostTemplate, ok := host.GetHostTemplate()
	if ok {
		// Host references known HostTemplate
		log.V(2).Infof("getHostTemplate() statefulSet %s use custom host template %s", statefulSetName, hostTemplate.Name)
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

	log.V(3).Infof("getHostTemplate() statefulSet %s use default host template", statefulSetName)

	return hostTemplate
}

// hostApplyHostTemplate
func hostApplyHostTemplate(host *chiv1.ChiHost, template *chiv1.ChiHostTemplate) {
	if host.Name == "" {
		host.Name = template.Spec.Name
	}

	for _, portDistribution := range template.PortDistribution {
		switch portDistribution.Type {
		case chiv1.PortDistributionUnspecified:
			if host.TCPPort == chPortNumberMustBeAssignedLater {
				host.TCPPort = template.Spec.TCPPort
			}
			if host.HTTPPort == chPortNumberMustBeAssignedLater {
				host.HTTPPort = template.Spec.HTTPPort
			}
			if host.InterserverHTTPPort == chPortNumberMustBeAssignedLater {
				host.InterserverHTTPPort = template.Spec.InterserverHTTPPort
			}
		case chiv1.PortDistributionClusterScopeIndex:
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
func hostApplyPortsFromSettings(host *chiv1.ChiHost) {
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
	n.chi.WalkHosts(func(host *chiv1.ChiHost) error {
		pods = append(pods, CreatePodName(host))
		fqdns = append(fqdns, CreatePodFQDN(host))
		return nil
	})
	n.chi.FillStatus(endpoint, pods, fqdns)
}

// normalizeStop normalizes .spec.stop
func (n *Normalizer) normalizeStop(stop *string) {
	// Set defaults for CHI object properties
	if !util.IsStringBool(*stop) {
		// In case it is unknown value - just use set it to false
		*stop = util.StringBoolFalseLowercase
	}
}

// normalizeDefaults normalizes .spec.defaults
func (n *Normalizer) normalizeDefaults(defaults *chiv1.ChiDefaults) {
	// Set defaults for CHI object properties
	n.normalizeDefaultsReplicasUseFQDN(defaults)
	n.normalizeDefaultsTemplates(defaults)
}

// normalizeConfiguration normalizes .spec.configuration
func (n *Normalizer) normalizeConfiguration(conf *chiv1.Configuration) {
	n.normalizeConfigurationZookeeper(&conf.Zookeeper)

	n.normalizeConfigurationUsers(&conf.Users)
	n.normalizeConfigurationProfiles(&conf.Profiles)
	n.normalizeConfigurationQuotas(&conf.Quotas)
	n.normalizeConfigurationSettings(&conf.Settings)
	n.normalizeConfigurationFiles(&conf.Files)

	// Configuration.Clusters
	n.normalizeClusters()
}

// normalizeTemplates normalizes .spec.templates
func (n *Normalizer) normalizeTemplates(templates *chiv1.ChiTemplates) {
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
}

// normalizeHostTemplate normalizes .spec.templates.hostTemplates
func (n *Normalizer) normalizeHostTemplate(template *chiv1.ChiHostTemplate) {
	// Name

	// PortDistribution

	if template.PortDistribution == nil {
		// In case no PortDistribution provided - setup default one
		template.PortDistribution = []chiv1.ChiPortDistribution{
			{Type: chiv1.PortDistributionUnspecified},
		}
	}
	// Normalize PortDistribution
	for i := range template.PortDistribution {
		portDistribution := &template.PortDistribution[i]
		switch portDistribution.Type {
		case
			chiv1.PortDistributionUnspecified,
			chiv1.PortDistributionClusterScopeIndex:
			// distribution is known
		default:
			// distribution is not known
			portDistribution.Type = chiv1.PortDistributionUnspecified
		}
	}

	// Spec
	n.normalizeHostTemplateSpec(&template.Spec)

	// Introduce HostTemplate into Index
	// Ensure map is in place
	if n.chi.Spec.Templates.HostTemplatesIndex == nil {
		n.chi.Spec.Templates.HostTemplatesIndex = make(map[string]*chiv1.ChiHostTemplate)
	}

	n.chi.Spec.Templates.HostTemplatesIndex[template.Name] = template
}

// normalizePodTemplate normalizes .spec.templates.podTemplates
func (n *Normalizer) normalizePodTemplate(template *chiv1.ChiPodTemplate) {
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
	if template.Distribution == chiv1.PodDistributionOnePerHost {
		// Known distribution, all is fine
	} else {
		// Default Pod Distribution
		template.Distribution = chiv1.PodDistributionUnspecified
	}

	// PodDistribution
	for i := range template.PodDistribution {
		podDistribution := &template.PodDistribution[i]
		switch podDistribution.Type {
		case
			chiv1.PodDistributionUnspecified,

			// AntiAffinity section
			chiv1.PodDistributionClickHouseAntiAffinity,
			chiv1.PodDistributionShardAntiAffinity,
			chiv1.PodDistributionReplicaAntiAffinity:
			if podDistribution.Scope == "" {
				podDistribution.Scope = chiv1.PodDistributionScopeCluster
			}
		case
			chiv1.PodDistributionAnotherNamespaceAntiAffinity,
			chiv1.PodDistributionAnotherClickHouseInstallationAntiAffinity,
			chiv1.PodDistributionAnotherClusterAntiAffinity:
			// PodDistribution is known
		case
			chiv1.PodDistributionMaxNumberPerNode:
			// PodDistribution is known
			if podDistribution.Number < 0 {
				podDistribution.Number = 0
			}
		case
			// Affinity section
			chiv1.PodDistributionNamespaceAffinity,
			chiv1.PodDistributionClickHouseInstallationAffinity,
			chiv1.PodDistributionClusterAffinity,
			chiv1.PodDistributionShardAffinity,
			chiv1.PodDistributionReplicaAffinity,
			chiv1.PodDistributionPreviousTailAffinity:
			// PodDistribution is known

		case chiv1.PodDistributionCircularReplication:
			// Shortcut section
			// All shortcuts have to be expanded

			// PodDistribution is known

			if podDistribution.Scope == "" {
				podDistribution.Scope = chiv1.PodDistributionScopeCluster
			}

			// TODO need to support multi-cluster
			cluster := &n.chi.Spec.Configuration.Clusters[0]

			template.PodDistribution = append(template.PodDistribution, chiv1.ChiPodDistribution{
				Type:  chiv1.PodDistributionShardAntiAffinity,
				Scope: podDistribution.Scope,
			})
			template.PodDistribution = append(template.PodDistribution, chiv1.ChiPodDistribution{
				Type:  chiv1.PodDistributionReplicaAntiAffinity,
				Scope: podDistribution.Scope,
			})
			template.PodDistribution = append(template.PodDistribution, chiv1.ChiPodDistribution{
				Type:   chiv1.PodDistributionMaxNumberPerNode,
				Scope:  podDistribution.Scope,
				Number: cluster.Layout.ReplicasCount,
			})

			template.PodDistribution = append(template.PodDistribution, chiv1.ChiPodDistribution{
				Type: chiv1.PodDistributionPreviousTailAffinity,
			})

			template.PodDistribution = append(template.PodDistribution, chiv1.ChiPodDistribution{
				Type: chiv1.PodDistributionNamespaceAffinity,
			})
			template.PodDistribution = append(template.PodDistribution, chiv1.ChiPodDistribution{
				Type: chiv1.PodDistributionClickHouseInstallationAffinity,
			})
			template.PodDistribution = append(template.PodDistribution, chiv1.ChiPodDistribution{
				Type: chiv1.PodDistributionClusterAffinity,
			})

		default:
			// PodDistribution is not known
			podDistribution.Type = chiv1.PodDistributionUnspecified
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
		n.chi.Spec.Templates.PodTemplatesIndex = make(map[string]*chiv1.ChiPodTemplate)
	}

	n.chi.Spec.Templates.PodTemplatesIndex[template.Name] = template
}

// newAffinity
func (n *Normalizer) newAffinity(template *chiv1.ChiPodTemplate) *v1.Affinity {
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
func (n *Normalizer) newNodeAffinity(template *chiv1.ChiPodTemplate) *v1.NodeAffinity {
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
func (n *Normalizer) newPodAffinity(template *chiv1.ChiPodTemplate) *v1.PodAffinity {
	podAffinity := &v1.PodAffinity{}

	for i := range template.PodDistribution {
		podDistribution := &template.PodDistribution[i]
		switch podDistribution.Type {
		case chiv1.PodDistributionNamespaceAffinity:
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = n.addWeightedPodAffinityTermWithMatchLabels(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				1,
				map[string]string{
					LabelNamespace: macrosNamespace,
				},
			)
		case chiv1.PodDistributionClickHouseInstallationAffinity:
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = n.addWeightedPodAffinityTermWithMatchLabels(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				1,
				map[string]string{
					LabelCHIName: macrosChiName,
				},
			)
		case chiv1.PodDistributionClusterAffinity:
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = n.addWeightedPodAffinityTermWithMatchLabels(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				1,
				map[string]string{
					LabelClusterName: macrosClusterName,
				},
			)
		case chiv1.PodDistributionShardAffinity:
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = n.addWeightedPodAffinityTermWithMatchLabels(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				1,
				map[string]string{
					LabelShardName: macrosShardName,
				},
			)
		case chiv1.PodDistributionReplicaAffinity:
			podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = n.addWeightedPodAffinityTermWithMatchLabels(
				podAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
				1,
				map[string]string{
					LabelReplicaName: macrosReplicaName,
				},
			)
		case chiv1.PodDistributionPreviousTailAffinity:
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
	podDistribution *chiv1.ChiPodDistribution,
	matchLabels map[string]string,
) map[string]string {
	var scopeLabels map[string]string

	switch podDistribution.Scope {
	case chiv1.PodDistributionScopeShard:
		scopeLabels = map[string]string{
			LabelNamespace:   macrosNamespace,
			LabelCHIName:     macrosChiName,
			LabelClusterName: macrosClusterName,
			LabelShardName:   macrosShardName,
		}
	case chiv1.PodDistributionScopeReplica:
		scopeLabels = map[string]string{
			LabelNamespace:   macrosNamespace,
			LabelCHIName:     macrosChiName,
			LabelClusterName: macrosClusterName,
			LabelReplicaName: macrosReplicaName,
		}
	case chiv1.PodDistributionScopeCluster:
		scopeLabels = map[string]string{
			LabelNamespace:   macrosNamespace,
			LabelCHIName:     macrosChiName,
			LabelClusterName: macrosClusterName,
		}
	case chiv1.PodDistributionScopeClickHouseInstallation:
		scopeLabels = map[string]string{
			LabelNamespace: macrosNamespace,
			LabelCHIName:   macrosChiName,
		}
	case chiv1.PodDistributionScopeNamespace:
		scopeLabels = map[string]string{
			LabelNamespace: macrosNamespace,
		}
	case chiv1.PodDistributionScopeGlobal:
		scopeLabels = map[string]string{}
	}

	return util.MergeStringMaps(matchLabels, scopeLabels)
}

// newPodAntiAffinity
func (n *Normalizer) newPodAntiAffinity(template *chiv1.ChiPodTemplate) *v1.PodAntiAffinity {
	podAntiAffinity := &v1.PodAntiAffinity{}

	// Distribution
	// DEPRECATED
	if template.Distribution == chiv1.PodDistributionOnePerHost {
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
		case chiv1.PodDistributionClickHouseAntiAffinity:
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = n.addPodAffinityTermWithMatchLabels(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				n.newMatchLabels(
					podDistribution,
					map[string]string{
						LabelAppName: LabelAppValue,
					},
				),
			)
		case chiv1.PodDistributionMaxNumberPerNode:
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = n.addPodAffinityTermWithMatchLabels(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				n.newMatchLabels(
					podDistribution,
					map[string]string{
						LabelClusterScopeCycleIndex: macrosClusterScopeCycleIndex,
					},
				),
			)
		case chiv1.PodDistributionShardAntiAffinity:
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = n.addPodAffinityTermWithMatchLabels(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				n.newMatchLabels(
					podDistribution,
					map[string]string{
						LabelShardName: macrosShardName,
					},
				),
			)
		case chiv1.PodDistributionReplicaAntiAffinity:
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = n.addPodAffinityTermWithMatchLabels(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				n.newMatchLabels(
					podDistribution,
					map[string]string{
						LabelReplicaName: macrosReplicaName,
					},
				),
			)
		case chiv1.PodDistributionAnotherNamespaceAntiAffinity:
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = n.addPodAffinityTermWithMatchExpressions(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				[]v12.LabelSelectorRequirement{
					{
						Key:      LabelNamespace,
						Operator: v12.LabelSelectorOpNotIn,
						Values: []string{
							macrosNamespace,
						},
					},
				},
			)
		case chiv1.PodDistributionAnotherClickHouseInstallationAntiAffinity:
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = n.addPodAffinityTermWithMatchExpressions(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				[]v12.LabelSelectorRequirement{
					{
						Key:      LabelCHIName,
						Operator: v12.LabelSelectorOpNotIn,
						Values: []string{
							macrosChiName,
						},
					},
				},
			)
		case chiv1.PodDistributionAnotherClusterAntiAffinity:
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = n.addPodAffinityTermWithMatchExpressions(
				podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
				[]v12.LabelSelectorRequirement{
					{
						Key:      LabelClusterName,
						Operator: v12.LabelSelectorOpNotIn,
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
			LabelSelector: &v12.LabelSelector{
				// A list of node selector requirements by node's labels.
				//MatchLabels: map[string]string{
				//	LabelClusterScopeCycleIndex: macrosClusterScopeCycleIndex,
				//},
				MatchLabels: matchLabels,
				// Switch to MatchLabels
				//MatchExpressions: []v12.LabelSelectorRequirement{
				//	{
				//		Key:      LabelAppName,
				//		Operator: v12.LabelSelectorOpIn,
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
func (n *Normalizer) addPodAffinityTermWithMatchExpressions(terms []v1.PodAffinityTerm, matchExpressions []v12.LabelSelectorRequirement) []v1.PodAffinityTerm {
	return append(terms,
		v1.PodAffinityTerm{
			LabelSelector: &v12.LabelSelector{
				// A list of node selector requirements by node's labels.
				//MatchLabels: map[string]string{
				//	LabelClusterScopeCycleIndex: macrosClusterScopeCycleIndex,
				//},
				//MatchExpressions: []v12.LabelSelectorRequirement{
				//	{
				//		Key:      LabelAppName,
				//		Operator: v12.LabelSelectorOpIn,
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
				LabelSelector: &v12.LabelSelector{
					// A list of node selector requirements by node's labels.
					//MatchLabels: map[string]string{
					//	LabelClusterScopeCycleIndex: macrosClusterScopeCycleIndex,
					//},
					MatchLabels: matchLabels,
					// Switch to MatchLabels
					//MatchExpressions: []v12.LabelSelectorRequirement{
					//	{
					//		Key:      LabelAppName,
					//		Operator: v12.LabelSelectorOpIn,
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
func (n *Normalizer) normalizeVolumeClaimTemplate(template *chiv1.ChiVolumeClaimTemplate) {
	// Check name
	// Check PVCReclaimPolicy
	if !template.PVCReclaimPolicy.IsValid() {
		template.PVCReclaimPolicy = chiv1.PVCReclaimPolicyDelete
	}
	// Check Spec

	// Ensure map is in place
	if n.chi.Spec.Templates.VolumeClaimTemplatesIndex == nil {
		n.chi.Spec.Templates.VolumeClaimTemplatesIndex = make(map[string]*chiv1.ChiVolumeClaimTemplate)
	}
	n.chi.Spec.Templates.VolumeClaimTemplatesIndex[template.Name] = template
}

// normalizeServiceTemplate normalizes .spec.templates.serviceTemplates
func (n *Normalizer) normalizeServiceTemplate(template *chiv1.ChiServiceTemplate) {
	// Check name
	// Check GenerateName
	// Check ObjectMeta
	// Check Spec

	// Ensure map is in place
	if n.chi.Spec.Templates.ServiceTemplatesIndex == nil {
		n.chi.Spec.Templates.ServiceTemplatesIndex = make(map[string]*chiv1.ChiServiceTemplate)
	}
	n.chi.Spec.Templates.ServiceTemplatesIndex[template.Name] = template
}

// normalizeUseTemplates normalizes .spec.useTemplates
func (n *Normalizer) normalizeUseTemplates(useTemplates *[]chiv1.ChiUseTemplate) {
	for i := range *useTemplates {
		useTemplate := &(*useTemplates)[i]
		n.normalizeUseTemplate(useTemplate)
	}
}

// normalizeUseTemplate normalizes ChiUseTemplate
func (n *Normalizer) normalizeUseTemplate(useTemplate *chiv1.ChiUseTemplate) {
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
func (n *Normalizer) normalizeClusters() {
	// We need to have at least one cluster available
	n.ensureCluster()

	// Normalize all clusters in this CHI
	n.chi.WalkClusters(func(cluster *chiv1.ChiCluster) error {
		return n.normalizeCluster(cluster)
	})
}

// ensureCluster
func (n *Normalizer) ensureCluster() {
	// Introduce default cluster in case it is required
	if len(n.chi.Spec.Configuration.Clusters) == 0 {
		if n.withDefaultCluster {
			n.chi.Spec.Configuration.Clusters = []chiv1.ChiCluster{
				{
					Name: "cluster",
				},
			}
		} else {
			n.chi.Spec.Configuration.Clusters = []chiv1.ChiCluster{}
		}
	}
}

// calcFingerprints calculates fingerprints for ClickHouse configuration data
func (n *Normalizer) calcFingerprints(host *chiv1.ChiHost) error {
	host.Config.ZookeeperFingerprint = util.Fingerprint(*host.GetZookeeper())
	host.Config.SettingsFingerprint = util.Fingerprint(
		fmt.Sprintf("%s%s",
			util.Fingerprint(n.chi.Spec.Configuration.Settings.AsSortedSliceOfStrings()),
			util.Fingerprint(host.Settings.AsSortedSliceOfStrings()),
		),
	)
	host.Config.FilesFingerprint = util.Fingerprint(
		fmt.Sprintf("%s%s",
			util.Fingerprint(
				n.chi.Spec.Configuration.Files.Filter(
					nil,
					[]chiv1.SettingsSection{chiv1.SectionUsers},
					true,
				).AsSortedSliceOfStrings(),
			),
			util.Fingerprint(
				host.Files.Filter(
					nil,
					[]chiv1.SettingsSection{chiv1.SectionUsers},
					true,
				).AsSortedSliceOfStrings(),
			),
		),
	)

	return nil
}

// normalizeConfigurationZookeeper normalizes .spec.configuration.zookeeper
func (n *Normalizer) normalizeConfigurationZookeeper(zk *chiv1.ChiZookeeperConfig) {
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
}

// normalizeConfigurationUsers normalizes .spec.configuration.users
func (n *Normalizer) normalizeConfigurationUsers(users *chiv1.Settings) {

	if users == nil {
		// Do not know what to do in this case
		return
	}

	if *users == nil {
		*users = chiv1.NewSettings()
	}

	(*users).Normalize()

	// Extract username from path
	usernameMap := make(map[string]bool)
	for path := range *users {
		// Split 'admin/password'
		tags := strings.Split(path, "/")

		// Basic sanity check - need to have at least "username/something" pair
		if len(tags) < 2 {
			// Skip incorrect entry
			continue
		}

		username := tags[0]
		usernameMap[username] = true
	}

	// Ensure "must have" sections are in place, which are
	// 1. user/profile
	// 2. user/quota
	// 3. user/networks/ip and user/networks/host_regexp defaults to the installation pods
	// 4. user/password_sha256_hex

	usernameMap["default"] = true // we need default user here in order to secure host_regexp
	for username := range usernameMap {
		if _, ok := (*users)[username+"/profile"]; !ok {
			// No 'user/profile' section
			(*users)[username+"/profile"] = chiv1.NewScalarSetting(n.chop.Config().CHConfigUserDefaultProfile)
		}
		if _, ok := (*users)[username+"/quota"]; !ok {
			// No 'user/quota' section
			(*users)[username+"/quota"] = chiv1.NewScalarSetting(n.chop.Config().CHConfigUserDefaultQuota)
		}
		if _, ok := (*users)[username+"/networks/ip"]; !ok {
			// No 'user/networks/ip' section
			(*users)[username+"/networks/ip"] = chiv1.NewVectorSetting(n.chop.Config().CHConfigUserDefaultNetworksIP)
		}
		if _, ok := (*users)[username+"/networks/host_regexp"]; !ok {
			// No 'user/networks/host_regexp' section
			(*users)[username+"/networks/host_regexp"] = chiv1.NewScalarSetting(CreatePodRegexp(n.chi, n.chop.Config().CHConfigNetworksHostRegexpTemplate))
		}

		var pass = ""
		_pass, okPassword := (*users)[username+"/password"]
		if okPassword {
			pass = fmt.Sprintf("%v", _pass)
		} else if username != "default" {
			pass = n.chop.Config().CHConfigUserDefaultPassword
		}

		_, okPasswordSHA256 := (*users)[username+"/password_sha256_hex"]
		// if SHA256 is not set, initialize it from the password
		if pass != "" && !okPasswordSHA256 {
			pass_sha256 := sha256.Sum256([]byte(pass))
			(*users)[username+"/password_sha256_hex"] = chiv1.NewScalarSetting(hex.EncodeToString(pass_sha256[:]))
			okPasswordSHA256 = true
		}

		if okPasswordSHA256 {
			// ClickHouse does not start if both password and sha256 are defined
			if username == "default" {
				// Set remove password flag for default user that is empty in stock ClickHouse users.xml
				(*users)[username+"/password"] = chiv1.NewScalarSetting("_removed_")
			} else {
				delete(*users, username+"/password")
			}
		}
	}
}

// normalizeConfigurationProfiles normalizes .spec.configuration.profiles
func (n *Normalizer) normalizeConfigurationProfiles(profiles *chiv1.Settings) {

	if profiles == nil {
		// Do not know what to do in this case
		return
	}

	if *profiles == nil {
		*profiles = chiv1.NewSettings()
	}
	(*profiles).Normalize()
}

// normalizeConfigurationQuotas normalizes .spec.configuration.quotas
func (n *Normalizer) normalizeConfigurationQuotas(quotas *chiv1.Settings) {

	if quotas == nil {
		// Do not know what to do in this case
		return
	}

	if *quotas == nil {
		*quotas = chiv1.NewSettings()
	}

	(*quotas).Normalize()
}

// normalizeConfigurationSettings normalizes .spec.configuration.settings
func (n *Normalizer) normalizeConfigurationSettings(settings *chiv1.Settings) {

	if settings == nil {
		// Do not know what to do in this case
		return
	}

	if *settings == nil {
		*settings = chiv1.NewSettings()
	}

	(*settings).Normalize()
}

// normalizeConfigurationFiles normalizes .spec.configuration.files
func (n *Normalizer) normalizeConfigurationFiles(files *chiv1.Settings) {

	if files == nil {
		// Do not know what to do in this case
		return
	}

	if *files == nil {
		*files = chiv1.NewSettings()
	}

	(*files).Normalize()
}

// normalizeCluster normalizes cluster and returns deployments usage counters for this cluster
func (n *Normalizer) normalizeCluster(cluster *chiv1.ChiCluster) error {
	cluster.FillShardReplicaSpecified()

	// Inherit from .spec.configuration.zookeeper
	cluster.InheritZookeeperFrom(n.chi)
	// Inherit from .spec.configuration.files
	cluster.InheritFilesFrom(n.chi)
	// Inherit from .spec.defaults
	cluster.InheritTemplatesFrom(n.chi)

	n.normalizeConfigurationZookeeper(&cluster.Zookeeper)
	n.normalizeConfigurationSettings(&cluster.Settings)
	n.normalizeConfigurationFiles(&cluster.Files)

	n.normalizeClusterLayoutShardsCountAndReplicasCount(&cluster.Layout)

	n.ensureClusterLayoutShards(&cluster.Layout)
	n.ensureClusterLayoutReplicas(&cluster.Layout)

	n.createHostsField(cluster)

	// Loop over all shards and replicas inside shards and fill structure
	cluster.WalkShards(func(index int, shard *chiv1.ChiShard) error {
		n.normalizeShard(shard, cluster, index)
		return nil
	})

	cluster.WalkReplicas(func(index int, replica *chiv1.ChiReplica) error {
		n.normalizeReplica(replica, cluster, index)
		return nil
	})

	cluster.Layout.HostsField.WalkHosts(func(shard, replica int, host *chiv1.ChiHost) error {
		n.normalizeHost(host, cluster.GetShard(shard), cluster.GetReplica(replica), cluster, shard, replica)
		return nil
	})

	return nil
}

// createHostsField
func (n *Normalizer) createHostsField(cluster *chiv1.ChiCluster) {
	cluster.Layout.HostsField = chiv1.NewHostsField(cluster.Layout.ShardsCount, cluster.Layout.ReplicasCount)

	// Need to migrate hosts from Shards and Replicas into HostsField
	hostMergeFunc := func(shard, replica int, host *chiv1.ChiHost) error {
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
func (n *Normalizer) normalizeClusterLayoutShardsCountAndReplicasCount(layout *chiv1.ChiClusterLayout) {
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
}

// ensureClusterLayoutShards ensures slice layout.Shards is in place
func (n *Normalizer) ensureClusterLayoutShards(layout *chiv1.ChiClusterLayout) {
	// Disposition of shards in slice would be
	// [explicitly specified shards 0..N, N+1..layout.ShardsCount-1 empty slots for to-be-filled shards]

	// Some (may be all) shards specified, need to append space for unspecified shards
	// TODO may be there is better way to append N slots to a slice
	for len(layout.Shards) < layout.ShardsCount {
		layout.Shards = append(layout.Shards, chiv1.ChiShard{})
	}
}

// ensureClusterLayoutReplicas ensures slice layout.Replicas is in place
func (n *Normalizer) ensureClusterLayoutReplicas(layout *chiv1.ChiClusterLayout) {
	// Disposition of replicas in slice would be
	// [explicitly specified replicas 0..N, N+1..layout.ReplicasCount-1 empty slots for to-be-filled replicas]

	// Some (may be all) replicas specified, need to append space for unspecified replicas
	// TODO may be there is better way to append N slots to a slice
	for len(layout.Replicas) < layout.ReplicasCount {
		layout.Replicas = append(layout.Replicas, chiv1.ChiReplica{})
	}
}

// normalizeShard normalizes a shard - walks over all fields
func (n *Normalizer) normalizeShard(shard *chiv1.ChiShard, cluster *chiv1.ChiCluster, shardIndex int) {
	n.normalizeShardName(shard, shardIndex)
	n.normalizeShardWeight(shard)
	// For each shard of this normalized cluster inherit from cluster
	shard.InheritSettingsFrom(cluster)
	n.normalizeConfigurationSettings(&shard.Settings)
	shard.InheritFilesFrom(cluster)
	n.normalizeConfigurationSettings(&shard.Files)
	shard.InheritTemplatesFrom(cluster)
	// Normalize Replicas
	n.normalizeShardReplicasCount(shard, cluster.Layout.ReplicasCount)
	n.normalizeShardHosts(shard, cluster, shardIndex)
	// Internal replication uses ReplicasCount thus it has to be normalized after shard ReplicaCount normalized
	n.normalizeShardInternalReplication(shard)
}

// normalizeReplica normalizes a replica - walks over all fields
func (n *Normalizer) normalizeReplica(replica *chiv1.ChiReplica, cluster *chiv1.ChiCluster, replicaIndex int) {
	n.normalizeReplicaName(replica, replicaIndex)
	// For each replica of this normalized cluster inherit from cluster
	replica.InheritSettingsFrom(cluster)
	n.normalizeConfigurationSettings(&replica.Settings)
	replica.InheritFilesFrom(cluster)
	n.normalizeConfigurationSettings(&replica.Files)
	replica.InheritTemplatesFrom(cluster)
	// Normalize Shards
	n.normalizeReplicaShardsCount(replica, cluster.Layout.ShardsCount)
	n.normalizeReplicaHosts(replica, cluster, replicaIndex)
}

// normalizeShardReplicasCount ensures shard.ReplicasCount filled properly
func (n *Normalizer) normalizeShardReplicasCount(shard *chiv1.ChiShard, layoutReplicasCount int) {
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
func (n *Normalizer) normalizeReplicaShardsCount(replica *chiv1.ChiReplica, layoutShardsCount int) {
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
func (n *Normalizer) normalizeShardName(shard *chiv1.ChiShard, index int) {
	if (len(shard.Name) > 0) && !IsAutoGeneratedShardName(shard.Name, shard, index) {
		// Has explicitly specified name already
		return
	}

	shard.Name = CreateShardName(shard, index)
}

// normalizeReplicaName normalizes replica name
func (n *Normalizer) normalizeReplicaName(replica *chiv1.ChiReplica, index int) {
	if (len(replica.Name) > 0) && !IsAutoGeneratedReplicaName(replica.Name, replica, index) {
		// Has explicitly specified name already
		return
	}

	replica.Name = CreateReplicaName(replica, index)
}

// normalizeShardName normalizes shard weight
func (n *Normalizer) normalizeShardWeight(shard *chiv1.ChiShard) {
}

// normalizeShardHosts normalizes all replicas of specified shard
func (n *Normalizer) normalizeShardHosts(shard *chiv1.ChiShard, cluster *chiv1.ChiCluster, shardIndex int) {
	// Use hosts from HostsField
	shard.Hosts = nil
	for len(shard.Hosts) < shard.ReplicasCount {
		// We still have some assumed hosts in this shard - let's add it as replicaIndex
		replicaIndex := len(shard.Hosts)
		// Check whether we have this host in HostsField
		host := cluster.Layout.HostsField.GetOrCreate(shardIndex, replicaIndex)
		shard.Hosts = append(shard.Hosts, host)
	}
}

// normalizeReplicaHosts normalizes all replicas of specified shard
func (n *Normalizer) normalizeReplicaHosts(replica *chiv1.ChiReplica, cluster *chiv1.ChiCluster, replicaIndex int) {
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
	host *chiv1.ChiHost,
	shard *chiv1.ChiShard,
	replica *chiv1.ChiReplica,
	cluster *chiv1.ChiCluster,
	shardIndex int,
	replicaIndex int,
) {
	n.normalizeHostName(host, shard, shardIndex, replica, replicaIndex)
	n.normalizeHostPorts(host)
	// Inherit from either Shard or Replica
	var s *chiv1.ChiShard
	var r *chiv1.ChiReplica
	if cluster.IsShardSpecified() {
		s = shard
	} else {
		r = replica
	}
	host.InheritSettingsFrom(s, r)
	n.normalizeConfigurationSettings(&host.Settings)
	host.InheritFilesFrom(s, r)
	n.normalizeConfigurationSettings(&host.Files)
	host.InheritTemplatesFrom(s, r, nil)
}

// normalizeHostTemplateSpec is the same as normalizeHost but for a template
func (n *Normalizer) normalizeHostTemplateSpec(host *chiv1.ChiHost) {
	n.normalizeHostPorts(host)
}

// normalizeHostName normalizes host's name
func (n *Normalizer) normalizeHostName(
	host *chiv1.ChiHost,
	shard *chiv1.ChiShard,
	shardIndex int,
	replica *chiv1.ChiReplica,
	replicaIndex int,
) {
	if (len(host.Name) > 0) && !IsAutoGeneratedHostName(host.Name, host, shard, shardIndex, replica, replicaIndex) {
		// Has explicitly specified name already
		return
	}

	host.Name = CreateHostName(host, shard, shardIndex, replica, replicaIndex)
}

// normalizeHostPorts ensures chiv1.ChiReplica.Port is reasonable
func (n *Normalizer) normalizeHostPorts(host *chiv1.ChiHost) {
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
func (n *Normalizer) normalizeShardInternalReplication(shard *chiv1.ChiShard) {
	// Shards with replicas are expected to have internal replication on by default
	defaultInternalReplication := false
	if shard.ReplicasCount > 1 {
		defaultInternalReplication = true
	}
	shard.InternalReplication = util.CastStringBoolToStringTrueFalse(shard.InternalReplication, defaultInternalReplication)
}

// normalizeDefaultsReplicasUseFQDN ensures chiv1.ChiDefaults.ReplicasUseFQDN section has proper values
func (n *Normalizer) normalizeDefaultsReplicasUseFQDN(d *chiv1.ChiDefaults) {
	// Default value set to false
	d.ReplicasUseFQDN = util.CastStringBoolToStringTrueFalse(d.ReplicasUseFQDN, false)
}

// normalizeDefaultsTemplates ensures chiv1.ChiDefaults.Templates section has proper values
func (n *Normalizer) normalizeDefaultsTemplates(d *chiv1.ChiDefaults) {
	d.Templates.HandleDeprecatedFields()
}
