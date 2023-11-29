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

package chi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube "k8s.io/client-go/kubernetes"

	"github.com/google/uuid"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	chiV1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// NormalizerContext specifies CHI-related normalization context
type NormalizerContext struct {
	// start specifies start CHI from which normalization has started
	start *chiV1.ClickHouseInstallation
	// chi specifies current CHI being normalized
	chi *chiV1.ClickHouseInstallation
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
	// DefaultUserAdditionalIPs specifies set of additional IPs applied to default user
	DefaultUserAdditionalIPs   []string
	DefaultUserInsertHostRegex bool
}

// NewNormalizerOptions creates new NormalizerOptions
func NewNormalizerOptions() *NormalizerOptions {
	return &NormalizerOptions{
		DefaultUserInsertHostRegex: true,
	}
}

// Normalizer specifies structures normalizer
type Normalizer struct {
	kubeClient kube.Interface
	ctx        *NormalizerContext
}

// NewNormalizer creates new normalizer
func NewNormalizer(kubeClient kube.Interface) *Normalizer {
	return &Normalizer{
		kubeClient: kubeClient,
	}
}

func newCHI() *chiV1.ClickHouseInstallation {
	return &chiV1.ClickHouseInstallation{
		TypeMeta: metav1.TypeMeta{
			Kind:       chiV1.ClickHouseInstallationCRDResourceKind,
			APIVersion: chiV1.SchemeGroupVersion.String(),
		},
	}
}

// CreateTemplatedCHI produces ready-to-use CHI object
func (n *Normalizer) CreateTemplatedCHI(
	chi *chiV1.ClickHouseInstallation,
	options *NormalizerOptions,
) (*chiV1.ClickHouseInstallation, error) {
	// New CHI starts with new context
	n.ctx = NewNormalizerContext(options)

	// Normalize start CHI
	chi = n.normalizeStartCHI(chi)
	// Create new chi that will be populated with data during normalization process
	n.ctx.chi = n.createBaseCHI()

	// At this moment context chi is either newly created 'empty' CHI or a system-wide template

	// Apply templates - both auto and explicitly requested - on top of context chi
	n.applyCHITemplates(chi)

	// After all templates applied, place provided CHI on top of the whole stack
	n.ctx.chi.MergeFrom(chi, chiV1.MergeTypeOverrideByNonEmptyValues)

	return n.normalize()
}

func (n *Normalizer) normalizeStartCHI(chi *chiV1.ClickHouseInstallation) *chiV1.ClickHouseInstallation {
	if chi == nil {
		// No CHI specified - meaning we are building over provided 'empty' CHI with no clusters inside
		chi = newCHI()
		n.ctx.options.WithDefaultCluster = false
	} else {
		// Even in case having CHI provided, we need to insert default cluster in case no clusters specified
		n.ctx.options.WithDefaultCluster = true
	}
	return chi
}

func (n *Normalizer) createBaseCHI() *chiV1.ClickHouseInstallation {
	// What base should be used to create CHI
	if chop.Config().Template.CHI.Runtime.Template == nil {
		// No template specified - start with clear page
		return newCHI()
	} else {
		// Template specified - start with template
		return chop.Config().Template.CHI.Runtime.Template.DeepCopy()
	}
}

// prepareListOfCHITemplates prepares list of CHI templates to be used by CHI
func (n *Normalizer) prepareListOfCHITemplates(chi *chiV1.ClickHouseInstallation) []chiV1.ChiUseTemplate {
	// useTemplates specifies list of templates to be applied to the CHI
	var useTemplates []chiV1.ChiUseTemplate

	// 1. Get list of auto templates available
	if autoTemplates := chop.Config().GetAutoTemplates(); len(autoTemplates) > 0 {
		log.V(1).M(chi).F().Info("Found auto-templates num: %d", len(autoTemplates))
		for _, template := range autoTemplates {
			log.V(1).M(chi).F().Info("Adding auto-template to list of applicable templates: %s/%s ", template.Namespace, template.Name)
			useTemplates = append(useTemplates, chiV1.ChiUseTemplate{
				Name:      template.Name,
				Namespace: template.Namespace,
				UseType:   useTypeMerge,
			})
		}
	}

	// 2. Append templates, explicitly requested by the CHI
	if len(chi.Spec.UseTemplates) > 0 {
		log.V(1).M(chi).F().Info("Found manual-templates num: %d", len(chi.Spec.UseTemplates))
		useTemplates = append(useTemplates, chi.Spec.UseTemplates...)
	}

	// In case useTemplates must contain reasonable data, thus has to be normalized
	if len(useTemplates) > 0 {
		log.V(1).M(chi).F().Info("Found applicable templates num: %d", len(useTemplates))
		n.normalizeUseTemplates(useTemplates)
	}

	return useTemplates
}

// applyCHITemplates applies CHI templates over n.ctx.chi
func (n *Normalizer) applyCHITemplates(chi *chiV1.ClickHouseInstallation) {
	// At this moment n.chi is either newly created 'empty' CHI or a system-wide template

	// useTemplates specifies list of templates to be applied to the CHI
	useTemplates := n.prepareListOfCHITemplates(chi)

	// Apply templates - both auto and explicitly requested
	for i := range useTemplates {
		// Convenience wrapper
		useTemplate := &useTemplates[i]
		template := chop.Config().FindTemplate(useTemplate, chi.Namespace)

		if template == nil {
			log.V(1).M(chi).F().Warning("Skip template: %s/%s UNABLE to find listed template. ", useTemplate.Namespace, useTemplate.Name)
			continue // skip to the next template
		}

		// What CHI this template wants to be applied to?
		// This is determined by matching selector of the template and CHI's labels
		selector := template.Spec.Templating.GetCHISelector()
		labels := chi.Labels

		if !selector.Matches(labels) {
			// This template does not want to be applied to this CHI
			log.V(1).M(chi).F().Info("Skip template: %s/%s. Selector: %v does not match labels: %v", useTemplate.Namespace, useTemplate.Name, selector, labels)
			continue // skip to the next template
		}

		//
		// Template is found and matches, let's apply template
		//

		log.V(1).M(chi).F().Info("Apply template: %s/%s. Selector: %v matches labels: %v", useTemplate.Namespace, useTemplate.Name, selector, labels)
		n.ctx.chi = n.mergeCHIFromTemplate(n.ctx.chi, template)

		// And append used template to the list of used templates
		n.ctx.chi.EnsureStatus().PushUsedTemplate(useTemplate)
	} // list of templates

	log.V(1).M(chi).F().Info("Used templates count: %d", n.ctx.chi.EnsureStatus().GetUsedTemplatesCount())
}

func (n *Normalizer) mergeCHIFromTemplate(chi, template *chiV1.ClickHouseInstallation) *chiV1.ClickHouseInstallation {
	// Merge template's Spec over CHI's Spec
	(&chi.Spec).MergeFrom(&template.Spec, chiV1.MergeTypeOverrideByNonEmptyValues)

	// Merge template's Labels over CHI's Labels
	chi.Labels = util.MergeStringMapsOverwrite(
		chi.Labels,
		util.CopyMapFilter(
			template.Labels,
			chop.Config().Label.Include,
			chop.Config().Label.Exclude,
		),
	)

	// Merge template's Annotations over CHI's Annotations
	chi.Annotations = util.MergeStringMapsOverwrite(
		chi.Annotations, util.CopyMapFilter(
			template.Annotations,
			chop.Config().Annotation.Include,
			append(chop.Config().Annotation.Exclude, util.ListSkippedAnnotations()...),
		),
	)

	return chi
}

// normalize normalizes whole CHI.
// Returns normalized CHI
func (n *Normalizer) normalize() (*chiV1.ClickHouseInstallation, error) {
	// Walk over ChiSpec datatype fields
	n.ctx.chi.Spec.TaskID = n.normalizeTaskID(n.ctx.chi.Spec.TaskID)
	n.ctx.chi.Spec.UseTemplates = n.normalizeUseTemplates(n.ctx.chi.Spec.UseTemplates)
	n.ctx.chi.Spec.Stop = n.normalizeStop(n.ctx.chi.Spec.Stop)
	n.ctx.chi.Spec.Restart = n.normalizeRestart(n.ctx.chi.Spec.Restart)
	n.ctx.chi.Spec.Troubleshoot = n.normalizeTroubleshoot(n.ctx.chi.Spec.Troubleshoot)
	n.ctx.chi.Spec.NamespaceDomainPattern = n.normalizeNamespaceDomainPattern(n.ctx.chi.Spec.NamespaceDomainPattern)
	n.ctx.chi.Spec.Templating = n.normalizeTemplating(n.ctx.chi.Spec.Templating)
	n.ctx.chi.Spec.Reconciling = n.normalizeReconciling(n.ctx.chi.Spec.Reconciling)
	n.ctx.chi.Spec.Defaults = n.normalizeDefaults(n.ctx.chi.Spec.Defaults)
	n.ctx.chi.Spec.Configuration = n.normalizeConfiguration(n.ctx.chi.Spec.Configuration)
	n.ctx.chi.Spec.Templates = n.normalizeTemplates(n.ctx.chi.Spec.Templates)
	// UseTemplates already done

	n.finalizeCHI()
	n.fillStatus()

	return n.ctx.chi, nil
}

// finalizeCHI performs some finalization tasks, which should be done after CHI is normalized
func (n *Normalizer) finalizeCHI() {
	n.ctx.chi.FillSelfCalculatedAddressInfo()
	n.ctx.chi.FillCHIPointer()
	n.ctx.chi.WalkHosts(func(host *chiV1.ChiHost) error {
		hostTemplate := n.getHostTemplate(host)
		hostApplyHostTemplate(host, hostTemplate)
		return nil
	})
	n.fillCHIAddressInfo()
}

// fillCHIAddressInfo
func (n *Normalizer) fillCHIAddressInfo() {
	n.ctx.chi.WalkHosts(func(host *chiV1.ChiHost) error {
		host.Address.StatefulSet = CreateStatefulSetName(host)
		host.Address.FQDN = CreateFQDN(host)
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
	if host.GetName() == "" {
		host.Name = template.Spec.Name
	}

	host.Insecure = host.Insecure.MergeFrom(template.Spec.Insecure)
	host.Secure = host.Secure.MergeFrom(template.Spec.Secure)

	for _, portDistribution := range template.PortDistribution {
		switch portDistribution.Type {
		case chiV1.PortDistributionUnspecified:
			if chiV1.IsPortUnassigned(host.TCPPort) {
				host.TCPPort = template.Spec.TCPPort
			}
			if chiV1.IsPortUnassigned(host.TLSPort) {
				host.TLSPort = template.Spec.TLSPort
			}
			if chiV1.IsPortUnassigned(host.HTTPPort) {
				host.HTTPPort = template.Spec.HTTPPort
			}
			if chiV1.IsPortUnassigned(host.HTTPSPort) {
				host.HTTPSPort = template.Spec.HTTPSPort
			}
			if chiV1.IsPortUnassigned(host.InterserverHTTPPort) {
				host.InterserverHTTPPort = template.Spec.InterserverHTTPPort
			}
		case chiV1.PortDistributionClusterScopeIndex:
			if chiV1.IsPortUnassigned(host.TCPPort) {
				base := chDefaultTCPPortNumber
				if chiV1.IsPortAssigned(template.Spec.TCPPort) {
					base = template.Spec.TCPPort
				}
				host.TCPPort = base + int32(host.Address.ClusterScopeIndex)
			}
			if chiV1.IsPortUnassigned(host.TLSPort) {
				base := chDefaultTLSPortNumber
				if chiV1.IsPortAssigned(template.Spec.TLSPort) {
					base = template.Spec.TLSPort
				}
				host.TLSPort = base + int32(host.Address.ClusterScopeIndex)
			}
			if chiV1.IsPortUnassigned(host.HTTPPort) {
				base := chDefaultHTTPPortNumber
				if chiV1.IsPortAssigned(template.Spec.HTTPPort) {
					base = template.Spec.HTTPPort
				}
				host.HTTPPort = base + int32(host.Address.ClusterScopeIndex)
			}
			if chiV1.IsPortUnassigned(host.HTTPSPort) {
				base := chDefaultHTTPSPortNumber
				if chiV1.IsPortAssigned(template.Spec.HTTPSPort) {
					base = template.Spec.HTTPSPort
				}
				host.HTTPSPort = base + int32(host.Address.ClusterScopeIndex)
			}
			if chiV1.IsPortUnassigned(host.InterserverHTTPPort) {
				base := chDefaultInterserverHTTPPortNumber
				if chiV1.IsPortAssigned(template.Spec.InterserverHTTPPort) {
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
	// Use host personal settings at first
	ensurePortValuesFromSettings(host, host.GetSettings(), false)
	// Fallback to common settings
	ensurePortValuesFromSettings(host, host.GetCHI().Spec.Configuration.Settings, true)
}

// ensurePortValuesFromSettings fetches port spec from settings, if any provided
func ensurePortValuesFromSettings(host *chiV1.ChiHost, settings *chiV1.Settings, final bool) {
	// For intermittent (non-final) setup fallback values should be from "MustBeAssignedLater" family,
	// because this is not final setup (just intermittent) and all these ports may be overwritten later
	fallbackTCPPort := chiV1.PortUnassigned()
	fallbackTLSPort := chiV1.PortUnassigned()
	fallbackHTTPPort := chiV1.PortUnassigned()
	fallbackHTTPSPort := chiV1.PortUnassigned()
	fallbackInterserverHTTPPort := chiV1.PortUnassigned()

	if final {
		// This is final setup and we need to assign real numbers to ports
		if host.IsInsecure() {
			fallbackTCPPort = chDefaultTCPPortNumber
			fallbackHTTPPort = chDefaultHTTPPortNumber
		}
		if host.IsSecure() {
			fallbackTLSPort = chDefaultTLSPortNumber
			fallbackHTTPSPort = chDefaultHTTPSPortNumber
		}
		fallbackInterserverHTTPPort = chDefaultInterserverHTTPPortNumber
	}

	host.TCPPort = chiV1.EnsurePortValue(host.TCPPort, settings.GetTCPPort(), fallbackTCPPort)
	host.TLSPort = chiV1.EnsurePortValue(host.TLSPort, settings.GetTCPPortSecure(), fallbackTLSPort)
	host.HTTPPort = chiV1.EnsurePortValue(host.HTTPPort, settings.GetHTTPPort(), fallbackHTTPPort)
	host.HTTPSPort = chiV1.EnsurePortValue(host.HTTPSPort, settings.GetHTTPSPort(), fallbackHTTPSPort)
	host.InterserverHTTPPort = chiV1.EnsurePortValue(host.InterserverHTTPPort, settings.GetInterserverHTTPPort(), fallbackInterserverHTTPPort)
}

// fillStatus fills .status section of a CHI with values based on current CHI
func (n *Normalizer) fillStatus() {
	endpoint := CreateCHIServiceFQDN(n.ctx.chi)
	pods := make([]string, 0)
	fqdns := make([]string, 0)
	n.ctx.chi.WalkHosts(func(host *chiV1.ChiHost) error {
		pods = append(pods, CreatePodName(host))
		fqdns = append(fqdns, CreateFQDN(host))
		return nil
	})
	ip, _ := chop.Get().ConfigManager.GetRuntimeParam(chiV1.OPERATOR_POD_IP)
	n.ctx.chi.FillStatus(endpoint, pods, fqdns, ip)
}

// normalizeTaskID normalizes .spec.taskID
func (n *Normalizer) normalizeTaskID(taskID *string) *string {
	if taskID != nil {
		if len(*taskID) > 0 {
			return taskID
		}
	}

	id := uuid.New().String()
	return &id
}

// normalizeStop normalizes .spec.stop
func (n *Normalizer) normalizeStop(stop *chiV1.StringBool) *chiV1.StringBool {
	if stop.IsValid() {
		// It is bool, use as it is
		return stop
	}

	// In case it is unknown value - just use set it to false
	return chiV1.NewStringBool(false)
}

// normalizeRestart normalizes .spec.restart
func (n *Normalizer) normalizeRestart(restart string) string {
	switch strings.ToLower(restart) {
	case strings.ToLower(chiV1.RestartAll):
		return chiV1.RestartAll
	case strings.ToLower(chiV1.RestartRollingUpdate):
		return chiV1.RestartRollingUpdate
	}

	// In case it is unknown value - just use empty
	return ""
}

// normalizeTroubleshoot normalizes .spec.stop
func (n *Normalizer) normalizeTroubleshoot(troubleshoot *chiV1.StringBool) *chiV1.StringBool {
	if troubleshoot.IsValid() {
		// It is bool, use as it is
		return troubleshoot
	}

	// In case it is unknown value - just use set it to false
	return chiV1.NewStringBool(false)
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
	defaults.ReplicasUseFQDN = defaults.ReplicasUseFQDN.Normalize(false)
	// Ensure field
	if defaults.DistributedDDL == nil {
		//defaults.DistributedDDL = chiV1.NewChiDistributedDDL()
	}
	// Ensure field
	if defaults.StorageManagement == nil {
		defaults.StorageManagement = chiV1.NewStorageManagement()
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
		//templates = chiV1.NewChiTemplates()
		return nil
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
	case strings.ToLower(chiV1.TemplatingPolicyManual):
		// Known policy, overwrite it to ensure case-ness
		templating.SetPolicy(chiV1.TemplatingPolicyManual)
	case strings.ToLower(chiV1.TemplatingPolicyAuto):
		// Known policy, overwrite it to ensure case-ness
		templating.SetPolicy(chiV1.TemplatingPolicyAuto)
	default:
		// Unknown policy, fallback to default
		templating.SetPolicy(chiV1.TemplatingPolicyManual)
	}
	return templating
}

// normalizeReconciling normalizes .spec.reconciling
func (n *Normalizer) normalizeReconciling(reconciling *chiV1.ChiReconciling) *chiV1.ChiReconciling {
	if reconciling == nil {
		reconciling = chiV1.NewChiReconciling().SetDefaults()
	}
	switch strings.ToLower(reconciling.GetPolicy()) {
	case strings.ToLower(chiV1.ReconcilingPolicyWait):
		// Known policy, overwrite it to ensure case-ness
		reconciling.SetPolicy(chiV1.ReconcilingPolicyWait)
	case strings.ToLower(chiV1.ReconcilingPolicyNoWait):
		// Known policy, overwrite it to ensure case-ness
		reconciling.SetPolicy(chiV1.ReconcilingPolicyNoWait)
	default:
		// Unknown policy, fallback to default
		reconciling.SetPolicy(chiV1.ReconcilingPolicyUnspecified)
	}
	reconciling.Cleanup = n.normalizeReconcilingCleanup(reconciling.Cleanup)
	return reconciling
}

func (n *Normalizer) normalizeReconcilingCleanup(cleanup *chiV1.ChiCleanup) *chiV1.ChiCleanup {
	if cleanup == nil {
		cleanup = chiV1.NewChiCleanup()
	}

	if cleanup.UnknownObjects == nil {
		cleanup.UnknownObjects = cleanup.DefaultUnknownObjects()
	}
	n.normalizeCleanup(&cleanup.UnknownObjects.StatefulSet, chiV1.ObjectsCleanupDelete)
	n.normalizeCleanup(&cleanup.UnknownObjects.PVC, chiV1.ObjectsCleanupDelete)
	n.normalizeCleanup(&cleanup.UnknownObjects.ConfigMap, chiV1.ObjectsCleanupDelete)
	n.normalizeCleanup(&cleanup.UnknownObjects.Service, chiV1.ObjectsCleanupDelete)

	if cleanup.ReconcileFailedObjects == nil {
		cleanup.ReconcileFailedObjects = cleanup.DefaultReconcileFailedObjects()
	}
	n.normalizeCleanup(&cleanup.ReconcileFailedObjects.StatefulSet, chiV1.ObjectsCleanupRetain)
	n.normalizeCleanup(&cleanup.ReconcileFailedObjects.PVC, chiV1.ObjectsCleanupRetain)
	n.normalizeCleanup(&cleanup.ReconcileFailedObjects.ConfigMap, chiV1.ObjectsCleanupRetain)
	n.normalizeCleanup(&cleanup.ReconcileFailedObjects.Service, chiV1.ObjectsCleanupRetain)
	return cleanup
}

func (n *Normalizer) normalizeCleanup(str *string, value string) {
	switch *str {
	case
		chiV1.ObjectsCleanupRetain,
		chiV1.ObjectsCleanupDelete:
	default:
		*str = value
	}
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
	n.ctx.chi.Spec.Templates.EnsureHostTemplatesIndex().Set(template.Name, template)
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
		template.Zone.Key = corev1.LabelTopologyZone
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
	template.Spec.Affinity = MergeAffinity(template.Spec.Affinity, NewAffinity(template))

	// In case we have hostNetwork specified, we need to have ClusterFirstWithHostNet DNS policy, because of
	// https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/#pod-s-dns-policy
	// which tells:  For Pods running with hostNetwork, you should explicitly set its DNS policy “ClusterFirstWithHostNet”.
	if template.Spec.HostNetwork {
		template.Spec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
	}

	// Introduce PodTemplate into Index
	n.ctx.chi.Spec.Templates.EnsurePodTemplatesIndex().Set(template.Name, template)
}

const defaultTopologyKey = corev1.LabelHostname

func (n *Normalizer) normalizePodDistribution(podDistribution *chiV1.ChiPodDistribution) []chiV1.ChiPodDistribution {
	if podDistribution.TopologyKey == "" {
		podDistribution.TopologyKey = defaultTopologyKey
	}
	switch podDistribution.Type {
	case
		chiV1.PodDistributionUnspecified,
		// AntiAffinity section
		chiV1.PodDistributionClickHouseAntiAffinity,
		chiV1.PodDistributionShardAntiAffinity,
		chiV1.PodDistributionReplicaAntiAffinity:
		// PodDistribution is known
		if podDistribution.Scope == "" {
			podDistribution.Scope = chiV1.PodDistributionScopeCluster
		}
		return nil
	case
		chiV1.PodDistributionAnotherNamespaceAntiAffinity,
		chiV1.PodDistributionAnotherClickHouseInstallationAntiAffinity,
		chiV1.PodDistributionAnotherClusterAntiAffinity:
		// PodDistribution is known
		return nil
	case
		chiV1.PodDistributionMaxNumberPerNode:
		// PodDistribution is known
		if podDistribution.Number < 0 {
			podDistribution.Number = 0
		}
		return nil
	case
		// Affinity section
		chiV1.PodDistributionNamespaceAffinity,
		chiV1.PodDistributionClickHouseInstallationAffinity,
		chiV1.PodDistributionClusterAffinity,
		chiV1.PodDistributionShardAffinity,
		chiV1.PodDistributionReplicaAffinity,
		chiV1.PodDistributionPreviousTailAffinity:
		// PodDistribution is known
		return nil

	case chiV1.PodDistributionCircularReplication:
		// PodDistribution is known
		// PodDistributionCircularReplication is a shortcut to simplify complex set of other distributions
		// All shortcuts have to be expanded

		if podDistribution.Scope == "" {
			podDistribution.Scope = chiV1.PodDistributionScopeCluster
		}

		// TODO need to support multi-cluster
		cluster := n.ctx.chi.Spec.Configuration.Clusters[0]

		// Expand shortcut
		return []chiV1.ChiPodDistribution{
			{
				Type:  chiV1.PodDistributionShardAntiAffinity,
				Scope: podDistribution.Scope,
			},
			{
				Type:  chiV1.PodDistributionReplicaAntiAffinity,
				Scope: podDistribution.Scope,
			},
			{
				Type:   chiV1.PodDistributionMaxNumberPerNode,
				Scope:  podDistribution.Scope,
				Number: cluster.Layout.ReplicasCount,
			},

			{
				Type: chiV1.PodDistributionPreviousTailAffinity,
			},

			{
				Type: chiV1.PodDistributionNamespaceAffinity,
			},
			{
				Type: chiV1.PodDistributionClickHouseInstallationAffinity,
			},
			{
				Type: chiV1.PodDistributionClusterAffinity,
			},
		}
	}

	// PodDistribution is not known
	podDistribution.Type = chiV1.PodDistributionUnspecified
	return nil
}

// normalizeVolumeClaimTemplate normalizes .spec.templates.volumeClaimTemplates
func (n *Normalizer) normalizeVolumeClaimTemplate(template *chiV1.ChiVolumeClaimTemplate) {
	// Check name
	// Skip for now

	// StorageManagement
	n.normalizeStorageManagement(&template.StorageManagement)

	// Check Spec
	// Skip for now

	// Introduce VolumeClaimTemplate into Index
	n.ctx.chi.Spec.Templates.EnsureVolumeClaimTemplatesIndex().Set(template.Name, template)
}

// normalizeStorageManagement normalizes StorageManagement
func (n *Normalizer) normalizeStorageManagement(storage *chiV1.StorageManagement) {
	// Check PVCProvisioner
	if !storage.PVCProvisioner.IsValid() {
		storage.PVCProvisioner = chiV1.PVCProvisionerUnspecified
	}

	// Check PVCReclaimPolicy
	if !storage.PVCReclaimPolicy.IsValid() {
		storage.PVCReclaimPolicy = chiV1.PVCReclaimPolicyUnspecified
	}
}

// normalizeServiceTemplate normalizes .spec.templates.serviceTemplates
func (n *Normalizer) normalizeServiceTemplate(template *chiV1.ChiServiceTemplate) {
	// Check name
	// Check GenerateName
	// Check ObjectMeta
	// Check Spec

	// Introduce ServiceClaimTemplate into Index
	n.ctx.chi.Spec.Templates.EnsureServiceTemplatesIndex().Set(template.Name, template)
}

// normalizeUseTemplates normalizes list of templates use specifications
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
func (n *Normalizer) normalizeClusters(clusters []*chiV1.Cluster) []*chiV1.Cluster {
	// We need to have at least one cluster available
	clusters = n.ensureClusters(clusters)

	// Normalize all clusters
	for i := range clusters {
		clusters[i] = n.normalizeCluster(clusters[i])
	}

	return clusters
}

// newDefaultCluster
func (n *Normalizer) newDefaultCluster() *chiV1.Cluster {
	return &chiV1.Cluster{
		Name: "cluster",
	}
}

// ensureClusters
func (n *Normalizer) ensureClusters(clusters []*chiV1.Cluster) []*chiV1.Cluster {
	if len(clusters) > 0 {
		return clusters
	}

	if n.ctx.options.WithDefaultCluster {
		return []*chiV1.Cluster{
			n.newDefaultCluster(),
		}
	}

	return []*chiV1.Cluster{}
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
		if chiV1.IsPortUnassigned(node.Port) {
			node.Port = zkDefaultPort
		}
	}

	// In case no ZK root specified - assign '/clickhouse/{namespace}/{chi name}'
	//if zk.Root == "" {
	//	zk.Root = fmt.Sprintf(zkDefaultRootTemplate, n.chi.Namespace, n.chi.Name)
	//}

	return zk
}

// substWithSecretField substitute users settings field with value from k8s secret
func (n *Normalizer) substWithSecretField(users *chiV1.Settings, username string, userSettingsField, userSettingsK8SSecretField string) bool {
	// Has to have source field specified
	if !users.Has(username + "/" + userSettingsK8SSecretField) {
		return false
	}

	// Anyway remove source field, it should not be included into final ClickHouse config,
	// because these source fields are synthetic ones (clickhouse does not know them).
	defer users.Delete(username + "/" + userSettingsK8SSecretField)

	secretFieldValue, err := n.fetchSecretFieldValue(users, username, userSettingsK8SSecretField)
	if err != nil {
		return false
	}

	users.Set(username+"/"+userSettingsField, chiV1.NewSettingScalar(secretFieldValue))
	return true
}

// substWithSecretEnvField substitute users settings field with value from k8s secret stored in ENV var
func (n *Normalizer) substWithSecretEnvField(users *chiV1.Settings, username string, userSettingsField, userSettingsK8SSecretField string) bool {
	// Fetch secret name and key within secret
	_, secretName, key, err := parseSecretFieldAddress(users, username, userSettingsK8SSecretField)
	if err != nil {
		return false
	}

	// Subst plaintext field with secret field
	if !n.substWithSecretField(users, username, userSettingsField, userSettingsK8SSecretField) {
		return false
	}

	// ENV VAR name and value
	envVarName := username + "_" + userSettingsField
	n.appendEnvVar(
		corev1.EnvVar{
			Name: envVarName,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: secretName,
					},
					Key: key,
				},
			},
		},
	)

	// Replace setting with empty value and reference to ENV VAR
	users.Set(username+"/"+userSettingsField, chiV1.NewSettingScalar("").SetAttribute("from_env", envVarName))
	return true
}

const internodeClusterSecretEnvName = "CLICKHOUSE_INTERNODE_CLUSTER_SECRET"

func (n *Normalizer) appendClusterSecretEnvVar(cluster *chiV1.Cluster) {
	switch cluster.Secret.Source() {
	case chiV1.ClusterSecretSourcePlaintext:
		// Secret has explicit value, it is not passed via ENV vars
		// Do nothing here
	case chiV1.ClusterSecretSourceSecretRef:
		// Secret has explicit SecretKeyRef
		// Set the password for internode communication using an ENV VAR
		n.appendEnvVar(
			corev1.EnvVar{
				Name: internodeClusterSecretEnvName,
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: cluster.Secret.GetSecretKeyRef(),
				},
			},
		)
	case chiV1.ClusterSecretSourceAuto:
		// Secret is auto-generated
		// Set the password for internode communication using an ENV VAR
		n.appendEnvVar(
			corev1.EnvVar{
				Name: internodeClusterSecretEnvName,
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: cluster.Secret.GetAutoSecretKeyRef(CreateClusterAutoSecretName(cluster)),
				},
			},
		)
	}
}

func (n *Normalizer) appendEnvVar(envVar corev1.EnvVar) {
	// Sanity check
	if envVar.Name == "" {
		return
	}

	for _, existingEnvVar := range n.ctx.chi.Attributes.ExchangeEnv {
		if existingEnvVar.Name == envVar.Name {
			// Such a variable already exists
			return
		}
	}

	n.ctx.chi.Attributes.ExchangeEnv = append(n.ctx.chi.Attributes.ExchangeEnv, envVar)
}

var (
	// ErrSecretFieldNotFound specifies error when secret key is not found
	ErrSecretFieldNotFound = fmt.Errorf("not found")
)

// parseSecretFieldAddress parses address into namespace, name, key triple
func parseSecretFieldAddress(users *chiV1.Settings, username, userSettingsK8SSecretField string) (string, string, string, error) {
	settingsPath := username + "/" + userSettingsK8SSecretField
	secretFieldAddress := users.Get(settingsPath).String()

	// Sanity check. In case secretFieldAddress not specified nothing to do here
	if secretFieldAddress == "" {
		return "", "", "", ErrSecretFieldNotFound
	}

	// Extract secret's namespace and name and then field name within the secret,
	// by splitting namespace/name/field (aka key) triple. Namespace can be omitted in the settings
	var namespace, name, key string
	switch tags := strings.Split(secretFieldAddress, "/"); len(tags) {
	case 2:
		// Assume namespace is omitted. Expect to have name/key pair
		namespace = chop.Config().Runtime.Namespace
		name = tags[0]
		key = tags[1]
	case 3:
		// All components are in place. Expect to have namespace/name/key triple
		namespace = tags[0]
		name = tags[1]
		key = tags[2]
	default:
		// Skip incorrect entry
		log.V(1).Warning("unable to parse secret field address: '%s:%s'", settingsPath, secretFieldAddress)
		return "", "", "", ErrSecretFieldNotFound
	}

	// Sanity check
	if (namespace == "") || (name == "") || (key == "") {
		log.V(1).M(namespace, name).F().Warning("incorrect secret field address: '%s:%s'", settingsPath, secretFieldAddress)
		return "", "", "", ErrSecretFieldNotFound
	}

	return namespace, name, key, nil
}

// fetchSecretFieldValue fetches the value of the specified field in the specified secret
func (n *Normalizer) fetchSecretFieldValue(users *chiV1.Settings, username, userSettingsK8SSecretField string) (string, error) {
	// Fetch address of the field
	namespace, name, key, err := parseSecretFieldAddress(users, username, userSettingsK8SSecretField)
	if err != nil {
		return "", err
	}

	secret, err := n.kubeClient.CoreV1().Secrets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		log.V(1).M(namespace, name).F().Info("unable to read secret %v", err)
		return "", ErrSecretFieldNotFound
	}

	// Find the field within the secret
	for k, value := range secret.Data {
		if key == k {
			return string(value), nil
		}
	}

	log.V(1).M(namespace, name).F().Info("unable to locate in specified address (namespace/name/key triple) from: %s/%s", username, userSettingsK8SSecretField)
	return "", ErrSecretFieldNotFound
}

// normalizeUsersList extracts usernames from provided 'users' settings
func (n *Normalizer) normalizeUsersList(users *chiV1.Settings, extra ...string) (usernames []string) {
	// Extract username from path
	usernameMap := make(map[string]bool)
	users.Walk(func(path string, _ *chiV1.Setting) {
		// Split username/action into username and all the rest. Ex. 'admin/password', 'admin/networks/ip'
		tags := strings.Split(path, "/")

		// Basic sanity check - need to have at least "username/something" pair
		// This "something" part is not used, it just has to be
		if len(tags) < 2 {
			// Skip incorrect entry
			return
		}

		// Register username
		username := tags[0]
		usernameMap[username] = true
	})

	// Add extra users
	for _, username := range extra {
		if username != "" {
			usernameMap[username] = true
		}
	}

	// Make sorted list of unique usernames
	for username := range usernameMap {
		usernames = append(usernames, username)
	}
	sort.Strings(usernames)

	return usernames
}

const defaultUsername = "default"
const chopProfile = "clickhouse_operator"

// normalizeConfigurationUsers normalizes .spec.configuration.users
func (n *Normalizer) normalizeConfigurationUsers(users *chiV1.Settings) *chiV1.Settings {
	// Ensure and normalize user settings
	if users == nil {
		users = chiV1.NewSettings()
	}
	users.Normalize()

	// Add special "default" user to the list of users, which is used/required for:
	// 1. ClickHouse hosts to communicate with each other
	// 2. Specify host_regexp for default user as "allowed hosts to visit from"
	// Add special "chop" user to the list of users, which is used/required for:
	// 1. Operator to communicate with hosts
	usernames := n.normalizeUsersList(
		// Original users list
		users,
		// Add default user which always exists
		defaultUsername,
		// Add CHOp user
		chop.Config().ClickHouse.Access.Username,
	)

	// Normalize each user in the list of users
	for _, username := range usernames {
		n.normalizeConfigurationUser(users, username)
	}

	// Remove plain password for default user
	n.removePlainPassword(users, defaultUsername)

	return users
}

func (n *Normalizer) removePlainPassword(users *chiV1.Settings, username string) {
	if users.Has(username+"/password_double_sha1_hex") || users.Has(username+"/password_sha256_hex") {
		// If user has encrypted password specified, we need to delete existing plaintext password.
		// Set "remove" flag for user's "password", which is specified as empty in stock ClickHouse users.xml,
		// thus we need to overwrite it.
		users.Set(username+"/password", chiV1.NewSettingScalar("").SetAttribute("remove", "1"))
	}
}

func (n *Normalizer) normalizeConfigurationUser(users *chiV1.Settings, username string) {
	n.normalizeConfigurationUserEnsureMandatorySections(users, username)
	n.normalizeConfigurationUserPassword(users, username)
}

func (n *Normalizer) normalizeConfigurationUserEnsureMandatorySections(users *chiV1.Settings, username string) {
	chopUsername := chop.Config().ClickHouse.Access.Username
	//
	// Ensure each user has mandatory sections:
	//
	// 1. user/profile
	// 2. user/quota
	// 3. user/networks/ip
	// 4. user/networks/host_regexp
	profile := chop.Config().ClickHouse.Config.User.Default.Profile
	quota := chop.Config().ClickHouse.Config.User.Default.Quota
	ips := append([]string{}, chop.Config().ClickHouse.Config.User.Default.NetworksIP...)
	regexp := CreatePodHostnameRegexp(n.ctx.chi, chop.Config().ClickHouse.Config.Network.HostRegexpTemplate)

	// Some users may have special options
	switch username {
	case defaultUsername:
		ips = append(ips, n.ctx.options.DefaultUserAdditionalIPs...)
		if !n.ctx.options.DefaultUserInsertHostRegex {
			regexp = ""
		}
	case chopUsername:
		ip, _ := chop.Get().ConfigManager.GetRuntimeParam(chiV1.OPERATOR_POD_IP)

		profile = chopProfile
		quota = ""
		ips = []string{ip}
		regexp = ""
	}

	// Ensure required values are in place and apply non-empty values in case no own value(s) provided
	if profile != "" {
		users.SetIfNotExists(username+"/profile", chiV1.NewSettingScalar(profile))
	}
	if quota != "" {
		users.SetIfNotExists(username+"/quota", chiV1.NewSettingScalar(quota))
	}
	if len(ips) > 0 {
		users.Set(username+"/networks/ip", chiV1.NewSettingVector(ips).MergeFrom(users.Get(username+"/networks/ip")))
	}
	if regexp != "" {
		users.SetIfNotExists(username+"/networks/host_regexp", chiV1.NewSettingScalar(regexp))
	}
}

func (n *Normalizer) normalizeConfigurationUserPassword(users *chiV1.Settings, username string) {
	//
	// Deal with passwords
	//

	// Values from the secret have higher priority
	n.substWithSecretField(users, username, "password", "k8s_secret_password")
	n.substWithSecretField(users, username, "password_sha256_hex", "k8s_secret_password_sha256_hex")
	n.substWithSecretField(users, username, "password_double_sha1_hex", "k8s_secret_password_double_sha1_hex")

	// Values from the secret passed via ENV have even higher priority
	n.substWithSecretEnvField(users, username, "password", "k8s_secret_env_password")
	n.substWithSecretEnvField(users, username, "password_sha256_hex", "k8s_secret_env_password_sha256_hex")
	n.substWithSecretEnvField(users, username, "password_double_sha1_hex", "k8s_secret_env_password_double_sha1_hex")

	// Out of all passwords, password_double_sha1_hex has top priority, thus keep it only
	if users.Has(username + "/password_double_sha1_hex") {
		users.Delete(username + "/password_sha256_hex")
		users.Delete(username + "/password")
		return // move to the next user
	}

	// Than goes password_sha256_hex, thus keep it only
	if users.Has(username + "/password_sha256_hex") {
		users.Delete(username + "/password_double_sha1_hex")
		users.Delete(username + "/password")
		return // move to the next user
	}

	// From now on we either have a plaintext password specified, or no password at all

	if users.Get(username + "/password").HasAttributes() {
		// Have plaintext password explicitly specified via ENV vars
		// This is still OK
		return // move to the next user
	}

	// From now on we either have plaintext password specified as an explicit string, or no password at all

	passwordPlaintext := users.Get(username + "/password").String()

	// Apply default password for password-less non-default users
	// 1. NB "default" user keeps empty password in here.
	// 2. ClickHouse user gets password from his section of chop configuration
	// 3. All the rest users get default password
	if passwordPlaintext == "" {
		switch username {
		case defaultUsername:
			// NB "default" user keeps empty password in here.
		case chop.Config().ClickHouse.Access.Username:
			// User used by CHOp to access instances
			passwordPlaintext = chop.Config().ClickHouse.Access.Password
		default:
			passwordPlaintext = chop.Config().ClickHouse.Config.User.Default.Password
		}
	}

	// Replace plaintext password with encrypted
	if passwordPlaintext != "" {
		passwordSHA256 := sha256.Sum256([]byte(passwordPlaintext))
		users.Set(username+"/password_sha256_hex", chiV1.NewSettingScalar(hex.EncodeToString(passwordSHA256[:])))
		// And keep only one password specification
		users.Delete(username + "/password_double_sha1_hex")
		users.Delete(username + "/password")
	}

	// NB "default" user may keep empty password in here.
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
func (n *Normalizer) normalizeCluster(cluster *chiV1.Cluster) *chiV1.Cluster {
	if cluster == nil {
		cluster = n.newDefaultCluster()
	}

	cluster.CHI = n.ctx.chi

	// Inherit from .spec.configuration.zookeeper
	cluster.InheritZookeeperFrom(n.ctx.chi)
	// Inherit from .spec.configuration.files
	cluster.InheritFilesFrom(n.ctx.chi)
	// Inherit from .spec.defaults
	cluster.InheritTemplatesFrom(n.ctx.chi)

	cluster.Zookeeper = n.normalizeConfigurationZookeeper(cluster.Zookeeper)
	cluster.Settings = n.normalizeConfigurationSettings(cluster.Settings)
	cluster.Files = n.normalizeConfigurationFiles(cluster.Files)

	cluster.SchemaPolicy = n.normalizeClusterSchemaPolicy(cluster.SchemaPolicy)

	if cluster.Layout == nil {
		cluster.Layout = chiV1.NewChiClusterLayout()
	}
	cluster.FillShardReplicaSpecified()
	cluster.Layout = n.normalizeClusterLayoutShardsCountAndReplicasCount(cluster.Layout)
	n.ensureClusterLayoutShards(cluster.Layout)
	n.ensureClusterLayoutReplicas(cluster.Layout)

	n.createHostsField(cluster)
	n.appendClusterSecretEnvVar(cluster)

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
func (n *Normalizer) createHostsField(cluster *chiV1.Cluster) {
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

// Values for Schema Policy
const (
	SchemaPolicyReplicaNone                = "None"
	SchemaPolicyReplicaAll                 = "All"
	SchemaPolicyShardNone                  = "None"
	SchemaPolicyShardAll                   = "All"
	SchemaPolicyShardDistributedTablesOnly = "DistributedTablesOnly"
)

// normalizeClusterLayoutShardsCountAndReplicasCount ensures at least 1 shard and 1 replica counters
func (n *Normalizer) normalizeClusterSchemaPolicy(policy *chiV1.SchemaPolicy) *chiV1.SchemaPolicy {
	if policy == nil {
		policy = chiV1.NewClusterSchemaPolicy()
	}

	switch strings.ToLower(policy.Replica) {
	case strings.ToLower(SchemaPolicyReplicaNone):
		policy.Replica = SchemaPolicyReplicaNone
	case strings.ToLower(SchemaPolicyReplicaAll):
		policy.Replica = SchemaPolicyReplicaAll
	default:
		policy.Replica = SchemaPolicyReplicaAll
	}

	switch strings.ToLower(policy.Shard) {
	case strings.ToLower(SchemaPolicyShardNone):
		policy.Shard = SchemaPolicyShardNone
	case strings.ToLower(SchemaPolicyShardAll):
		policy.Shard = SchemaPolicyShardAll
	case strings.ToLower(SchemaPolicyShardDistributedTablesOnly):
		policy.Shard = SchemaPolicyShardDistributedTablesOnly
	default:
		policy.Shard = SchemaPolicyShardAll
	}

	return policy
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
func (n *Normalizer) normalizeShard(shard *chiV1.ChiShard, cluster *chiV1.Cluster, shardIndex int) {
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
func (n *Normalizer) normalizeReplica(replica *chiV1.ChiReplica, cluster *chiV1.Cluster, replicaIndex int) {
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
func (n *Normalizer) normalizeShardHosts(shard *chiV1.ChiShard, cluster *chiV1.Cluster, shardIndex int) {
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
func (n *Normalizer) normalizeReplicaHosts(replica *chiV1.ChiReplica, cluster *chiV1.Cluster, replicaIndex int) {
	// Use hosts from HostsField
	replica.Hosts = nil
	for len(replica.Hosts) < replica.ShardsCount {
		// We still have some assumed hosts in this replica - let's add it as shardIndex
		shardIndex := len(replica.Hosts)
		// Check whether we have this host in HostsField
		host := cluster.GetOrCreateHost(shardIndex, replicaIndex)
		replica.Hosts = append(replica.Hosts, host)
	}
}

// normalizeHost normalizes a host/replica
func (n *Normalizer) normalizeHost(
	host *chiV1.ChiHost,
	shard *chiV1.ChiShard,
	replica *chiV1.ChiReplica,
	cluster *chiV1.Cluster,
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
	if (len(host.GetName()) > 0) && !IsAutoGeneratedHostName(host.GetName(), host, shard, shardIndex, replica, replicaIndex) {
		// Has explicitly specified name already
		return
	}

	host.Name = CreateHostName(host, shard, shardIndex, replica, replicaIndex)
}

// normalizeHostPorts ensures chiV1.ChiReplica.Port is reasonable
func (n *Normalizer) normalizeHostPorts(host *chiV1.ChiHost) {
	// Deprecated
	if chiV1.IsPortInvalid(host.Port) {
		host.Port = chiV1.PortUnassigned()
	}

	if chiV1.IsPortInvalid(host.TCPPort) {
		host.TCPPort = chiV1.PortUnassigned()
	}

	if chiV1.IsPortInvalid(host.TLSPort) {
		host.TLSPort = chiV1.PortUnassigned()
	}

	if chiV1.IsPortInvalid(host.HTTPPort) {
		host.HTTPPort = chiV1.PortUnassigned()
	}

	if chiV1.IsPortInvalid(host.HTTPSPort) {
		host.HTTPSPort = chiV1.PortUnassigned()
	}

	if chiV1.IsPortInvalid(host.InterserverHTTPPort) {
		host.InterserverHTTPPort = chiV1.PortUnassigned()
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
	shard.InternalReplication = shard.InternalReplication.Normalize(defaultInternalReplication)
}
