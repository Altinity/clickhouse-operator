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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/google/uuid"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube "k8s.io/client-go/kubernetes"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	model "github.com/altinity/clickhouse-operator/pkg/model/chi"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/creator"
	entitiesNormalizer "github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer/entities"
	templatesNormalizer "github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer/templates"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Normalizer specifies structures normalizer
type Normalizer struct {
	kubeClient kube.Interface
	ctx        *Context
}

// NewNormalizer creates new normalizer
func NewNormalizer(kubeClient kube.Interface) *Normalizer {
	return &Normalizer{
		kubeClient: kubeClient,
	}
}

func newCHI() *api.ClickHouseInstallation {
	return &api.ClickHouseInstallation{
		TypeMeta: meta.TypeMeta{
			Kind:       api.ClickHouseInstallationCRDResourceKind,
			APIVersion: api.SchemeGroupVersion.String(),
		},
	}
}

// CreateTemplatedCHI produces ready-to-use CHI object
func (n *Normalizer) CreateTemplatedCHI(
	chi *api.ClickHouseInstallation,
	options *Options,
) (*api.ClickHouseInstallation, error) {
	// New CHI starts with new context
	n.ctx = NewContext(options)

	// Normalize start CHI
	chi = n.normalizeStartCHI(chi)
	// Create new chi that will be populated with data during normalization process
	n.ctx.chi = n.createBaseCHI()

	// At this moment context chi is either newly created 'empty' CHI or a system-wide template

	// Apply templates - both auto and explicitly requested - on top of context chi
	n.applyCHITemplates(chi)

	// After all templates applied, place provided CHI on top of the whole stack
	n.ctx.chi.MergeFrom(chi, api.MergeTypeOverrideByNonEmptyValues)

	return n.normalize()
}

func (n *Normalizer) normalizeStartCHI(chi *api.ClickHouseInstallation) *api.ClickHouseInstallation {
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

func (n *Normalizer) createBaseCHI() *api.ClickHouseInstallation {
	// What base should be used to create CHI
	if chop.Config().Template.CHI.Runtime.Template == nil {
		// No template specified - start with clear page
		return newCHI()
	} else {
		// Template specified - start with template
		return chop.Config().Template.CHI.Runtime.Template.DeepCopy()
	}
}

// prepareListOfCHITemplates prepares list of CHI templates to be used by the CHI
func (n *Normalizer) prepareListOfCHITemplates(chi *api.ClickHouseInstallation) (useTemplates []api.ChiTemplateRef) {
	// 1. Get list of auto templates available
	if autoTemplates := chop.Config().GetAutoTemplates(); len(autoTemplates) > 0 {
		log.V(1).M(chi).F().Info("Found auto-templates num: %d", len(autoTemplates))
		for _, template := range autoTemplates {
			log.V(1).M(chi).F().Info(
				"Adding auto-template to list of applicable templates: %s/%s ",
				template.Namespace, template.Name)
			useTemplates = append(useTemplates, api.ChiTemplateRef{
				Name:      template.Name,
				Namespace: template.Namespace,
				UseType:   model.UseTypeMerge,
			})
		}
	}

	// 2. Append templates which are explicitly requested by the CHI
	if len(chi.Spec.UseTemplates) > 0 {
		log.V(1).M(chi).F().Info("Found manual-templates num: %d", len(chi.Spec.UseTemplates))
		useTemplates = append(useTemplates, chi.Spec.UseTemplates...)
	}

	n.normalizeUseTemplates(useTemplates)

	log.V(1).M(chi).F().Info("Found applicable templates num: %d", len(useTemplates))
	return useTemplates
}

// applyCHITemplates applies CHI templates over n.ctx.chi
func (n *Normalizer) applyCHITemplates(chi *api.ClickHouseInstallation) {
	// At this moment n.chi is either newly created 'empty' CHI or a system-wide template

	// useTemplates specifies list of templates to be applied to the CHI
	useTemplates := n.prepareListOfCHITemplates(chi)

	// Apply templates - both auto and explicitly requested
	for i := range useTemplates {
		n.applyCHITemplate(&useTemplates[i], chi)
	}

	log.V(1).M(chi).F().Info("Used templates count: %d", n.ctx.chi.EnsureStatus().GetUsedTemplatesCount())
}

func (n *Normalizer) applyCHITemplate(templateRef *api.ChiTemplateRef, chi *api.ClickHouseInstallation) {
	if templateRef == nil {
		log.Warning("nil templateRef provided")
		return
	}

	// What template are we going to apply?
	template := chop.Config().FindTemplate(templateRef, chi.Namespace)

	if template == nil {
		log.V(1).M(templateRef.Namespace, templateRef.Name).F().Warning(
			"Skip template: %s/%s UNABLE to find listed template. ",
			templateRef.Namespace, templateRef.Name)
		return
	}

	// What CHI this template wants to be applied to?
	// This is determined by matching selector of the template and CHI's labels
	// Convenience wrapper
	if selector := template.Spec.Templating.GetCHISelector(); selector.Matches(chi.Labels) {
		log.V(1).M(templateRef.Namespace, templateRef.Name).F().Info(
			"Apply template: %s/%s. Selector: %v matches labels: %v",
			templateRef.Namespace, templateRef.Name, selector, chi.Labels)
	} else {
		// This template does not want to be applied to this CHI
		log.V(1).M(templateRef.Namespace, templateRef.Name).F().Info(
			"Skip template: %s/%s. Selector: %v does not match labels: %v",
			templateRef.Namespace, templateRef.Name, selector, chi.Labels)
		return
	}

	//
	// Template is found and matches, let's apply template and append used template to the list of used templates
	//
	n.ctx.chi = n.mergeCHIFromTemplate(n.ctx.chi, template)
	n.ctx.chi.EnsureStatus().PushUsedTemplate(templateRef)
}

func (n *Normalizer) mergeCHIFromTemplate(chi, template *api.ClickHouseInstallation) *api.ClickHouseInstallation {
	// Merge template's Spec over CHI's Spec
	(&chi.Spec).MergeFrom(&template.Spec, api.MergeTypeOverrideByNonEmptyValues)

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
func (n *Normalizer) normalize() (*api.ClickHouseInstallation, error) {
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
	n.ctx.chi.WalkHosts(func(host *api.ChiHost) error {
		hostTemplate := n.getHostTemplate(host)
		hostApplyHostTemplate(host, hostTemplate)
		return nil
	})
	n.fillCHIAddressInfo()
}

// fillCHIAddressInfo
func (n *Normalizer) fillCHIAddressInfo() {
	n.ctx.chi.WalkHosts(func(host *api.ChiHost) error {
		host.Runtime.Address.StatefulSet = model.CreateStatefulSetName(host)
		host.Runtime.Address.FQDN = model.CreateFQDN(host)
		return nil
	})
}

// getHostTemplate gets Host Template to be used to normalize Host
func (n *Normalizer) getHostTemplate(host *api.ChiHost) *api.ChiHostTemplate {
	statefulSetName := model.CreateStatefulSetName(host)

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
			hostTemplate = creator.NewDefaultHostTemplateForHostNetwork(statefulSetName)
		}
	}

	// In case hostTemplate still is not assigned - use default one
	if hostTemplate == nil {
		hostTemplate = creator.NewDefaultHostTemplate(statefulSetName)
	}

	log.V(3).M(host).F().Info("StatefulSet %s use default hostTemplate", statefulSetName)

	return hostTemplate
}

// hostApplyHostTemplate
func hostApplyHostTemplate(host *api.ChiHost, template *api.ChiHostTemplate) {
	if host.GetName() == "" {
		host.Name = template.Spec.Name
	}

	host.Insecure = host.Insecure.MergeFrom(template.Spec.Insecure)
	host.Secure = host.Secure.MergeFrom(template.Spec.Secure)

	for _, portDistribution := range template.PortDistribution {
		switch portDistribution.Type {
		case deployment.PortDistributionUnspecified:
			if api.IsPortUnassigned(host.TCPPort) {
				host.TCPPort = template.Spec.TCPPort
			}
			if api.IsPortUnassigned(host.TLSPort) {
				host.TLSPort = template.Spec.TLSPort
			}
			if api.IsPortUnassigned(host.HTTPPort) {
				host.HTTPPort = template.Spec.HTTPPort
			}
			if api.IsPortUnassigned(host.HTTPSPort) {
				host.HTTPSPort = template.Spec.HTTPSPort
			}
			if api.IsPortUnassigned(host.InterserverHTTPPort) {
				host.InterserverHTTPPort = template.Spec.InterserverHTTPPort
			}
		case deployment.PortDistributionClusterScopeIndex:
			if api.IsPortUnassigned(host.TCPPort) {
				base := model.ChDefaultTCPPortNumber
				if api.IsPortAssigned(template.Spec.TCPPort) {
					base = template.Spec.TCPPort
				}
				host.TCPPort = base + int32(host.Runtime.Address.ClusterScopeIndex)
			}
			if api.IsPortUnassigned(host.TLSPort) {
				base := model.ChDefaultTLSPortNumber
				if api.IsPortAssigned(template.Spec.TLSPort) {
					base = template.Spec.TLSPort
				}
				host.TLSPort = base + int32(host.Runtime.Address.ClusterScopeIndex)
			}
			if api.IsPortUnassigned(host.HTTPPort) {
				base := model.ChDefaultHTTPPortNumber
				if api.IsPortAssigned(template.Spec.HTTPPort) {
					base = template.Spec.HTTPPort
				}
				host.HTTPPort = base + int32(host.Runtime.Address.ClusterScopeIndex)
			}
			if api.IsPortUnassigned(host.HTTPSPort) {
				base := model.ChDefaultHTTPSPortNumber
				if api.IsPortAssigned(template.Spec.HTTPSPort) {
					base = template.Spec.HTTPSPort
				}
				host.HTTPSPort = base + int32(host.Runtime.Address.ClusterScopeIndex)
			}
			if api.IsPortUnassigned(host.InterserverHTTPPort) {
				base := model.ChDefaultInterserverHTTPPortNumber
				if api.IsPortAssigned(template.Spec.InterserverHTTPPort) {
					base = template.Spec.InterserverHTTPPort
				}
				host.InterserverHTTPPort = base + int32(host.Runtime.Address.ClusterScopeIndex)
			}
		}
	}

	hostApplyPortsFromSettings(host)

	host.InheritTemplatesFrom(nil, nil, template)
}

// hostApplyPortsFromSettings
func hostApplyPortsFromSettings(host *api.ChiHost) {
	// Use host personal settings at first
	ensurePortValuesFromSettings(host, host.GetSettings(), false)
	// Fallback to common settings
	ensurePortValuesFromSettings(host, host.GetCHI().Spec.Configuration.Settings, true)
}

// ensurePortValuesFromSettings fetches port spec from settings, if any provided
func ensurePortValuesFromSettings(host *api.ChiHost, settings *api.Settings, final bool) {
	// For intermittent (non-final) setup fallback values should be from "MustBeAssignedLater" family,
	// because this is not final setup (just intermittent) and all these ports may be overwritten later
	fallbackTCPPort := api.PortUnassigned()
	fallbackTLSPort := api.PortUnassigned()
	fallbackHTTPPort := api.PortUnassigned()
	fallbackHTTPSPort := api.PortUnassigned()
	fallbackInterserverHTTPPort := api.PortUnassigned()

	if final {
		// This is final setup and we need to assign real numbers to ports
		if host.IsInsecure() {
			fallbackTCPPort = model.ChDefaultTCPPortNumber
			fallbackHTTPPort = model.ChDefaultHTTPPortNumber
		}
		if host.IsSecure() {
			fallbackTLSPort = model.ChDefaultTLSPortNumber
			fallbackHTTPSPort = model.ChDefaultHTTPSPortNumber
		}
		fallbackInterserverHTTPPort = model.ChDefaultInterserverHTTPPortNumber
	}

	host.TCPPort = api.EnsurePortValue(host.TCPPort, settings.GetTCPPort(), fallbackTCPPort)
	host.TLSPort = api.EnsurePortValue(host.TLSPort, settings.GetTCPPortSecure(), fallbackTLSPort)
	host.HTTPPort = api.EnsurePortValue(host.HTTPPort, settings.GetHTTPPort(), fallbackHTTPPort)
	host.HTTPSPort = api.EnsurePortValue(host.HTTPSPort, settings.GetHTTPSPort(), fallbackHTTPSPort)
	host.InterserverHTTPPort = api.EnsurePortValue(host.InterserverHTTPPort, settings.GetInterserverHTTPPort(), fallbackInterserverHTTPPort)
}

// fillStatus fills .status section of a CHI with values based on current CHI
func (n *Normalizer) fillStatus() {
	endpoint := model.CreateCHIServiceFQDN(n.ctx.chi)
	pods := make([]string, 0)
	fqdns := make([]string, 0)
	n.ctx.chi.WalkHosts(func(host *api.ChiHost) error {
		pods = append(pods, model.CreatePodName(host))
		fqdns = append(fqdns, model.CreateFQDN(host))
		return nil
	})
	ip, _ := chop.Get().ConfigManager.GetRuntimeParam(deployment.OPERATOR_POD_IP)
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
func (n *Normalizer) normalizeStop(stop *api.StringBool) *api.StringBool {
	if stop.IsValid() {
		// It is bool, use as it is
		return stop
	}

	// In case it is unknown value - just use set it to false
	return api.NewStringBool(false)
}

// normalizeRestart normalizes .spec.restart
func (n *Normalizer) normalizeRestart(restart string) string {
	switch strings.ToLower(restart) {
	case strings.ToLower(api.RestartRollingUpdate):
		// Known value, overwrite it to ensure case-ness
		return api.RestartRollingUpdate
	}

	// In case it is unknown value - just use empty
	return ""
}

// normalizeTroubleshoot normalizes .spec.stop
func (n *Normalizer) normalizeTroubleshoot(troubleshoot *api.StringBool) *api.StringBool {
	if troubleshoot.IsValid() {
		// It is bool, use as it is
		return troubleshoot
	}

	// In case it is unknown value - just use set it to false
	return api.NewStringBool(false)
}

func isNamespaceDomainPatternValid(namespaceDomainPattern string) bool {
	if strings.Count(namespaceDomainPattern, "%s") > 1 {
		return false
	} else {
		return true
	}
}

// normalizeNamespaceDomainPattern normalizes .spec.namespaceDomainPattern
func (n *Normalizer) normalizeNamespaceDomainPattern(namespaceDomainPattern string) string {
	if isNamespaceDomainPatternValid(namespaceDomainPattern) {
		return namespaceDomainPattern
	}
	// In case namespaceDomainPattern is not valid - do not use it
	return ""
}

// normalizeDefaults normalizes .spec.defaults
func (n *Normalizer) normalizeDefaults(defaults *api.ChiDefaults) *api.ChiDefaults {
	if defaults == nil {
		defaults = api.NewChiDefaults()
	}
	// Set defaults for CHI object properties
	defaults.ReplicasUseFQDN = defaults.ReplicasUseFQDN.Normalize(false)
	// Ensure field
	if defaults.DistributedDDL == nil {
		//defaults.DistributedDDL = api.NewChiDistributedDDL()
	}
	// Ensure field
	if defaults.StorageManagement == nil {
		defaults.StorageManagement = api.NewStorageManagement()
	}
	// Ensure field
	if defaults.Templates == nil {
		//defaults.Templates = api.NewChiTemplateNames()
	}
	defaults.Templates.HandleDeprecatedFields()
	return defaults
}

// normalizeConfiguration normalizes .spec.configuration
func (n *Normalizer) normalizeConfiguration(conf *api.Configuration) *api.Configuration {
	if conf == nil {
		conf = api.NewConfiguration()
	}
	conf.Zookeeper = n.normalizeConfigurationZookeeper(conf.Zookeeper)
	n.normalizeConfigurationSettingsBased(conf)
	conf.Clusters = n.normalizeClusters(conf.Clusters)
	return conf
}

// normalizeConfigurationSettingsBased normalizes Settings-based configuration
func (n *Normalizer) normalizeConfigurationSettingsBased(conf *api.Configuration) {
	conf.Users = n.normalizeConfigurationUsers(conf.Users)
	conf.Profiles = n.normalizeConfigurationProfiles(conf.Profiles)
	conf.Quotas = n.normalizeConfigurationQuotas(conf.Quotas)
	conf.Settings = n.normalizeConfigurationSettings(conf.Settings)
	conf.Files = n.normalizeConfigurationFiles(conf.Files)
}

// normalizeTemplates normalizes .spec.templates
func (n *Normalizer) normalizeTemplates(templates *api.ChiTemplates) *api.ChiTemplates {
	if templates == nil {
		//templates = api.NewChiTemplates()
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
func (n *Normalizer) normalizeTemplating(templating *api.ChiTemplating) *api.ChiTemplating {
	if templating == nil {
		templating = api.NewChiTemplating()
	}
	switch strings.ToLower(templating.GetPolicy()) {
	case strings.ToLower(api.TemplatingPolicyAuto):
		// Known value, overwrite it to ensure case-ness
		templating.SetPolicy(api.TemplatingPolicyAuto)
	case strings.ToLower(api.TemplatingPolicyManual):
		// Known value, overwrite it to ensure case-ness
		templating.SetPolicy(api.TemplatingPolicyManual)
	default:
		// Unknown value, fallback to default
		templating.SetPolicy(api.TemplatingPolicyManual)
	}
	return templating
}

// normalizeReconciling normalizes .spec.reconciling
func (n *Normalizer) normalizeReconciling(reconciling *api.ChiReconciling) *api.ChiReconciling {
	if reconciling == nil {
		reconciling = api.NewChiReconciling().SetDefaults()
	}
	switch strings.ToLower(reconciling.GetPolicy()) {
	case strings.ToLower(api.ReconcilingPolicyWait):
		// Known value, overwrite it to ensure case-ness
		reconciling.SetPolicy(api.ReconcilingPolicyWait)
	case strings.ToLower(api.ReconcilingPolicyNoWait):
		// Known value, overwrite it to ensure case-ness
		reconciling.SetPolicy(api.ReconcilingPolicyNoWait)
	default:
		// Unknown value, fallback to default
		reconciling.SetPolicy(api.ReconcilingPolicyUnspecified)
	}
	reconciling.Cleanup = n.normalizeReconcilingCleanup(reconciling.Cleanup)
	return reconciling
}

func (n *Normalizer) normalizeReconcilingCleanup(cleanup *api.ChiCleanup) *api.ChiCleanup {
	if cleanup == nil {
		cleanup = api.NewChiCleanup()
	}

	if cleanup.UnknownObjects == nil {
		cleanup.UnknownObjects = cleanup.DefaultUnknownObjects()
	}
	n.normalizeCleanup(&cleanup.UnknownObjects.StatefulSet, api.ObjectsCleanupDelete)
	n.normalizeCleanup(&cleanup.UnknownObjects.PVC, api.ObjectsCleanupDelete)
	n.normalizeCleanup(&cleanup.UnknownObjects.ConfigMap, api.ObjectsCleanupDelete)
	n.normalizeCleanup(&cleanup.UnknownObjects.Service, api.ObjectsCleanupDelete)

	if cleanup.ReconcileFailedObjects == nil {
		cleanup.ReconcileFailedObjects = cleanup.DefaultReconcileFailedObjects()
	}
	n.normalizeCleanup(&cleanup.ReconcileFailedObjects.StatefulSet, api.ObjectsCleanupRetain)
	n.normalizeCleanup(&cleanup.ReconcileFailedObjects.PVC, api.ObjectsCleanupRetain)
	n.normalizeCleanup(&cleanup.ReconcileFailedObjects.ConfigMap, api.ObjectsCleanupRetain)
	n.normalizeCleanup(&cleanup.ReconcileFailedObjects.Service, api.ObjectsCleanupRetain)
	return cleanup
}

func (n *Normalizer) normalizeCleanup(str *string, value string) {
	if str == nil {
		return
	}
	switch strings.ToLower(*str) {
	case strings.ToLower(api.ObjectsCleanupRetain):
		// Known value, overwrite it to ensure case-ness
		*str = api.ObjectsCleanupRetain
	case strings.ToLower(api.ObjectsCleanupDelete):
		// Known value, overwrite it to ensure case-ness
		*str = api.ObjectsCleanupDelete
	default:
		// Unknown value, fallback to default
		*str = value
	}
}

// normalizeHostTemplate normalizes .spec.templates.hostTemplates
func (n *Normalizer) normalizeHostTemplate(template *api.ChiHostTemplate) {
	templatesNormalizer.NormalizeHostTemplate(template)
	// Introduce HostTemplate into Index
	n.ctx.chi.Spec.Templates.EnsureHostTemplatesIndex().Set(template.Name, template)
}

// normalizePodTemplate normalizes .spec.templates.podTemplates
func (n *Normalizer) normalizePodTemplate(template *api.ChiPodTemplate) {
	// TODO need to support multi-cluster
	replicasCount := 1
	if len(n.ctx.chi.Spec.Configuration.Clusters) > 0 {
		replicasCount = n.ctx.chi.Spec.Configuration.Clusters[0].Layout.ReplicasCount
	}
	templatesNormalizer.NormalizePodTemplate(replicasCount, template)
	// Introduce PodTemplate into Index
	n.ctx.chi.Spec.Templates.EnsurePodTemplatesIndex().Set(template.Name, template)
}

// normalizeVolumeClaimTemplate normalizes .spec.templates.volumeClaimTemplates
func (n *Normalizer) normalizeVolumeClaimTemplate(template *api.ChiVolumeClaimTemplate) {
	templatesNormalizer.NormalizeVolumeClaimTemplate(template)
	// Introduce VolumeClaimTemplate into Index
	n.ctx.chi.Spec.Templates.EnsureVolumeClaimTemplatesIndex().Set(template.Name, template)
}

// normalizeServiceTemplate normalizes .spec.templates.serviceTemplates
func (n *Normalizer) normalizeServiceTemplate(template *api.ChiServiceTemplate) {
	templatesNormalizer.NormalizeServiceTemplate(template)
	// Introduce ServiceClaimTemplate into Index
	n.ctx.chi.Spec.Templates.EnsureServiceTemplatesIndex().Set(template.Name, template)
}

// normalizeUseTemplates normalizes list of templates use specifications
func (n *Normalizer) normalizeUseTemplates(useTemplates []api.ChiTemplateRef) []api.ChiTemplateRef {
	for i := range useTemplates {
		useTemplate := &useTemplates[i]
		n.normalizeUseTemplate(useTemplate)
	}
	return useTemplates
}

// normalizeUseTemplate normalizes ChiTemplateRef
func (n *Normalizer) normalizeUseTemplate(templateRef *api.ChiTemplateRef) {
	// Check Name
	if templateRef.Name == "" {
		// This is strange
	}

	// Check Namespace
	if templateRef.Namespace == "" {
		// So far do nothing with empty namespace
	}

	// Ensure UseType
	switch templateRef.UseType {
	case model.UseTypeMerge:
		// Known use type, all is fine, do nothing
	default:
		// Unknown - use default value
		templateRef.UseType = model.UseTypeMerge
	}
}

// normalizeClusters normalizes clusters
func (n *Normalizer) normalizeClusters(clusters []*api.Cluster) []*api.Cluster {
	// We need to have at least one cluster available
	clusters = n.ensureClusters(clusters)

	// Normalize all clusters
	for i := range clusters {
		clusters[i] = n.normalizeCluster(clusters[i])
	}

	return clusters
}

// ensureClusters
func (n *Normalizer) ensureClusters(clusters []*api.Cluster) []*api.Cluster {
	if len(clusters) > 0 {
		return clusters
	}

	if n.ctx.options.WithDefaultCluster {
		return []*api.Cluster{
			creator.NewDefaultCluster(),
		}
	}

	return []*api.Cluster{}
}

// normalizeConfigurationZookeeper normalizes .spec.configuration.zookeeper
func (n *Normalizer) normalizeConfigurationZookeeper(zk *api.ChiZookeeperConfig) *api.ChiZookeeperConfig {
	if zk == nil {
		return nil
	}

	// In case no ZK port specified - assign default
	for i := range zk.Nodes {
		// Convenience wrapper
		node := &zk.Nodes[i]
		if api.IsPortUnassigned(node.Port) {
			node.Port = model.ZkDefaultPort
		}
	}

	// In case no ZK root specified - assign '/clickhouse/{namespace}/{chi name}'
	//if zk.Root == "" {
	//	zk.Root = fmt.Sprintf(zkDefaultRootTemplate, n.chi.Namespace, n.chi.Name)
	//}

	return zk
}

type SettingsSubstitution interface {
	Has(string) bool
	Get(string) *api.Setting
	Set(string, *api.Setting) *api.Settings
	Delete(string)
	Name2Key(string) string
}

// substSettingsFieldWithDataFromDataSource substitute settings field with new setting built from the data source
func (n *Normalizer) substSettingsFieldWithDataFromDataSource(
	settings SettingsSubstitution,
	dstField,
	srcSecretRefField string,
	parseScalarString bool,
	newSettingCreator func(api.ObjectAddress) (*api.Setting, error),
) bool {
	// Has to have source field specified
	if !settings.Has(srcSecretRefField) {
		return false
	}

	// Fetch data source address from the source setting field
	setting := settings.Get(srcSecretRefField)
	secretAddress, err := setting.FetchDataSourceAddress(n.ctx.chi.Namespace, parseScalarString)
	if err != nil {
		// This is not necessarily an error, just no address specified, most likely setting is not data source ref
		return false
	}

	// Create setting from the secret with a provided function
	if newSetting, err := newSettingCreator(secretAddress); err == nil {
		// Set the new setting as dst.
		// Replacing src in case src name is the same as dst name.
		settings.Set(dstField, newSetting)
	}

	// In case we are NOT replacing the same field with its new value, then remove the source field.
	// Typically non-replaced source field is not expected to be included into the final ClickHouse config,
	// mainly because very often these source fields are synthetic ones (clickhouse does not know them).
	if dstField != srcSecretRefField {
		settings.Delete(srcSecretRefField)
	}

	// All is done
	return true
}

// substSettingsFieldWithSecretFieldValue substitute users settings field with the value read from k8s secret
func (n *Normalizer) substSettingsFieldWithSecretFieldValue(
	settings SettingsSubstitution,
	dstField,
	srcSecretRefField string,
) bool {
	return n.substSettingsFieldWithDataFromDataSource(settings, dstField, srcSecretRefField, true,
		func(secretAddress api.ObjectAddress) (*api.Setting, error) {
			secretFieldValue, err := n.fetchSecretFieldValue(secretAddress)
			if err != nil {
				return nil, err
			}

			return api.NewSettingScalar(secretFieldValue), nil
		})
}

// substSettingsFieldWithEnvRefToSecretField substitute users settings field with ref to ENV var where value from k8s secret is stored in
func (n *Normalizer) substSettingsFieldWithEnvRefToSecretField(
	settings SettingsSubstitution,
	dstField,
	srcSecretRefField,
	envVarNamePrefix string,
	parseScalarString bool,
) bool {
	return n.substSettingsFieldWithDataFromDataSource(settings, dstField, srcSecretRefField, parseScalarString,
		func(secretAddress api.ObjectAddress) (*api.Setting, error) {
			// ENV VAR name and value
			// In case not OK env var name will be empty and config will be incorrect. CH may not start
			envVarName, _ := util.BuildShellEnvVarName(envVarNamePrefix + "_" + settings.Name2Key(dstField))
			n.appendAdditionalEnvVar(
				core.EnvVar{
					Name: envVarName,
					ValueFrom: &core.EnvVarSource{
						SecretKeyRef: &core.SecretKeySelector{
							LocalObjectReference: core.LocalObjectReference{
								Name: secretAddress.Name,
							},
							Key: secretAddress.Key,
						},
					},
				},
			)

			return api.NewSettingScalar("").SetAttribute("from_env", envVarName), nil
		})
}

func (n *Normalizer) substSettingsFieldWithMountedFile(settings *api.Settings, srcSecretRefField string) bool {
	var defaultMode int32 = 0644
	return n.substSettingsFieldWithDataFromDataSource(settings, "", srcSecretRefField, false,
		func(secretAddress api.ObjectAddress) (*api.Setting, error) {
			volumeName, ok1 := util.BuildRFC1035Label(srcSecretRefField)
			volumeMountName, ok2 := util.BuildRFC1035Label(srcSecretRefField)
			filenameInSettingsOrFiles := srcSecretRefField
			filenameInMountedFS := secretAddress.Key

			if !ok1 || !ok2 {
				return nil, fmt.Errorf("unable to build k8s object name")
			}

			n.appendAdditionalVolume(core.Volume{
				Name: volumeName,
				VolumeSource: core.VolumeSource{
					Secret: &core.SecretVolumeSource{
						SecretName: secretAddress.Name,
						Items: []core.KeyToPath{
							{
								Key:  secretAddress.Key,
								Path: filenameInMountedFS,
							},
						},
						DefaultMode: &defaultMode,
					},
				},
			})

			// TODO setting may have specified mountPath explicitly
			mountPath := filepath.Join(model.DirPathSecretFilesConfig, filenameInSettingsOrFiles, secretAddress.Name)
			// TODO setting may have specified subPath explicitly
			// Mount as file
			//subPath := filename
			// Mount as folder
			subPath := ""
			n.appendAdditionalVolumeMount(core.VolumeMount{
				Name:      volumeMountName,
				ReadOnly:  true,
				MountPath: mountPath,
				SubPath:   subPath,
			})

			return nil, fmt.Errorf("no need to create a new setting")
		})
}

func (n *Normalizer) appendClusterSecretEnvVar(cluster *api.Cluster) {
	switch cluster.Secret.Source() {
	case api.ClusterSecretSourcePlaintext:
		// Secret has explicit value, it is not passed via ENV vars
		// Do nothing here
	case api.ClusterSecretSourceSecretRef:
		// Secret has explicit SecretKeyRef
		// Set the password for internode communication using an ENV VAR
		n.appendAdditionalEnvVar(
			core.EnvVar{
				Name: model.InternodeClusterSecretEnvName,
				ValueFrom: &core.EnvVarSource{
					SecretKeyRef: cluster.Secret.GetSecretKeyRef(),
				},
			},
		)
	case api.ClusterSecretSourceAuto:
		// Secret is auto-generated
		// Set the password for internode communication using an ENV VAR
		n.appendAdditionalEnvVar(
			core.EnvVar{
				Name: model.InternodeClusterSecretEnvName,
				ValueFrom: &core.EnvVarSource{
					SecretKeyRef: cluster.Secret.GetAutoSecretKeyRef(model.CreateClusterAutoSecretName(cluster)),
				},
			},
		)
	}
}

func (n *Normalizer) appendAdditionalEnvVar(envVar core.EnvVar) {
	// Sanity check
	if envVar.Name == "" {
		return
	}

	for _, existingEnvVar := range n.ctx.chi.Runtime.Attributes.AdditionalEnvVars {
		if existingEnvVar.Name == envVar.Name {
			// Such a variable already exists
			return
		}
	}

	n.ctx.chi.Runtime.Attributes.AdditionalEnvVars = append(n.ctx.chi.Runtime.Attributes.AdditionalEnvVars, envVar)
}

func (n *Normalizer) appendAdditionalVolume(volume core.Volume) {
	// Sanity check
	if volume.Name == "" {
		return
	}

	for _, existingVolume := range n.ctx.chi.Runtime.Attributes.AdditionalVolumes {
		if existingVolume.Name == volume.Name {
			// Such a variable already exists
			return
		}
	}

	n.ctx.chi.Runtime.Attributes.AdditionalVolumes = append(n.ctx.chi.Runtime.Attributes.AdditionalVolumes, volume)
}

func (n *Normalizer) appendAdditionalVolumeMount(volumeMount core.VolumeMount) {
	// Sanity check
	if volumeMount.Name == "" {
		return
	}

	for _, existingVolumeMount := range n.ctx.chi.Runtime.Attributes.AdditionalVolumeMounts {
		if existingVolumeMount.Name == volumeMount.Name {
			// Such a variable already exists
			return
		}
	}

	n.ctx.chi.Runtime.Attributes.AdditionalVolumeMounts = append(n.ctx.chi.Runtime.Attributes.AdditionalVolumeMounts, volumeMount)
}

var ErrSecretValueNotFound = fmt.Errorf("secret value not found")

// fetchSecretFieldValue fetches the value of the specified field in the specified secret
// TODO this is the only useage of k8s API in the normalizer. How to remove it?
func (n *Normalizer) fetchSecretFieldValue(secretAddress api.ObjectAddress) (string, error) {

	// Fetch the secret
	secret, err := n.kubeClient.CoreV1().Secrets(secretAddress.Namespace).Get(context.TODO(), secretAddress.Name, controller.NewGetOptions())
	if err != nil {
		log.V(1).M(secretAddress.Namespace, secretAddress.Name).F().Info("unable to read secret %s %v", secretAddress, err)
		return "", ErrSecretValueNotFound
	}

	// Find the field within the secret
	for k, value := range secret.Data {
		if secretAddress.Key == k {
			return string(value), nil
		}
	}

	log.V(1).M(secretAddress.Namespace, secretAddress.Name).F().
		Warning("unable to locate secret data by namespace/name/key: %s", secretAddress)

	return "", ErrSecretValueNotFound
}

// normalizeUsersList extracts usernames from provided 'users' settings and adds some extra usernames
func (n *Normalizer) normalizeUsersList(users *api.Settings, extraUsernames ...string) (usernames []string) {
	usernames = append(usernames, users.Groups()...)
	usernames = append(usernames, extraUsernames...)
	usernames = util.NonEmpty(util.Unique(usernames))
	sort.Strings(usernames)

	return usernames
}

const defaultUsername = "default"
const chopProfile = "clickhouse_operator"

// normalizeConfigurationUsers normalizes .spec.configuration.users
func (n *Normalizer) normalizeConfigurationUsers(users *api.Settings) *api.Settings {
	// Ensure and normalize user settings
	users = users.Ensure().Normalize()

	// Add special "default" user to the list of users, which is used/required for:
	// 1. ClickHouse hosts to communicate with each other
	// 2. Specify host_regexp for default user as "allowed hosts to visit from"
	// Add special "chop" user to the list of users, which is used/required for:
	// 1. Operator to communicate with hosts
	usernames := n.normalizeUsersList(
		// user-based settings contains non-explicit users list in it
		users,
		// Add default user which always exists
		defaultUsername,
		// Add CHOp user
		chop.Config().ClickHouse.Access.Username,
	)

	// Normalize each user in the list of users
	for _, username := range usernames {
		n.normalizeConfigurationUser(api.NewSettingsUser(users, username))
	}

	// Remove plain password for the default user
	n.removePlainPassword(api.NewSettingsUser(users, defaultUsername))

	return users
}

func (n *Normalizer) removePlainPassword(user *api.SettingsUser) {
	// If user has any of encrypted password(s) specified, we need to delete existing plaintext password.
	// Set `remove` flag for user's plaintext `password`, which is specified as empty in stock ClickHouse users.xml,
	// thus we need to overwrite it.
	if user.Has("password_double_sha1_hex") || user.Has("password_sha256_hex") {
		user.Set("password", api.NewSettingScalar("").SetAttribute("remove", "1"))
	}
}

const (
	envVarNamePrefixConfigurationUsers    = "CONFIGURATION_USERS"
	envVarNamePrefixConfigurationSettings = "CONFIGURATION_SETTINGS"
)

func (n *Normalizer) normalizeConfigurationUser(user *api.SettingsUser) {
	n.normalizeConfigurationUserSecretRef(user)
	n.normalizeConfigurationUserPassword(user)
	n.normalizeConfigurationUserEnsureMandatoryFields(user)
}

func (n *Normalizer) normalizeConfigurationUserSecretRef(user *api.SettingsUser) {
	user.WalkSafe(func(name string, _ *api.Setting) {
		if strings.HasPrefix(name, "k8s_secret_") {
			// TODO remove as obsoleted
			// Skip this user field, it will be processed later
		} else {
			n.substSettingsFieldWithEnvRefToSecretField(user, name, name, envVarNamePrefixConfigurationUsers, false)
		}
	})
}

func (n *Normalizer) normalizeConfigurationUserEnsureMandatoryFields(user *api.SettingsUser) {
	//
	// Ensure each user has mandatory fields:
	//
	// 1. user/profile
	// 2. user/quota
	// 3. user/networks/ip
	// 4. user/networks/host_regexp
	profile := chop.Config().ClickHouse.Config.User.Default.Profile
	quota := chop.Config().ClickHouse.Config.User.Default.Quota
	ips := append([]string{}, chop.Config().ClickHouse.Config.User.Default.NetworksIP...)
	hostRegexp := model.CreatePodHostnameRegexp(n.ctx.chi, chop.Config().ClickHouse.Config.Network.HostRegexpTemplate)

	// Some users may have special options for mandatory fields
	switch user.Username() {
	case defaultUsername:
		// "default" user
		ips = append(ips, n.ctx.options.DefaultUserAdditionalIPs...)
		if !n.ctx.options.DefaultUserInsertHostRegex {
			hostRegexp = ""
		}
	case chop.Config().ClickHouse.Access.Username:
		// User used by CHOp to access ClickHouse instances.
		ip, _ := chop.Get().ConfigManager.GetRuntimeParam(deployment.OPERATOR_POD_IP)

		profile = chopProfile
		quota = ""
		ips = []string{ip}
		hostRegexp = ""
	}

	// Ensure required values are in place and apply non-empty values in case no own value(s) provided
	n.setMandatoryUserFields(user, &userFields{
		profile:    profile,
		quota:      quota,
		ips:        ips,
		hostRegexp: hostRegexp,
	})
}

type userFields struct {
	profile    string
	quota      string
	ips        []string
	hostRegexp string
}

// setMandatoryUserFields sets user fields
func (n *Normalizer) setMandatoryUserFields(user *api.SettingsUser, fields *userFields) {
	// Ensure required values are in place and apply non-empty values in case no own value(s) provided
	if fields.profile != "" {
		user.SetIfNotExists("profile", api.NewSettingScalar(fields.profile))
	}
	if fields.quota != "" {
		user.SetIfNotExists("quota", api.NewSettingScalar(fields.quota))
	}
	if len(fields.ips) > 0 {
		user.Set("networks/ip", api.NewSettingVector(fields.ips).MergeFrom(user.Get("networks/ip")))
	}
	if fields.hostRegexp != "" {
		user.SetIfNotExists("networks/host_regexp", api.NewSettingScalar(fields.hostRegexp))
	}
}

// normalizeConfigurationUserPassword deals with user passwords
func (n *Normalizer) normalizeConfigurationUserPassword(user *api.SettingsUser) {
	// Values from the secret have higher priority
	n.substSettingsFieldWithSecretFieldValue(user, "password", "k8s_secret_password")
	n.substSettingsFieldWithSecretFieldValue(user, "password_sha256_hex", "k8s_secret_password_sha256_hex")
	n.substSettingsFieldWithSecretFieldValue(user, "password_double_sha1_hex", "k8s_secret_password_double_sha1_hex")

	// Values from the secret passed via ENV have even higher priority
	n.substSettingsFieldWithEnvRefToSecretField(user, "password", "k8s_secret_env_password", envVarNamePrefixConfigurationUsers, true)
	n.substSettingsFieldWithEnvRefToSecretField(user, "password_sha256_hex", "k8s_secret_env_password_sha256_hex", envVarNamePrefixConfigurationUsers, true)
	n.substSettingsFieldWithEnvRefToSecretField(user, "password_double_sha1_hex", "k8s_secret_env_password_double_sha1_hex", envVarNamePrefixConfigurationUsers, true)

	// Out of all passwords, password_double_sha1_hex has top priority, thus keep it only
	if user.Has("password_double_sha1_hex") {
		user.Delete("password_sha256_hex")
		user.Delete("password")
		// This is all for this user
		return
	}

	// Than goes password_sha256_hex, thus keep it only
	if user.Has("password_sha256_hex") {
		user.Delete("password_double_sha1_hex")
		user.Delete("password")
		// This is all for this user
		return
	}

	// From now on we either have a plaintext password specified (explicitly or via ENV), or no password at all

	if user.Get("password").HasAttributes() {
		// Have plaintext password with attributes - means we have plaintext password explicitly specified via ENV var
		// This is fine
		// This is all for this user
		return
	}

	// From now on we either have plaintext password specified as an explicit string, or no password at all

	passwordPlaintext := user.Get("password").String()

	// Apply default password for password-less non-default users
	// 1. NB "default" user keeps empty password in here.
	// 2. ClickHouse user gets password from his section of CHOp configuration
	// 3. All the rest users get default password
	if passwordPlaintext == "" {
		switch user.Username() {
		case defaultUsername:
			// NB "default" user keeps empty password in here.
		case chop.Config().ClickHouse.Access.Username:
			// User used by CHOp to access ClickHouse instances.
			// Gets ClickHouse access password from "ClickHouse.Access.Password"
			passwordPlaintext = chop.Config().ClickHouse.Access.Password
		default:
			// All the rest users get default password from "ClickHouse.Config.User.Default.Password"
			passwordPlaintext = chop.Config().ClickHouse.Config.User.Default.Password
		}
	}

	// It may come that plaintext password is still empty.
	// For example, user `default` quite often has empty password.
	if passwordPlaintext == "" {
		// This is fine
		// This is all for this user
		return
	}

	// Have plaintext password specified.
	// Replace plaintext password with encrypted one
	passwordSHA256 := sha256.Sum256([]byte(passwordPlaintext))
	user.Set("password_sha256_hex", api.NewSettingScalar(hex.EncodeToString(passwordSHA256[:])))
	// And keep only one password specification - delete all the rest (if any exists)
	user.Delete("password_double_sha1_hex")
	user.Delete("password")
}

// normalizeConfigurationProfiles normalizes .spec.configuration.profiles
func (n *Normalizer) normalizeConfigurationProfiles(profiles *api.Settings) *api.Settings {
	if profiles == nil {
		//profiles = api.NewSettings()
		return nil
	}
	profiles.Normalize()
	return profiles
}

// normalizeConfigurationQuotas normalizes .spec.configuration.quotas
func (n *Normalizer) normalizeConfigurationQuotas(quotas *api.Settings) *api.Settings {
	if quotas == nil {
		//quotas = api.NewSettings()
		return nil
	}
	quotas.Normalize()
	return quotas
}

// normalizeConfigurationSettings normalizes .spec.configuration.settings
func (n *Normalizer) normalizeConfigurationSettings(settings *api.Settings) *api.Settings {
	if settings == nil {
		//settings = api.NewSettings()
		return nil
	}
	settings.Normalize()

	settings.WalkSafe(func(name string, setting *api.Setting) {
		n.substSettingsFieldWithEnvRefToSecretField(settings, name, name, envVarNamePrefixConfigurationSettings, false)
	})
	return settings
}

// normalizeConfigurationFiles normalizes .spec.configuration.files
func (n *Normalizer) normalizeConfigurationFiles(files *api.Settings) *api.Settings {
	if files == nil {
		//files = api.NewSettings()
		return nil
	}
	files.Normalize()

	files.WalkSafe(func(key string, setting *api.Setting) {
		n.substSettingsFieldWithMountedFile(files, key)
	})

	return files
}

// normalizeCluster normalizes cluster and returns deployments usage counters for this cluster
func (n *Normalizer) normalizeCluster(cluster *api.Cluster) *api.Cluster {
	if cluster == nil {
		cluster = creator.NewDefaultCluster()
	}

	cluster.Runtime.CHI = n.ctx.chi

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
		cluster.Layout = api.NewChiClusterLayout()
	}
	cluster.FillShardReplicaSpecified()
	cluster.Layout = n.normalizeClusterLayoutShardsCountAndReplicasCount(cluster.Layout)
	n.ensureClusterLayoutShards(cluster.Layout)
	n.ensureClusterLayoutReplicas(cluster.Layout)

	n.createHostsField(cluster)
	n.appendClusterSecretEnvVar(cluster)

	// Loop over all shards and replicas inside shards and fill structure
	cluster.WalkShards(func(index int, shard *api.ChiShard) error {
		n.normalizeShard(shard, cluster, index)
		return nil
	})

	cluster.WalkReplicas(func(index int, replica *api.ChiReplica) error {
		n.normalizeReplica(replica, cluster, index)
		return nil
	})

	cluster.Layout.HostsField.WalkHosts(func(shard, replica int, host *api.ChiHost) error {
		n.normalizeHost(host, cluster.GetShard(shard), cluster.GetReplica(replica), cluster, shard, replica)
		return nil
	})

	return cluster
}

// createHostsField
func (n *Normalizer) createHostsField(cluster *api.Cluster) {
	cluster.Layout.HostsField = api.NewHostsField(cluster.Layout.ShardsCount, cluster.Layout.ReplicasCount)

	// Need to migrate hosts from Shards and Replicas into HostsField
	hostMergeFunc := func(shard, replica int, host *api.ChiHost) error {
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
func (n *Normalizer) normalizeClusterSchemaPolicy(policy *api.SchemaPolicy) *api.SchemaPolicy {
	if policy == nil {
		policy = api.NewClusterSchemaPolicy()
	}

	switch strings.ToLower(policy.Replica) {
	case strings.ToLower(model.SchemaPolicyReplicaNone):
		// Known value, overwrite it to ensure case-ness
		policy.Replica = model.SchemaPolicyReplicaNone
	case strings.ToLower(model.SchemaPolicyReplicaAll):
		// Known value, overwrite it to ensure case-ness
		policy.Replica = model.SchemaPolicyReplicaAll
	default:
		// Unknown value, fallback to default
		policy.Replica = model.SchemaPolicyReplicaAll
	}

	switch strings.ToLower(policy.Shard) {
	case strings.ToLower(model.SchemaPolicyShardNone):
		// Known value, overwrite it to ensure case-ness
		policy.Shard = model.SchemaPolicyShardNone
	case strings.ToLower(model.SchemaPolicyShardAll):
		// Known value, overwrite it to ensure case-ness
		policy.Shard = model.SchemaPolicyShardAll
	case strings.ToLower(model.SchemaPolicyShardDistributedTablesOnly):
		// Known value, overwrite it to ensure case-ness
		policy.Shard = model.SchemaPolicyShardDistributedTablesOnly
	default:
		// unknown value, fallback to default
		policy.Shard = model.SchemaPolicyShardAll
	}

	return policy
}

// normalizeClusterLayoutShardsCountAndReplicasCount ensures at least 1 shard and 1 replica counters
func (n *Normalizer) normalizeClusterLayoutShardsCountAndReplicasCount(clusterLayout *api.ChiClusterLayout) *api.ChiClusterLayout {
	if clusterLayout == nil {
		clusterLayout = api.NewChiClusterLayout()
	}

	// ChiClusterLayout.ShardsCount
	// and
	// ChiClusterLayout.ReplicasCount
	// must represent max number of shards and replicas requested respectively

	// Deal with unspecified ShardsCount
	if clusterLayout.ShardsCount == 0 {
		// We need to have at least one Shard
		clusterLayout.ShardsCount = 1
	}

	// Adjust layout.ShardsCount to max known count

	if len(clusterLayout.Shards) > clusterLayout.ShardsCount {
		// We have more explicitly specified shards than count specified.
		// Need to adjust.
		clusterLayout.ShardsCount = len(clusterLayout.Shards)
	}

	// Let's look for explicitly specified Shards in Layout.Replicas
	for i := range clusterLayout.Replicas {
		replica := &clusterLayout.Replicas[i]

		if replica.ShardsCount > clusterLayout.ShardsCount {
			// We have Shards number specified explicitly in this replica
			clusterLayout.ShardsCount = replica.ShardsCount
		}

		if len(replica.Hosts) > clusterLayout.ShardsCount {
			// We have more explicitly specified shards than count specified.
			// Need to adjust.
			clusterLayout.ShardsCount = len(replica.Hosts)
		}
	}

	// Deal with unspecified ReplicasCount
	if clusterLayout.ReplicasCount == 0 {
		// We need to have at least one Replica
		clusterLayout.ReplicasCount = 1
	}

	// Adjust layout.ReplicasCount to max known count

	if len(clusterLayout.Replicas) > clusterLayout.ReplicasCount {
		// We have more explicitly specified replicas than count specified.
		// Need to adjust.
		clusterLayout.ReplicasCount = len(clusterLayout.Replicas)
	}

	// Let's look for explicitly specified Replicas in Layout.Shards
	for i := range clusterLayout.Shards {
		shard := &clusterLayout.Shards[i]

		if shard.ReplicasCount > clusterLayout.ReplicasCount {
			// We have Replicas number specified explicitly in this shard
			clusterLayout.ReplicasCount = shard.ReplicasCount
		}

		if len(shard.Hosts) > clusterLayout.ReplicasCount {
			// We have more explicitly specified replcas than count specified.
			// Need to adjust.
			clusterLayout.ReplicasCount = len(shard.Hosts)
		}
	}

	return clusterLayout
}

// ensureClusterLayoutShards ensures slice layout.Shards is in place
func (n *Normalizer) ensureClusterLayoutShards(layout *api.ChiClusterLayout) {
	// Disposition of shards in slice would be
	// [explicitly specified shards 0..N, N+1..layout.ShardsCount-1 empty slots for to-be-filled shards]

	// Some (may be all) shards specified, need to append space for unspecified shards
	// TODO may be there is better way to append N slots to a slice
	for len(layout.Shards) < layout.ShardsCount {
		layout.Shards = append(layout.Shards, api.ChiShard{})
	}
}

// ensureClusterLayoutReplicas ensures slice layout.Replicas is in place
func (n *Normalizer) ensureClusterLayoutReplicas(layout *api.ChiClusterLayout) {
	// Disposition of replicas in slice would be
	// [explicitly specified replicas 0..N, N+1..layout.ReplicasCount-1 empty slots for to-be-filled replicas]

	// Some (may be all) replicas specified, need to append space for unspecified replicas
	// TODO may be there is better way to append N slots to a slice
	for len(layout.Replicas) < layout.ReplicasCount {
		layout.Replicas = append(layout.Replicas, api.ChiReplica{})
	}
}

// normalizeShard normalizes a shard - walks over all fields
func (n *Normalizer) normalizeShard(shard *api.ChiShard, cluster *api.Cluster, shardIndex int) {
	n.normalizeShardName(shard, shardIndex)
	n.normalizeShardWeight(shard)
	// For each shard of this normalized cluster inherit from cluster
	shard.InheritSettingsFrom(cluster)
	shard.Settings = n.normalizeConfigurationSettings(shard.Settings)
	shard.InheritFilesFrom(cluster)
	shard.Files = n.normalizeConfigurationFiles(shard.Files)
	shard.InheritTemplatesFrom(cluster)
	// Normalize Replicas
	n.normalizeShardReplicasCount(shard, cluster.Layout.ReplicasCount)
	n.normalizeShardHosts(shard, cluster, shardIndex)
	// Internal replication uses ReplicasCount thus it has to be normalized after shard ReplicaCount normalized
	n.normalizeShardInternalReplication(shard)
}

// normalizeReplica normalizes a replica - walks over all fields
func (n *Normalizer) normalizeReplica(replica *api.ChiReplica, cluster *api.Cluster, replicaIndex int) {
	n.normalizeReplicaName(replica, replicaIndex)
	// For each replica of this normalized cluster inherit from cluster
	replica.InheritSettingsFrom(cluster)
	replica.Settings = n.normalizeConfigurationSettings(replica.Settings)
	replica.InheritFilesFrom(cluster)
	replica.Files = n.normalizeConfigurationFiles(replica.Files)
	replica.InheritTemplatesFrom(cluster)
	// Normalize Shards
	n.normalizeReplicaShardsCount(replica, cluster.Layout.ShardsCount)
	n.normalizeReplicaHosts(replica, cluster, replicaIndex)
}

// normalizeShardReplicasCount ensures shard.ReplicasCount filled properly
func (n *Normalizer) normalizeShardReplicasCount(shard *api.ChiShard, layoutReplicasCount int) {
	if shard.ReplicasCount > 0 {
		// Shard has explicitly specified number of replicas
		return
	}

	// Here we have shard.ReplicasCount = 0,
	// meaning that shard does not have explicitly specified number of replicas.
	// We need to fill it.

	// Look for explicitly specified Replicas first
	if len(shard.Hosts) > 0 {
		// We have Replicas specified as a slice and no other replicas count provided,
		// this means we have explicitly specified replicas only and exact ReplicasCount is known
		shard.ReplicasCount = len(shard.Hosts)
		return
	}

	// No shard.ReplicasCount specified, no replicas explicitly provided,
	// so we have to use ReplicasCount from layout
	shard.ReplicasCount = layoutReplicasCount
}

// normalizeReplicaShardsCount ensures replica.ShardsCount filled properly
func (n *Normalizer) normalizeReplicaShardsCount(replica *api.ChiReplica, layoutShardsCount int) {
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
func (n *Normalizer) normalizeShardName(shard *api.ChiShard, index int) {
	if (len(shard.Name) > 0) && !model.IsAutoGeneratedShardName(shard.Name, shard, index) {
		// Has explicitly specified name already
		return
	}

	shard.Name = model.CreateShardName(shard, index)
}

// normalizeReplicaName normalizes replica name
func (n *Normalizer) normalizeReplicaName(replica *api.ChiReplica, index int) {
	if (len(replica.Name) > 0) && !model.IsAutoGeneratedReplicaName(replica.Name, replica, index) {
		// Has explicitly specified name already
		return
	}

	replica.Name = model.CreateReplicaName(replica, index)
}

// normalizeShardName normalizes shard weight
func (n *Normalizer) normalizeShardWeight(shard *api.ChiShard) {
}

// normalizeShardHosts normalizes all replicas of specified shard
func (n *Normalizer) normalizeShardHosts(shard *api.ChiShard, cluster *api.Cluster, shardIndex int) {
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
func (n *Normalizer) normalizeReplicaHosts(replica *api.ChiReplica, cluster *api.Cluster, replicaIndex int) {
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
	host *api.ChiHost,
	shard *api.ChiShard,
	replica *api.ChiReplica,
	cluster *api.Cluster,
	shardIndex int,
	replicaIndex int,
) {

	n.normalizeHostName(host, shard, shardIndex, replica, replicaIndex)
	entitiesNormalizer.NormalizeHostPorts(host)
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
	host *api.ChiHost,
	shard *api.ChiShard,
	shardIndex int,
	replica *api.ChiReplica,
	replicaIndex int,
) {
	if (len(host.GetName()) > 0) && !model.IsAutoGeneratedHostName(host.GetName(), host, shard, shardIndex, replica, replicaIndex) {
		// Has explicitly specified name already
		return
	}

	host.Name = model.CreateHostName(host, shard, shardIndex, replica, replicaIndex)
}

// normalizeShardInternalReplication ensures reasonable values in
// .spec.configuration.clusters.layout.shards.internalReplication
func (n *Normalizer) normalizeShardInternalReplication(shard *api.ChiShard) {
	// Shards with replicas are expected to have internal replication on by default
	defaultInternalReplication := false
	if shard.ReplicasCount > 1 {
		defaultInternalReplication = true
	}
	shard.InternalReplication = shard.InternalReplication.Normalize(defaultInternalReplication)
}
