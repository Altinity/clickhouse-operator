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
	"sort"
	"strings"

	"github.com/google/uuid"

	core "k8s.io/api/core/v1"

	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/macro"
	crTemplatesNormalizer "github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer/templates_cr"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/schemer"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	commonCreator "github.com/altinity/clickhouse-operator/pkg/model/common/creator"
	commonNamer "github.com/altinity/clickhouse-operator/pkg/model/common/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer/subst"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer/templates"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

const (
	// defaultReconcileShardsThreadsNumber specifies the default number of threads usable for concurrent shard reconciliation
	// within a single cluster reconciliation. Defaults to 1, which means strictly sequential shard reconciliation.
	defaultReconcileShardsThreadsNumber = 1

	// defaultReconcileShardsMaxConcurrencyPercent specifies the maximum integer percentage of shards that may be reconciled
	// concurrently during cluster reconciliation. This counterbalances the fact that this is an operator setting,
	// that different clusters will have different shard counts, and that the shard concurrency capacity is specified
	// above in terms of a number of threads to use (up to). Example: overriding to 100 means all shards may be
	// reconciled concurrently, if the number of shard reconciliation threads is greater than or equal to the number
	// of shards in the cluster.
	defaultReconcileShardsMaxConcurrencyPercent = 50
)

// Normalizer specifies structures normalizer
type Normalizer struct {
	secretGet subst.SecretGetter
	req       *Request
	namer     interfaces.INameManager
	macro     interfaces.IMacro
	labeler   interfaces.ILabeler
}

// New creates new normalizer
func New(secretGet subst.SecretGetter) *Normalizer {
	return &Normalizer{
		secretGet: secretGet,
		namer:     managers.NewNameManager(managers.NameManagerTypeClickHouse),
		macro:     macro.New(),
		labeler:   labeler.New(nil),
	}
}

// CreateTemplated produces ready-to-use object
func (n *Normalizer) CreateTemplated(subj *chi.ClickHouseInstallation, options *normalizer.Options[chi.ClickHouseInstallation]) (
	*chi.ClickHouseInstallation,
	error,
) {
	// Normalization starts with a new request
	n.buildRequest(options)
	// Ensure normalization subject presence
	subj = n.ensureSubject(subj)
	// Build target from all templates and subject
	n.buildTargetFromTemplates(subj)
	// And launch normalization of the whole stack
	return n.normalizeTarget()
}

func (n *Normalizer) buildRequest(options *normalizer.Options[chi.ClickHouseInstallation]) {
	n.req = NewRequest(options)
}

func (n *Normalizer) buildTargetFromTemplates(subj *chi.ClickHouseInstallation) {
	// Create new target that will be populated with data during normalization process
	n.req.SetTarget(n.newSubject())

	// At this moment we have target available - it is either newly created or a system-wide template

	// Apply internal CR templates on top of target
	n.applyInternalCRTemplatesOnTarget()

	// Apply external CR templates - both auto and explicitly requested - on top of target
	n.applyExternalCRTemplatesOnTarget(subj)

	// After all CR templates applied, place provided 'subject' on top of the whole target stack
	n.applyCROnTarget(subj)
}

func (n *Normalizer) applyInternalCRTemplatesOnTarget() {
	for _, template := range n.req.Options().Templates {
		n.req.GetTarget().MergeFrom(template, chi.MergeTypeOverrideByNonEmptyValues)
	}
}

func (n *Normalizer) applyExternalCRTemplatesOnTarget(templateRefSrc crTemplatesNormalizer.TemplateRefListSource) {
	usedTemplates := crTemplatesNormalizer.ApplyTemplates(n.req.GetTarget(), templateRefSrc)
	n.req.GetTarget().EnsureStatus().PushUsedTemplate(usedTemplates...)
}

func (n *Normalizer) applyCROnTarget(cr *chi.ClickHouseInstallation) {
	n.req.GetTarget().MergeFrom(cr, chi.MergeTypeOverrideByNonEmptyValues)
}

func (n *Normalizer) newSubject() *chi.ClickHouseInstallation {
	return managers.CreateCustomResource(managers.CustomResourceCHI).(*chi.ClickHouseInstallation)
}

func (n *Normalizer) shouldCreateDefaultCluster(subj *chi.ClickHouseInstallation) bool {
	if subj == nil {
		// No subject specified - meaning we are normalizing non-existing subject and it should have no clusters inside
		return false
	} else {
		// Subject specified - meaning we are normalizing existing subject and we need to ensure default cluster presence
		return true
	}
}

func (n *Normalizer) ensureSubject(subj *chi.ClickHouseInstallation) *chi.ClickHouseInstallation {
	n.req.Options().WithDefaultCluster = n.shouldCreateDefaultCluster(subj)

	if subj == nil {
		// Need to create subject
		return n.newSubject()
	} else {
		// Subject specified
		return subj
	}
}

// normalizeTarget normalizes target
func (n *Normalizer) normalizeTarget() (*chi.ClickHouseInstallation, error) {
	n.normalizeSpec()
	n.finalize()
	n.fillStatus()

	return n.req.GetTarget(), nil
}

func (n *Normalizer) normalizeSpec() {
	// Walk over Spec datatype fields
	n.req.GetTarget().GetSpecT().TaskID = n.normalizeTaskID(n.req.GetTarget().GetSpecT().TaskID)
	n.req.GetTarget().GetSpecT().UseTemplates = n.normalizeUseTemplates(n.req.GetTarget().GetSpecT().UseTemplates)
	n.req.GetTarget().GetSpecT().Stop = n.normalizeStop(n.req.GetTarget().GetSpecT().Stop)
	n.req.GetTarget().GetSpecT().Restart = n.normalizeRestart(n.req.GetTarget().GetSpecT().Restart)
	n.req.GetTarget().GetSpecT().Troubleshoot = n.normalizeTroubleshoot(n.req.GetTarget().GetSpecT().Troubleshoot)
	n.req.GetTarget().GetSpecT().NamespaceDomainPattern = n.normalizeNamespaceDomainPattern(n.req.GetTarget().GetSpecT().NamespaceDomainPattern)
	n.req.GetTarget().GetSpecT().Templating = n.normalizeTemplating(n.req.GetTarget().GetSpecT().Templating)
	n.req.GetTarget().GetSpecT().Reconciling = n.normalizeReconciling(n.req.GetTarget().GetSpecT().Reconciling)
	n.req.GetTarget().GetSpecT().Defaults = n.normalizeDefaults(n.req.GetTarget().GetSpecT().Defaults)
	n.req.GetTarget().GetSpecT().Configuration = n.normalizeConfiguration(n.req.GetTarget().GetSpecT().Configuration)
	n.req.GetTarget().GetSpecT().Templates = n.normalizeTemplates(n.req.GetTarget().GetSpecT().Templates)
	// UseTemplates already done
}

// finalize performs some finalization tasks, which should be done after CHI is normalized
func (n *Normalizer) finalize() {
	n.req.GetTarget().Fill()
	n.req.GetTarget().WalkHosts(func(host *chi.Host) error {
		n.hostApplyHostTemplateSpecifiedOrDefault(host)
		return nil
	})
	n.fillCRAddressInfo()
}

// fillCRAddressInfo
func (n *Normalizer) fillCRAddressInfo() {
	n.req.GetTarget().WalkHosts(func(host *chi.Host) error {
		host.Runtime.Address.StatefulSet = n.namer.Name(interfaces.NameStatefulSet, host)
		host.Runtime.Address.FQDN = n.namer.Name(interfaces.NameFQDN, host)
		return nil
	})
}

// fillStatus fills .status section of a CHI with values based on current CHI
func (n *Normalizer) fillStatus() {
	pods := make([]string, 0)
	fqdns := make([]string, 0)
	n.req.GetTarget().WalkHosts(func(host *chi.Host) error {
		pods = append(pods, n.namer.Name(interfaces.NamePod, host))
		fqdns = append(fqdns, n.namer.Name(interfaces.NameFQDN, host))
		return nil
	})
	ip, _ := chop.GetRuntimeParam(deployment.OPERATOR_POD_IP)
	n.req.GetTarget().FillStatus(n.endpoints(), pods, fqdns, ip)
}

func (n *Normalizer) endpoints() []string {
	var names []string
	if templates, ok := n.req.GetTarget().GetRootServiceTemplates(); ok {
		for _, template := range templates {
			names = append(names,
				n.namer.Name(interfaces.NameCRServiceFQDN, n.req.GetTarget(), n.req.GetTarget().GetSpec().GetNamespaceDomainPattern(), template),
			)
		}
	} else {
		names = append(names,
			n.namer.Name(interfaces.NameCRServiceFQDN, n.req.GetTarget(), n.req.GetTarget().GetSpec().GetNamespaceDomainPattern()),
		)
	}
	return names
}

// normalizeTaskID normalizes .spec.taskID
func (n *Normalizer) normalizeTaskID(taskID *types.String) *types.String {
	if len(taskID.Value()) > 0 {
		return taskID
	}

	return types.NewString(uuid.New().String())
}

// normalizeStop normalizes .spec.stop
func (n *Normalizer) normalizeStop(stop *types.StringBool) *types.StringBool {
	if stop.IsValid() {
		// It is bool, use as it is
		return stop
	}

	// In case it is unknown value - just use set it to false
	return types.NewStringBool(false)
}

// normalizeRestart normalizes .spec.restart
func (n *Normalizer) normalizeRestart(restart *types.String) *types.String {
	switch strings.ToLower(restart.Value()) {
	case strings.ToLower(chi.RestartRollingUpdate):
		// Known value, overwrite it to ensure case-ness
		return types.NewString(chi.RestartRollingUpdate)
	}

	// In case it is unknown value - just use empty
	return nil
}

// normalizeTroubleshoot normalizes .spec.stop
func (n *Normalizer) normalizeTroubleshoot(troubleshoot *types.StringBool) *types.StringBool {
	if troubleshoot.IsValid() {
		// It is bool, use as it is
		return troubleshoot
	}

	// In case it is unknown value - just use set it to false
	return types.NewStringBool(false)
}

func isNamespaceDomainPatternValid(namespaceDomainPattern *types.String) bool {
	if strings.Count(namespaceDomainPattern.Value(), "%s") > 1 {
		return false
	} else {
		return true
	}
}

// normalizeNamespaceDomainPattern normalizes .spec.namespaceDomainPattern
func (n *Normalizer) normalizeNamespaceDomainPattern(namespaceDomainPattern *types.String) *types.String {
	if isNamespaceDomainPatternValid(namespaceDomainPattern) {
		return namespaceDomainPattern
	}
	// In case namespaceDomainPattern is not valid - do not use it
	return nil
}

// normalizeDefaults normalizes .spec.defaults
func (n *Normalizer) normalizeDefaults(defaults *chi.Defaults) *chi.Defaults {
	if defaults == nil {
		defaults = chi.NewDefaults()
	}
	// Set defaults for CHI object properties
	defaults.ReplicasUseFQDN = defaults.ReplicasUseFQDN.Normalize(false)
	// Ensure field
	if defaults.DistributedDDL == nil {
		//defaults.DistributedDDL = api.NewDistributedDDL()
	}
	// Ensure field
	if defaults.StorageManagement == nil {
		defaults.StorageManagement = chi.NewStorageManagement()
	}
	// Ensure field
	if defaults.Templates == nil {
		//defaults.Templates = api.NewChiTemplateNames()
	}
	defaults.Templates.HandleDeprecatedFields()
	return defaults
}

// normalizeConfiguration normalizes .spec.configuration
func (n *Normalizer) normalizeConfiguration(conf *chi.Configuration) *chi.Configuration {
	if conf == nil {
		conf = chi.NewConfiguration()
	}
	conf.Zookeeper = n.normalizeConfigurationZookeeper(conf.Zookeeper)
	n.normalizeConfigurationAllSettingsBasedSections(conf)
	conf.Clusters = n.normalizeClusters(conf.Clusters)
	return conf
}

// normalizeConfigurationAllSettingsBasedSections normalizes Settings-based configuration
func (n *Normalizer) normalizeConfigurationAllSettingsBasedSections(conf *chi.Configuration) {
	conf.Users = n.normalizeConfigurationUsers(conf.Users, n.req.GetTarget())
	conf.Profiles = n.normalizeConfigurationProfiles(conf.Profiles, n.req.GetTarget())
	conf.Quotas = n.normalizeConfigurationQuotas(conf.Quotas, n.req.GetTarget())
	conf.Settings = n.normalizeConfigurationSettings(conf.Settings, n.req.GetTarget())
	conf.Files = n.normalizeConfigurationFiles(conf.Files, n.req.GetTarget())
}

// normalizeTemplates normalizes .spec.templates
func (n *Normalizer) normalizeTemplates(templates *chi.Templates) *chi.Templates {
	if templates == nil {
		return nil
	}

	n.normalizeHostTemplates(templates)
	n.normalizePodTemplates(templates)
	n.normalizeVolumeClaimTemplates(templates)
	n.normalizeServiceTemplates(templates)
	return templates
}

// normalizeTemplating normalizes .spec.templating
func (n *Normalizer) normalizeTemplating(templating *chi.ChiTemplating) *chi.ChiTemplating {
	if templating == nil {
		templating = chi.NewChiTemplating()
	}
	switch strings.ToLower(templating.GetPolicy()) {
	case strings.ToLower(chi.TemplatingPolicyAuto):
		// Known value, overwrite it to ensure case-ness
		templating.SetPolicy(chi.TemplatingPolicyAuto)
	case strings.ToLower(chi.TemplatingPolicyManual):
		// Known value, overwrite it to ensure case-ness
		templating.SetPolicy(chi.TemplatingPolicyManual)
	default:
		// Unknown value, fallback to default
		templating.SetPolicy(chi.TemplatingPolicyManual)
	}
	return templating
}

// normalizeReconciling normalizes .spec.reconciling
func (n *Normalizer) normalizeReconciling(reconciling *chi.Reconciling) *chi.Reconciling {
	if reconciling == nil {
		reconciling = chi.NewReconciling().SetDefaults()
	}

	// Policy
	switch strings.ToLower(reconciling.GetPolicy()) {
	case strings.ToLower(chi.ReconcilingPolicyWait):
		// Known value, overwrite it to ensure case-ness
		reconciling.SetPolicy(chi.ReconcilingPolicyWait)
	case strings.ToLower(chi.ReconcilingPolicyNoWait):
		// Known value, overwrite it to ensure case-ness
		reconciling.SetPolicy(chi.ReconcilingPolicyNoWait)
	default:
		// Unknown value, fallback to default
		reconciling.SetPolicy(chi.ReconcilingPolicyUnspecified)
	}

	// ConfigMapPropagationTimeout
	// No normalization yet

	// Cleanup
	reconciling.SetCleanup(n.normalizeReconcilingCleanup(reconciling.GetCleanup()))

	// Runtime
	// No normalization yet
	// Macros
	// No normalization yet

	return reconciling
}

func (n *Normalizer) normalizeReconcilingCleanup(cleanup *chi.Cleanup) *chi.Cleanup {
	if cleanup == nil {
		cleanup = chi.NewCleanup()
	}

	if cleanup.UnknownObjects == nil {
		cleanup.UnknownObjects = cleanup.DefaultUnknownObjects()
	}
	n.normalizeCleanup(&cleanup.UnknownObjects.StatefulSet, chi.ObjectsCleanupDelete)
	n.normalizeCleanup(&cleanup.UnknownObjects.PVC, chi.ObjectsCleanupDelete)
	n.normalizeCleanup(&cleanup.UnknownObjects.ConfigMap, chi.ObjectsCleanupDelete)
	n.normalizeCleanup(&cleanup.UnknownObjects.Service, chi.ObjectsCleanupDelete)

	if cleanup.ReconcileFailedObjects == nil {
		cleanup.ReconcileFailedObjects = cleanup.DefaultReconcileFailedObjects()
	}
	n.normalizeCleanup(&cleanup.ReconcileFailedObjects.StatefulSet, chi.ObjectsCleanupRetain)
	n.normalizeCleanup(&cleanup.ReconcileFailedObjects.PVC, chi.ObjectsCleanupRetain)
	n.normalizeCleanup(&cleanup.ReconcileFailedObjects.ConfigMap, chi.ObjectsCleanupRetain)
	n.normalizeCleanup(&cleanup.ReconcileFailedObjects.Service, chi.ObjectsCleanupRetain)
	return cleanup
}

func (n *Normalizer) normalizeCleanup(str *string, value string) {
	if str == nil {
		return
	}
	switch strings.ToLower(*str) {
	case strings.ToLower(chi.ObjectsCleanupRetain):
		// Known value, overwrite it to ensure case-ness
		*str = chi.ObjectsCleanupRetain
	case strings.ToLower(chi.ObjectsCleanupDelete):
		// Known value, overwrite it to ensure case-ness
		*str = chi.ObjectsCleanupDelete
	default:
		// Unknown value, fallback to default
		*str = value
	}
}

func (n *Normalizer) normalizeHostTemplates(templates *chi.Templates) {
	for i := range templates.HostTemplates {
		n.normalizeHostTemplate(&templates.HostTemplates[i])
	}
}

func (n *Normalizer) normalizePodTemplates(templates *chi.Templates) {
	for i := range templates.PodTemplates {
		n.normalizePodTemplate(&templates.PodTemplates[i])
	}
}

func (n *Normalizer) normalizeVolumeClaimTemplates(templates *chi.Templates) {
	for i := range templates.VolumeClaimTemplates {
		n.normalizeVolumeClaimTemplate(&templates.VolumeClaimTemplates[i])
	}
}

func (n *Normalizer) normalizeServiceTemplates(templates *chi.Templates) {
	for i := range templates.ServiceTemplates {
		n.normalizeServiceTemplate(&templates.ServiceTemplates[i])
	}
}

// normalizeHostTemplate normalizes .spec.templates.hostTemplates
func (n *Normalizer) normalizeHostTemplate(template *chi.HostTemplate) {
	templates.NormalizeHostTemplate(template)
	// Introduce HostTemplate into Index
	n.req.GetTarget().GetSpecT().GetTemplates().EnsureHostTemplatesIndex().Set(template.Name, template)
}

// normalizePodTemplate normalizes .spec.templates.podTemplates
func (n *Normalizer) normalizePodTemplate(template *chi.PodTemplate) {
	// TODO need to support multi-cluster
	replicasCount := 1
	if len(n.req.GetTarget().GetSpecT().Configuration.Clusters) > 0 {
		replicasCount = n.req.GetTarget().GetSpecT().Configuration.Clusters[0].Layout.ReplicasCount
	}
	templates.NormalizePodTemplate(n.macro, n.labeler, replicasCount, template)
	// Introduce PodTemplate into Index
	n.req.GetTarget().GetSpecT().GetTemplates().EnsurePodTemplatesIndex().Set(template.Name, template)
}

// normalizeVolumeClaimTemplate normalizes .spec.templates.volumeClaimTemplates
func (n *Normalizer) normalizeVolumeClaimTemplate(template *chi.VolumeClaimTemplate) {
	templates.NormalizeVolumeClaimTemplate(template)
	// Introduce VolumeClaimTemplate into Index
	n.req.GetTarget().GetSpecT().GetTemplates().EnsureVolumeClaimTemplatesIndex().Set(template.Name, template)
}

// normalizeServiceTemplate normalizes .spec.templates.serviceTemplates
func (n *Normalizer) normalizeServiceTemplate(template *chi.ServiceTemplate) {
	templates.NormalizeServiceTemplate(template)
	// Introduce ServiceClaimTemplate into Index
	n.req.GetTarget().GetSpecT().GetTemplates().EnsureServiceTemplatesIndex().Set(template.Name, template)
}

// normalizeUseTemplates is a wrapper to hold the name of normalized section
func (n *Normalizer) normalizeUseTemplates(templates []*chi.TemplateRef) []*chi.TemplateRef {
	return crTemplatesNormalizer.NormalizeTemplateRefList(templates)
}

// normalizeClusters normalizes clusters
func (n *Normalizer) normalizeClusters(clusters []*chi.Cluster) []*chi.Cluster {
	// We need to have at least one cluster available
	clusters = n.ensureClusters(clusters)
	// Normalize all clusters
	for i := range clusters {
		clusters[i] = n.normalizeCluster(clusters[i])
	}
	return clusters
}

// ensureClusters
func (n *Normalizer) ensureClusters(clusters []*chi.Cluster) []*chi.Cluster {
	// May be we have cluster(s) available
	if len(clusters) > 0 {
		return clusters
	}

	// In case no clusters available, we may want to create a default one
	if n.req.Options().WithDefaultCluster {
		return []*chi.Cluster{
			commonCreator.CreateCluster(interfaces.ClusterCHIDefault).(*chi.Cluster),
		}
	}

	// Nope, no clusters expected
	return nil
}

// normalizeConfigurationZookeeper normalizes .spec.configuration.zookeeper
func (n *Normalizer) normalizeConfigurationZookeeper(zk *chi.ZookeeperConfig) *chi.ZookeeperConfig {
	if zk == nil {
		return nil
	}

	// In case no ZK port specified - assign default
	for i := range zk.Nodes {
		// Convenience wrapper
		node := &zk.Nodes[i]
		if !node.Port.IsValid() {
			node.Port = types.NewInt32(config.ZkDefaultPort)
		}
	}

	// In case no ZK root specified - assign '/clickhouse/{namespace}/{target name}'
	//if zk.Root == "" {
	//	zk.Root = fmt.Sprintf(zkDefaultRootTemplate, n.target.Namespace, n.target.Name)
	//}

	return zk
}

func (n *Normalizer) appendClusterSecretEnvVar(cluster chi.ICluster) {
	switch cluster.GetSecret().Source() {
	case chi.ClusterSecretSourcePlaintext:
		// Secret has explicit value, it is not passed via ENV vars
		// Do nothing here
		return
	case chi.ClusterSecretSourceSecretRef:
		// Secret has explicit SecretKeyRef
		// Set the password for inter-node communication using an ENV VAR
		n.req.AppendAdditionalEnvVar(
			core.EnvVar{
				Name: config.InternodeClusterSecretEnvName,
				ValueFrom: &core.EnvVarSource{
					SecretKeyRef: cluster.GetSecret().GetSecretKeyRef(),
				},
			},
		)
	case chi.ClusterSecretSourceAuto:
		// Secret is auto-generated
		// Set the password for inter-node communication using an ENV VAR
		n.req.AppendAdditionalEnvVar(
			core.EnvVar{
				Name: config.InternodeClusterSecretEnvName,
				ValueFrom: &core.EnvVarSource{
					SecretKeyRef: cluster.GetSecret().GetAutoSecretKeyRef(n.namer.Name(interfaces.NameClusterAutoSecret, cluster)),
				},
			},
		)
	}
}

// normalizeUsersList extracts usernames from provided 'users' settings and adds some extra usernames
func (n *Normalizer) normalizeUsersList(users *chi.Settings, extraUsernames ...string) (usernames []string) {
	usernames = append(usernames, users.Groups()...)
	usernames = append(usernames, extraUsernames...)
	usernames = util.NonEmpty(util.Unique(usernames))
	sort.Strings(usernames)

	return usernames
}

const defaultUsername = "default"
const chopProfile = "clickhouse_operator"

const clickhouseOperatorUserMacro = "{clickhouseOperatorUser}"

func clickhouseOperatorUserMacroReplacer() *util.Replacer {
	return util.NewReplacer(
		map[string]string{
			clickhouseOperatorUserMacro: chop.Config().ClickHouse.Access.Username,
		},
	)
}

type replacerSection string

var (
	replacerFiles    replacerSection = "files"
	replacerProfiles replacerSection = "profiles"
	replacerQuotas   replacerSection = "quotas"
	replacerSettings replacerSection = "settings"
	replacerUsers    replacerSection = "users"
)

func (n *Normalizer) replacers(section replacerSection, scope any, additional ...*util.Replacer) (replacers []*util.Replacer) {
	scoped := false
	sections := n.req.GetTarget().Spec.Reconciling.Macros.Sections

	switch section {
	case replacerFiles:
		if sections.Files.Enabled.IsTrue() {
			log.V(2).M(scope).F().Info("macros enabled for files")
			scoped = true
		}
		break
	case replacerProfiles:
		if sections.Profiles.Enabled.IsTrue() {
			log.V(2).M(scope).F().Info("macros enabled for profiles")
			scoped = true
		}
		break
	case replacerQuotas:
		if sections.Quotas.Enabled.IsTrue() {
			log.V(2).M(scope).F().Info("macros enabled for quotas")
			scoped = true
		}
		break
	case replacerSettings:
		if sections.Settings.Enabled.IsTrue() {
			log.V(2).M(scope).F().Info("macros enabled for settings")
			scoped = true
		}
		break
	case replacerUsers:
		if sections.Users.Enabled.IsTrue() {
			log.V(2).M(scope).F().Info("macros enabled for users")
			scoped = true
		}
		break
	}
	if scoped {
		replacers = append(replacers, n.macro.Scope(scope).Replacer())
		log.V(2).M(scope).F().Info("macros enabled for section: %s", section)
	} else {
		log.V(2).M(scope).F().Info("no macros enabled for section: %s", section)
	}
	replacers = append(replacers, additional...)
	return replacers
}

func (n *Normalizer) settingsNormalizerOptions(section replacerSection, scope any, additional ...*util.Replacer) *chi.SettingsNormalizerOptions {
	return &chi.SettingsNormalizerOptions{
		Replacers: n.replacers(section, scope, additional...),
	}
}

// normalizeConfigurationUsers normalizes .spec.configuration.users
func (n *Normalizer) normalizeConfigurationUsers(users *chi.Settings, scope any) *chi.Settings {
	// Ensure and normalize target user settings
	users = users.Ensure().Normalize(n.settingsNormalizerOptions(replacerUsers, scope, clickhouseOperatorUserMacroReplacer()))

	// Add special "default" user to the list of users, which is used/required for:
	//   1. ClickHouse hosts to communicate with each other
	//   2. Specify host_regexp for default user as "allowed hosts to visit from"
	// Add special "chop" user to the list of users, which is used/required for:
	//   1. Operator to communicate with hosts
	usernames := n.normalizeUsersList(
		// User-based settings section contains non-explicit users list in it - as part of paths
		users,
		// Add default user which always exists
		defaultUsername,
		// Add CHOp user
		chop.Config().ClickHouse.Access.Username,
	)

	// Normalize each user in the list of users
	for _, username := range usernames {
		n.normalizeConfigurationUser(chi.NewSettingsUser(users, username))
	}

	// Remove plain password for the "default" user
	n.removePlainPassword(chi.NewSettingsUser(users, defaultUsername))

	return users
}

func (n *Normalizer) removePlainPassword(user *chi.SettingsUser) {
	// If user has any of encrypted password(s) specified, we need to delete existing plaintext password.
	// Set 'remove' flag for user's plaintext 'password' setting, which is specified as empty in stock ClickHouse users.xml,
	// thus we need to overwrite it.
	if user.Has("password_double_sha1_hex") || user.Has("password_sha256_hex") {
		user.Set("password", chi.NewSettingScalar("").SetAttribute("remove", "1"))
	}
}

// normalizeConfigurationProfiles normalizes .spec.configuration.profiles
func (n *Normalizer) normalizeConfigurationProfiles(profiles *chi.Settings, scope any) *chi.Settings {
	if profiles == nil {
		return nil
	}
	profiles.Normalize(n.settingsNormalizerOptions(replacerProfiles, scope))
	return profiles
}

// normalizeConfigurationQuotas normalizes .spec.configuration.quotas
func (n *Normalizer) normalizeConfigurationQuotas(quotas *chi.Settings, scope any) *chi.Settings {
	if quotas == nil {
		return nil
	}
	quotas.Normalize(n.settingsNormalizerOptions(replacerQuotas, scope))
	return quotas
}

const envVarNamePrefixConfigurationSettings = "CONFIGURATION_SETTINGS"

// normalizeConfigurationSettings normalizes .spec.configuration.settings
func (n *Normalizer) normalizeConfigurationSettings(settings *chi.Settings, scope any) *chi.Settings {
	if settings == nil {
		return nil
	}
	settings.Normalize(n.settingsNormalizerOptions(replacerSettings, scope))

	settings.WalkSafe(func(name string, setting *chi.Setting) {
		subst.ReplaceSettingsFieldWithEnvRefToSecretField(n.req, settings, name, name, envVarNamePrefixConfigurationSettings, false)
	})
	return settings
}

// normalizeConfigurationFiles normalizes .spec.configuration.files
func (n *Normalizer) normalizeConfigurationFiles(files *chi.Settings, scope any) *chi.Settings {
	if files == nil {
		return nil
	}
	files.Normalize(n.settingsNormalizerOptions(replacerFiles, scope))

	files.WalkSafe(func(key string, setting *chi.Setting) {
		subst.ReplaceSettingsFieldWithMountedFile(n.req, files, key)
	})

	return files
}

// normalizeCluster normalizes cluster and returns deployments usage counters for this cluster
func (n *Normalizer) normalizeCluster(cluster *chi.Cluster) *chi.Cluster {
	cluster = cluster.Ensure(func() *chi.Cluster {
		return commonCreator.CreateCluster(interfaces.ClusterCHIDefault).(*chi.Cluster)
	})

	// Runtime has to be prepared first
	cluster.GetRuntime().SetCR(n.req.GetTarget())

	// Then we need to inherit values from the parent
	// Inherit from .spec.configuration.zookeeper
	cluster.InheritZookeeperFrom(n.req.GetTarget())
	// Inherit from .spec.configuration.files
	cluster.InheritFilesFrom(n.req.GetTarget())
	// Inherit from .spec.reconciling
	cluster.InheritReconcileFrom(n.req.GetTarget())
	// Inherit from .spec.defaults
	cluster.InheritTemplatesFrom(n.req.GetTarget())

	cluster.Zookeeper = n.normalizeConfigurationZookeeper(cluster.Zookeeper)
	cluster.Settings = n.normalizeConfigurationSettings(cluster.Settings, cluster)
	cluster.Files = n.normalizeConfigurationFiles(cluster.Files, cluster)

	cluster.SchemaPolicy = n.normalizeClusterSchemaPolicy(cluster.SchemaPolicy)
	cluster.PDBMaxUnavailable = n.normalizePDBMaxUnavailable(cluster.PDBMaxUnavailable)

	// Ensure layout
	cluster.Layout = cluster.Layout.Ensure()
	cluster.FillShardsReplicasExplicitlySpecified()
	cluster.Layout = n.normalizeClusterLayoutShardsCountAndReplicasCount(cluster.Layout)
	cluster.Reconcile = n.normalizeClusterReconcile(cluster.Reconcile)

	n.ensureClusterLayoutShards(cluster.Layout)
	n.ensureClusterLayoutReplicas(cluster.Layout)

	createHostsField(cluster)
	n.appendClusterSecretEnvVar(cluster)

	// Loop over all shards and replicas inside shards and fill structure
	cluster.WalkShards(func(index int, shard chi.IShard) error {
		n.normalizeShard(shard.(*chi.ChiShard), cluster, index)
		return nil
	})

	cluster.WalkReplicas(func(index int, replica *chi.ChiReplica) error {
		n.normalizeReplica(replica, cluster, index)
		return nil
	})

	cluster.Layout.HostsField.WalkHosts(func(shard, replica int, host *chi.Host) error {
		n.normalizeHost(host, cluster.GetShard(shard), cluster.GetReplica(replica), cluster, shard, replica)
		return nil
	})

	return cluster
}

// normalizeClusterLayoutShardsCountAndReplicasCount ensures at least 1 shard and 1 replica counters
func (n *Normalizer) normalizeClusterSchemaPolicy(policy *chi.SchemaPolicy) *chi.SchemaPolicy {
	if policy == nil {
		policy = chi.NewClusterSchemaPolicy()
	}

	switch strings.ToLower(policy.Replica) {
	case strings.ToLower(schemer.SchemaPolicyReplicaNone):
		// Known value, overwrite it to ensure case-ness
		policy.Replica = schemer.SchemaPolicyReplicaNone
	case strings.ToLower(schemer.SchemaPolicyReplicaAll):
		// Known value, overwrite it to ensure case-ness
		policy.Replica = schemer.SchemaPolicyReplicaAll
	default:
		// Unknown value, fallback to default
		policy.Replica = schemer.SchemaPolicyReplicaAll
	}

	switch strings.ToLower(policy.Shard) {
	case strings.ToLower(schemer.SchemaPolicyShardNone):
		// Known value, overwrite it to ensure case-ness
		policy.Shard = schemer.SchemaPolicyShardNone
	case strings.ToLower(schemer.SchemaPolicyShardAll):
		// Known value, overwrite it to ensure case-ness
		policy.Shard = schemer.SchemaPolicyShardAll
	case strings.ToLower(schemer.SchemaPolicyShardDistributedTablesOnly):
		// Known value, overwrite it to ensure case-ness
		policy.Shard = schemer.SchemaPolicyShardDistributedTablesOnly
	default:
		// unknown value, fallback to default
		policy.Shard = schemer.SchemaPolicyShardAll
	}

	return policy
}

// normalizePDBMaxUnavailable normalizes PDBMaxUnavailable
func (n *Normalizer) normalizePDBMaxUnavailable(value *types.Int32) *types.Int32 {
	return value.Normalize(1)
}

// normalizeClusterLayoutShardsCountAndReplicasCount ensures at least 1 shard and 1 replica counters
func (n *Normalizer) normalizeClusterLayoutShardsCountAndReplicasCount(clusterLayout *chi.ChiClusterLayout) *chi.ChiClusterLayout {
	// clusterLayout.ShardsCount
	// and
	// clusterLayout.ReplicasCount
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
		replica := clusterLayout.Replicas[i]

		if replica.ShardsCount > clusterLayout.ShardsCount {
			// We have Shards number specified explicitly in this replica,
			// and this replica has more shards than specified in cluster.
			// Well, enlarge cluster shards count
			clusterLayout.ShardsCount = replica.ShardsCount
		}

		if len(replica.Hosts) > clusterLayout.ShardsCount {
			// We have more explicitly specified shards than count specified.
			// Well, enlarge cluster shards count
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
		// Well, enlarge cluster replicas count
		clusterLayout.ReplicasCount = len(clusterLayout.Replicas)
	}

	// Let's look for explicitly specified Replicas in Layout.Shards
	for i := range clusterLayout.Shards {
		shard := clusterLayout.Shards[i]

		if shard.ReplicasCount > clusterLayout.ReplicasCount {
			// We have Replicas number specified explicitly in this shard
			// Well, enlarge cluster replicas count
			clusterLayout.ReplicasCount = shard.ReplicasCount
		}

		if len(shard.Hosts) > clusterLayout.ReplicasCount {
			// We have more explicitly specified replicas than count specified.
			// Well, enlarge cluster replicas count
			clusterLayout.ReplicasCount = len(shard.Hosts)
		}
	}

	return clusterLayout
}

func (n *Normalizer) normalizeClusterReconcile(reconcile chi.ClusterReconcile) chi.ClusterReconcile {
	reconcile.Runtime = n.normalizeReconcileRuntime(reconcile.Runtime)
	return reconcile
}

func (n *Normalizer) normalizeReconcileRuntime(runtime chi.ReconcileRuntime) chi.ReconcileRuntime {
	if runtime.ReconcileShardsThreadsNumber == 0 {
		runtime.ReconcileShardsThreadsNumber = chop.Config().Reconcile.Runtime.ReconcileShardsThreadsNumber
	}
	if runtime.ReconcileShardsThreadsNumber == 0 {
		runtime.ReconcileShardsThreadsNumber = defaultReconcileShardsThreadsNumber
	}
	if runtime.ReconcileShardsMaxConcurrencyPercent == 0 {
		runtime.ReconcileShardsMaxConcurrencyPercent = chop.Config().Reconcile.Runtime.ReconcileShardsMaxConcurrencyPercent
	}
	if runtime.ReconcileShardsMaxConcurrencyPercent == 0 {
		runtime.ReconcileShardsMaxConcurrencyPercent = defaultReconcileShardsMaxConcurrencyPercent
	}
	return runtime
}

// ensureClusterLayoutShards ensures slice layout.Shards is in place
func (n *Normalizer) ensureClusterLayoutShards(layout *chi.ChiClusterLayout) {
	// Disposition of shards in slice would be
	// [explicitly specified shards 0..N, N+1..layout.ShardsCount-1 empty slots for to-be-filled shards]

	// Some (may be all) shards specified, need to append assumed (unspecified, but expected to exist) shards
	// TODO may be there is better way to append N slots to a slice
	for len(layout.Shards) < layout.ShardsCount {
		layout.Shards = append(layout.Shards, &chi.ChiShard{})
	}
}

// ensureClusterLayoutReplicas ensures slice layout.Replicas is in place
func (n *Normalizer) ensureClusterLayoutReplicas(layout *chi.ChiClusterLayout) {
	// Disposition of replicas in slice would be
	// [explicitly specified replicas 0..N, N+1..layout.ReplicasCount-1 empty slots for to-be-filled replicas]

	// Some (may be all) replicas specified, need to append assumed (unspecified, but expected to exist) replicas
	// TODO may be there is better way to append N slots to a slice
	for len(layout.Replicas) < layout.ReplicasCount {
		layout.Replicas = append(layout.Replicas, &chi.ChiReplica{})
	}
}

// normalizeShard normalizes a shard - walks over all fields
func (n *Normalizer) normalizeShard(shard *chi.ChiShard, cluster *chi.Cluster, shardIndex int) {
	n.normalizeShardName(shard, shardIndex)
	n.normalizeShardWeight(shard)
	// For each shard of this normalized cluster inherit from cluster
	shard.InheritSettingsFrom(cluster)
	shard.Settings = n.normalizeConfigurationSettings(shard.Settings, shard)
	shard.InheritFilesFrom(cluster)
	shard.Files = n.normalizeConfigurationFiles(shard.Files, shard)
	shard.InheritTemplatesFrom(cluster)
	// Normalize Replicas
	n.normalizeShardReplicasCount(shard, cluster.Layout.ReplicasCount)
	n.normalizeShardHosts(shard, cluster, shardIndex)
	// Internal replication uses ReplicasCount thus it has to be normalized after shard ReplicaCount normalized
	n.normalizeShardInternalReplication(shard)
}

// normalizeReplica normalizes a replica - walks over all fields
func (n *Normalizer) normalizeReplica(replica *chi.ChiReplica, cluster *chi.Cluster, replicaIndex int) {
	n.normalizeReplicaName(replica, replicaIndex)
	// For each replica of this normalized cluster inherit from cluster
	replica.InheritSettingsFrom(cluster)
	replica.Settings = n.normalizeConfigurationSettings(replica.Settings, replica)
	replica.InheritFilesFrom(cluster)
	replica.Files = n.normalizeConfigurationFiles(replica.Files, replica)
	replica.InheritTemplatesFrom(cluster)
	// Normalize Shards
	n.normalizeReplicaShardsCount(replica, cluster.Layout.ShardsCount)
	n.normalizeReplicaHosts(replica, cluster, replicaIndex)
}

// normalizeShardReplicasCount ensures shard.ReplicasCount filled properly
func (n *Normalizer) normalizeShardReplicasCount(shard *chi.ChiShard, layoutReplicasCount int) {
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
func (n *Normalizer) normalizeReplicaShardsCount(replica *chi.ChiReplica, layoutShardsCount int) {
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
func (n *Normalizer) normalizeShardName(shard *chi.ChiShard, index int) {
	if (len(shard.GetName()) > 0) && !commonNamer.IsAutoGeneratedShardName(shard.GetName(), shard, index) {
		// Has explicitly specified name already
		return
	}

	shard.Name = n.namer.Name(interfaces.NameShard, shard, index)
}

// normalizeReplicaName normalizes replica name
func (n *Normalizer) normalizeReplicaName(replica *chi.ChiReplica, index int) {
	if (len(replica.Name) > 0) && !commonNamer.IsAutoGeneratedReplicaName(replica.Name, replica, index) {
		// Has explicitly specified name already
		return
	}

	replica.Name = n.namer.Name(interfaces.NameReplica, replica, index)
}

// normalizeShardName normalizes shard weight
func (n *Normalizer) normalizeShardWeight(shard *chi.ChiShard) {
}

// normalizeShardHosts normalizes all replicas of specified shard
func (n *Normalizer) normalizeShardHosts(shard *chi.ChiShard, cluster *chi.Cluster, shardIndex int) {
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
func (n *Normalizer) normalizeReplicaHosts(replica *chi.ChiReplica, cluster *chi.Cluster, replicaIndex int) {
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

// normalizeShardInternalReplication ensures reasonable values in
// .spec.configuration.clusters.layout.shards.internalReplication
func (n *Normalizer) normalizeShardInternalReplication(shard *chi.ChiShard) {
	// Shards with replicas are expected to have internal replication on by default
	defaultInternalReplication := false
	if shard.ReplicasCount > 1 {
		defaultInternalReplication = true
	}
	shard.InternalReplication = shard.InternalReplication.Normalize(defaultInternalReplication)
}
