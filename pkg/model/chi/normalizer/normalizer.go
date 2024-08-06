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
	"sort"
	"strings"

	"github.com/google/uuid"

	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	crTemplatesNormalizer "github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer/templates_cr"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/schemer"
	commonCreator "github.com/altinity/clickhouse-operator/pkg/model/common/creator"
	commonNamer "github.com/altinity/clickhouse-operator/pkg/model/common/namer"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer/subst_settings"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer/templates"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Normalizer specifies structures normalizer
type Normalizer struct {
	secretGet subst_settings.SecretGetter
	ctx       *Context
	namer     interfaces.INameManager
}

// New creates new normalizer
func New(secretGet subst_settings.SecretGetter) *Normalizer {
	return &Normalizer{
		secretGet: secretGet,
		namer:     managers.NewNameManager(managers.NameManagerTypeClickHouse),
	}
}

// CreateTemplated produces ready-to-use object
func (n *Normalizer) CreateTemplated(subj *api.ClickHouseInstallation, options *normalizer.Options) (
	*api.ClickHouseInstallation,
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

func (n *Normalizer) buildTargetFromTemplates(subj *api.ClickHouseInstallation) {
	// Create new target that will be populated with data during normalization process
	n.ctx.SetTarget(n.createTarget())

	// At this moment we have target available - is either newly created or a system-wide template

	// Apply templates - both auto and explicitly requested - on top of target
	n.applyTemplatesOnTarget(subj)

	// After all templates applied, place provided 'subject' on top of the whole stack (target)
	n.ctx.GetTarget().MergeFrom(subj, api.MergeTypeOverrideByNonEmptyValues)
}

func (n *Normalizer) applyTemplatesOnTarget(subj crTemplatesNormalizer.TemplateSubject) {
	for _, template := range crTemplatesNormalizer.ApplyTemplates(n.ctx.GetTarget(), subj) {
		n.ctx.GetTarget().EnsureStatus().PushUsedTemplate(template)
	}
}

func (n *Normalizer) newSubject() *api.ClickHouseInstallation {
	return managers.CreateCustomResource(managers.CustomResourceCHI).(*api.ClickHouseInstallation)
}

func (n *Normalizer) shouldCreateDefaultCluster(subj *api.ClickHouseInstallation) bool {
	if subj == nil {
		// No subject specified - meaning we are normalizing non-existing subject and it should have no clusters inside
		return false
	} else {
		// Subject specified - meaning we are normalizing existing subject and we need to ensure default cluster presence
		return true
	}
}

func (n *Normalizer) ensureSubject(subj *api.ClickHouseInstallation) *api.ClickHouseInstallation {
	n.ctx.Options().WithDefaultCluster = n.shouldCreateDefaultCluster(subj)

	if subj == nil {
		// Need to create subject
		return n.newSubject()
	} else {
		// Subject specified
		return subj
	}
}

func (n *Normalizer) GetTargetTemplate() *api.ClickHouseInstallation {
	return chop.Config().Template.CHI.Runtime.Template
}

func (n *Normalizer) HasTargetTemplate() bool {
	return n.GetTargetTemplate() != nil
}

func (n *Normalizer) createTarget() *api.ClickHouseInstallation {
	if n.HasTargetTemplate() {
		// Template specified - start with template
		return n.GetTargetTemplate().DeepCopy()
	} else {
		// No template specified - start with clear page
		return n.newSubject()
	}
}

// normalizeTarget normalizes target
func (n *Normalizer) normalizeTarget() (*api.ClickHouseInstallation, error) {
	n.normalizeSpec()
	n.finalize()
	n.fillStatus()

	return n.ctx.GetTarget(), nil
}

func (n *Normalizer) normalizeSpec() {
	// Walk over Spec datatype fields
	n.ctx.GetTarget().GetSpecT().TaskID = n.normalizeTaskID(n.ctx.GetTarget().GetSpecT().TaskID)
	n.ctx.GetTarget().GetSpecT().UseTemplates = n.normalizeUseTemplates(n.ctx.GetTarget().GetSpecT().UseTemplates)
	n.ctx.GetTarget().GetSpecT().Stop = n.normalizeStop(n.ctx.GetTarget().GetSpecT().Stop)
	n.ctx.GetTarget().GetSpecT().Restart = n.normalizeRestart(n.ctx.GetTarget().GetSpecT().Restart)
	n.ctx.GetTarget().GetSpecT().Troubleshoot = n.normalizeTroubleshoot(n.ctx.GetTarget().GetSpecT().Troubleshoot)
	n.ctx.GetTarget().GetSpecT().NamespaceDomainPattern = n.normalizeNamespaceDomainPattern(n.ctx.GetTarget().GetSpecT().NamespaceDomainPattern)
	n.ctx.GetTarget().GetSpecT().Templating = n.normalizeTemplating(n.ctx.GetTarget().GetSpecT().Templating)
	n.ctx.GetTarget().GetSpecT().Reconciling = n.normalizeReconciling(n.ctx.GetTarget().GetSpecT().Reconciling)
	n.ctx.GetTarget().GetSpecT().Defaults = n.normalizeDefaults(n.ctx.GetTarget().GetSpecT().Defaults)
	n.ctx.GetTarget().GetSpecT().Configuration = n.normalizeConfiguration(n.ctx.GetTarget().GetSpecT().Configuration)
	n.ctx.GetTarget().GetSpecT().Templates = n.normalizeTemplates(n.ctx.GetTarget().GetSpecT().Templates)
	// UseTemplates already done
}

// finalize performs some finalization tasks, which should be done after CHI is normalized
func (n *Normalizer) finalize() {
	n.ctx.GetTarget().Fill()
	n.ctx.GetTarget().WalkHosts(func(host *api.Host) error {
		n.hostApplyHostTemplateSpecifiedOrDefault(host)
		return nil
	})
	n.fillCHIAddressInfo()
}

// fillCHIAddressInfo
func (n *Normalizer) fillCHIAddressInfo() {
	n.ctx.GetTarget().WalkHosts(func(host *api.Host) error {
		host.Runtime.Address.StatefulSet = n.namer.Name(interfaces.NameStatefulSet, host)
		host.Runtime.Address.FQDN = n.namer.Name(interfaces.NameFQDN, host)
		return nil
	})
}

// fillStatus fills .status section of a CHI with values based on current CHI
func (n *Normalizer) fillStatus() {
	endpoint := n.namer.Name(interfaces.NameCRServiceFQDN, n.ctx.GetTarget(), n.ctx.GetTarget().GetSpec().GetNamespaceDomainPattern())
	pods := make([]string, 0)
	fqdns := make([]string, 0)
	n.ctx.GetTarget().WalkHosts(func(host *api.Host) error {
		pods = append(pods, n.namer.Name(interfaces.NamePod, host))
		fqdns = append(fqdns, n.namer.Name(interfaces.NameFQDN, host))
		return nil
	})
	ip, _ := chop.Get().ConfigManager.GetRuntimeParam(deployment.OPERATOR_POD_IP)
	n.ctx.GetTarget().FillStatus(endpoint, pods, fqdns, ip)
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
	case strings.ToLower(api.RestartRollingUpdate):
		// Known value, overwrite it to ensure case-ness
		return types.NewString(api.RestartRollingUpdate)
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
func (n *Normalizer) normalizeDefaults(defaults *api.ChiDefaults) *api.ChiDefaults {
	if defaults == nil {
		defaults = api.NewChiDefaults()
	}
	// Set defaults for CHI object properties
	defaults.ReplicasUseFQDN = defaults.ReplicasUseFQDN.Normalize(false)
	// Ensure field
	if defaults.DistributedDDL == nil {
		//defaults.DistributedDDL = api.NewDistributedDDL()
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
	n.normalizeConfigurationAllSettingsBasedSections(conf)
	conf.Clusters = n.normalizeClusters(conf.Clusters)
	return conf
}

// normalizeConfigurationAllSettingsBasedSections normalizes Settings-based configuration
func (n *Normalizer) normalizeConfigurationAllSettingsBasedSections(conf *api.Configuration) {
	conf.Users = n.normalizeConfigurationUsers(conf.Users)
	conf.Profiles = n.normalizeConfigurationProfiles(conf.Profiles)
	conf.Quotas = n.normalizeConfigurationQuotas(conf.Quotas)
	conf.Settings = n.normalizeConfigurationSettings(conf.Settings)
	conf.Files = n.normalizeConfigurationFiles(conf.Files)
}

// normalizeTemplates normalizes .spec.templates
func (n *Normalizer) normalizeTemplates(templates *api.Templates) *api.Templates {
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
func (n *Normalizer) normalizeReconciling(reconciling *api.Reconciling) *api.Reconciling {
	if reconciling == nil {
		reconciling = api.NewReconciling().SetDefaults()
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
	reconciling.SetCleanup(n.normalizeReconcilingCleanup(reconciling.GetCleanup()))
	return reconciling
}

func (n *Normalizer) normalizeReconcilingCleanup(cleanup *api.Cleanup) *api.Cleanup {
	if cleanup == nil {
		cleanup = api.NewCleanup()
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

func (n *Normalizer) normalizeHostTemplates(templates *api.Templates) {
	for i := range templates.HostTemplates {
		n.normalizeHostTemplate(&templates.HostTemplates[i])
	}
}

func (n *Normalizer) normalizePodTemplates(templates *api.Templates) {
	for i := range templates.PodTemplates {
		n.normalizePodTemplate(&templates.PodTemplates[i])
	}
}

func (n *Normalizer) normalizeVolumeClaimTemplates(templates *api.Templates) {
	for i := range templates.VolumeClaimTemplates {
		n.normalizeVolumeClaimTemplate(&templates.VolumeClaimTemplates[i])
	}
}

func (n *Normalizer) normalizeServiceTemplates(templates *api.Templates) {
	for i := range templates.ServiceTemplates {
		n.normalizeServiceTemplate(&templates.ServiceTemplates[i])
	}
}

// normalizeHostTemplate normalizes .spec.templates.hostTemplates
func (n *Normalizer) normalizeHostTemplate(template *api.HostTemplate) {
	templates.NormalizeHostTemplate(template)
	// Introduce HostTemplate into Index
	n.ctx.GetTarget().GetSpecT().GetTemplates().EnsureHostTemplatesIndex().Set(template.Name, template)
}

// normalizePodTemplate normalizes .spec.templates.podTemplates
func (n *Normalizer) normalizePodTemplate(template *api.PodTemplate) {
	// TODO need to support multi-cluster
	replicasCount := 1
	if len(n.ctx.GetTarget().GetSpecT().Configuration.Clusters) > 0 {
		replicasCount = n.ctx.GetTarget().GetSpecT().Configuration.Clusters[0].Layout.ReplicasCount
	}
	templates.NormalizePodTemplate(replicasCount, template)
	// Introduce PodTemplate into Index
	n.ctx.GetTarget().GetSpecT().GetTemplates().EnsurePodTemplatesIndex().Set(template.Name, template)
}

// normalizeVolumeClaimTemplate normalizes .spec.templates.volumeClaimTemplates
func (n *Normalizer) normalizeVolumeClaimTemplate(template *api.VolumeClaimTemplate) {
	templates.NormalizeVolumeClaimTemplate(template)
	// Introduce VolumeClaimTemplate into Index
	n.ctx.GetTarget().GetSpecT().GetTemplates().EnsureVolumeClaimTemplatesIndex().Set(template.Name, template)
}

// normalizeServiceTemplate normalizes .spec.templates.serviceTemplates
func (n *Normalizer) normalizeServiceTemplate(template *api.ServiceTemplate) {
	templates.NormalizeServiceTemplate(template)
	// Introduce ServiceClaimTemplate into Index
	n.ctx.GetTarget().GetSpecT().GetTemplates().EnsureServiceTemplatesIndex().Set(template.Name, template)
}

// normalizeUseTemplates is a wrapper to hold the name of normalized section
func (n *Normalizer) normalizeUseTemplates(templates []*api.TemplateRef) []*api.TemplateRef {
	return crTemplatesNormalizer.NormalizeTemplatesList(templates)
}

// normalizeClusters normalizes clusters
func (n *Normalizer) normalizeClusters(clusters []*api.ChiCluster) []*api.ChiCluster {
	// We need to have at least one cluster available
	clusters = n.ensureClusters(clusters)
	// Normalize all clusters
	for i := range clusters {
		clusters[i] = n.normalizeCluster(clusters[i])
	}
	return clusters
}

// ensureClusters
func (n *Normalizer) ensureClusters(clusters []*api.ChiCluster) []*api.ChiCluster {
	// May be we have cluster(s) available
	if len(clusters) > 0 {
		return clusters
	}

	// In case no clusters available, we may want to create a default one
	if n.ctx.Options().WithDefaultCluster {
		return []*api.ChiCluster{
			commonCreator.CreateCluster(interfaces.ClusterCHIDefault).(*api.ChiCluster),
		}
	}

	// Nope, no clusters expected
	return nil
}

// normalizeConfigurationZookeeper normalizes .spec.configuration.zookeeper
func (n *Normalizer) normalizeConfigurationZookeeper(zk *api.ZookeeperConfig) *api.ZookeeperConfig {
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

func (n *Normalizer) appendClusterSecretEnvVar(cluster api.ICluster) {
	switch cluster.GetSecret().Source() {
	case api.ClusterSecretSourcePlaintext:
		// Secret has explicit value, it is not passed via ENV vars
		// Do nothing here
		return
	case api.ClusterSecretSourceSecretRef:
		// Secret has explicit SecretKeyRef
		// Set the password for internode communication using an ENV VAR
		n.ctx.AppendAdditionalEnvVar(
			core.EnvVar{
				Name: config.InternodeClusterSecretEnvName,
				ValueFrom: &core.EnvVarSource{
					SecretKeyRef: cluster.GetSecret().GetSecretKeyRef(),
				},
			},
		)
	case api.ClusterSecretSourceAuto:
		// Secret is auto-generated
		// Set the password for internode communication using an ENV VAR
		n.ctx.AppendAdditionalEnvVar(
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
	// Ensure and normalizeTarget user settings
	users = users.Ensure().Normalize()

	// Add special "default" user to the list of users, which is used/required for:
	// 1. ClickHouse hosts to communicate with each other
	// 2. Specify host_regexp for default user as "allowed hosts to visit from"
	// Add special "chop" user to the list of users, which is used/required for:
	// 1. Operator to communicate with hosts
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
		n.normalizeConfigurationUser(api.NewSettingsUser(users, username))
	}

	// Remove plain password for the "default" user
	n.removePlainPassword(api.NewSettingsUser(users, defaultUsername))

	return users
}

func (n *Normalizer) removePlainPassword(user *api.SettingsUser) {
	// If user has any of encrypted password(s) specified, we need to delete existing plaintext password.
	// Set 'remove' flag for user's plaintext 'password' setting, which is specified as empty in stock ClickHouse users.xml,
	// thus we need to overwrite it.
	if user.Has("password_double_sha1_hex") || user.Has("password_sha256_hex") {
		user.Set("password", api.NewSettingScalar("").SetAttribute("remove", "1"))
	}
}

// normalizeConfigurationProfiles normalizes .spec.configuration.profiles
func (n *Normalizer) normalizeConfigurationProfiles(profiles *api.Settings) *api.Settings {
	if profiles == nil {
		return nil
	}
	profiles.Normalize()
	return profiles
}

// normalizeConfigurationQuotas normalizes .spec.configuration.quotas
func (n *Normalizer) normalizeConfigurationQuotas(quotas *api.Settings) *api.Settings {
	if quotas == nil {
		return nil
	}
	quotas.Normalize()
	return quotas
}

const envVarNamePrefixConfigurationSettings = "CONFIGURATION_SETTINGS"

// normalizeConfigurationSettings normalizes .spec.configuration.settings
func (n *Normalizer) normalizeConfigurationSettings(settings *api.Settings) *api.Settings {
	if settings == nil {
		return nil
	}
	settings.Normalize()

	settings.WalkSafe(func(name string, setting *api.Setting) {
		subst_settings.SubstSettingsFieldWithEnvRefToSecretField(n.ctx, settings, name, name, envVarNamePrefixConfigurationSettings, false)
	})
	return settings
}

// normalizeConfigurationFiles normalizes .spec.configuration.files
func (n *Normalizer) normalizeConfigurationFiles(files *api.Settings) *api.Settings {
	if files == nil {
		return nil
	}
	files.Normalize()

	files.WalkSafe(func(key string, setting *api.Setting) {
		subst_settings.SubstSettingsFieldWithMountedFile(n.ctx, files, key)
	})

	return files
}

// normalizeCluster normalizes cluster and returns deployments usage counters for this cluster
func (n *Normalizer) normalizeCluster(cluster *api.ChiCluster) *api.ChiCluster {
	// Ensure cluster
	if cluster == nil {
		cluster = commonCreator.CreateCluster(interfaces.ClusterCHIDefault).(*api.ChiCluster)
	}

	// Runtime has to be prepared first
	cluster.GetRuntime().SetCR(n.ctx.GetTarget())

	// Then we need to inherit values from the parent
	// Inherit from .spec.configuration.zookeeper
	cluster.InheritZookeeperFrom(n.ctx.GetTarget())
	// Inherit from .spec.configuration.files
	cluster.InheritFilesFrom(n.ctx.GetTarget())
	// Inherit from .spec.defaults
	cluster.InheritTemplatesFrom(n.ctx.GetTarget())

	cluster.Zookeeper = n.normalizeConfigurationZookeeper(cluster.Zookeeper)
	cluster.Settings = n.normalizeConfigurationSettings(cluster.Settings)
	cluster.Files = n.normalizeConfigurationFiles(cluster.Files)

	cluster.SchemaPolicy = n.normalizeClusterSchemaPolicy(cluster.SchemaPolicy)
	cluster.PDBMaxUnavailable = n.normalizePDBMaxUnavailable(cluster.PDBMaxUnavailable)

	// Ensure layout
	if cluster.Layout == nil {
		cluster.Layout = api.NewChiClusterLayout()
	}
	cluster.FillShardReplicaSpecified()
	cluster.Layout = n.normalizeClusterLayoutShardsCountAndReplicasCount(cluster.Layout)
	n.ensureClusterLayoutShards(cluster.Layout)
	n.ensureClusterLayoutReplicas(cluster.Layout)

	createHostsField(cluster)
	n.appendClusterSecretEnvVar(cluster)

	// Loop over all shards and replicas inside shards and fill structure
	cluster.WalkShards(func(index int, shard api.IShard) error {
		n.normalizeShard(shard.(*api.ChiShard), cluster, index)
		return nil
	})

	cluster.WalkReplicas(func(index int, replica *api.ChiReplica) error {
		n.normalizeReplica(replica, cluster, index)
		return nil
	})

	cluster.Layout.HostsField.WalkHosts(func(shard, replica int, host *api.Host) error {
		n.normalizeHost(host, cluster.GetShard(shard), cluster.GetReplica(replica), cluster, shard, replica)
		return nil
	})

	return cluster
}

// normalizeClusterLayoutShardsCountAndReplicasCount ensures at least 1 shard and 1 replica counters
func (n *Normalizer) normalizeClusterSchemaPolicy(policy *api.SchemaPolicy) *api.SchemaPolicy {
	if policy == nil {
		policy = api.NewClusterSchemaPolicy()
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
func (n *Normalizer) normalizeClusterLayoutShardsCountAndReplicasCount(clusterLayout *api.ChiClusterLayout) *api.ChiClusterLayout {
	// Ensure layout
	if clusterLayout == nil {
		clusterLayout = api.NewChiClusterLayout()
	}

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

// ensureClusterLayoutShards ensures slice layout.Shards is in place
func (n *Normalizer) ensureClusterLayoutShards(layout *api.ChiClusterLayout) {
	// Disposition of shards in slice would be
	// [explicitly specified shards 0..N, N+1..layout.ShardsCount-1 empty slots for to-be-filled shards]

	// Some (may be all) shards specified, need to append assumed (unspecified, but expected to exist) shards
	// TODO may be there is better way to append N slots to a slice
	for len(layout.Shards) < layout.ShardsCount {
		layout.Shards = append(layout.Shards, &api.ChiShard{})
	}
}

// ensureClusterLayoutReplicas ensures slice layout.Replicas is in place
func (n *Normalizer) ensureClusterLayoutReplicas(layout *api.ChiClusterLayout) {
	// Disposition of replicas in slice would be
	// [explicitly specified replicas 0..N, N+1..layout.ReplicasCount-1 empty slots for to-be-filled replicas]

	// Some (may be all) replicas specified, need to append assumed (unspecified, but expected to exist) replicas
	// TODO may be there is better way to append N slots to a slice
	for len(layout.Replicas) < layout.ReplicasCount {
		layout.Replicas = append(layout.Replicas, &api.ChiReplica{})
	}
}

// normalizeShard normalizes a shard - walks over all fields
func (n *Normalizer) normalizeShard(shard *api.ChiShard, cluster *api.ChiCluster, shardIndex int) {
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
func (n *Normalizer) normalizeReplica(replica *api.ChiReplica, cluster *api.ChiCluster, replicaIndex int) {
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
	if (len(shard.Name) > 0) && !commonNamer.IsAutoGeneratedShardName(shard.Name, shard, index) {
		// Has explicitly specified name already
		return
	}

	shard.Name = n.namer.Name(interfaces.NameShard, shard, index)
}

// normalizeReplicaName normalizes replica name
func (n *Normalizer) normalizeReplicaName(replica *api.ChiReplica, index int) {
	if (len(replica.Name) > 0) && !commonNamer.IsAutoGeneratedReplicaName(replica.Name, replica, index) {
		// Has explicitly specified name already
		return
	}

	replica.Name = n.namer.Name(interfaces.NameReplica, replica, index)
}

// normalizeShardName normalizes shard weight
func (n *Normalizer) normalizeShardWeight(shard *api.ChiShard) {
}

// normalizeShardHosts normalizes all replicas of specified shard
func (n *Normalizer) normalizeShardHosts(shard *api.ChiShard, cluster *api.ChiCluster, shardIndex int) {
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
func (n *Normalizer) normalizeReplicaHosts(replica *api.ChiReplica, cluster *api.ChiCluster, replicaIndex int) {
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
func (n *Normalizer) normalizeShardInternalReplication(shard *api.ChiShard) {
	// Shards with replicas are expected to have internal replication on by default
	defaultInternalReplication := false
	if shard.ReplicasCount > 1 {
		defaultInternalReplication = true
	}
	shard.InternalReplication = shard.InternalReplication.Normalize(defaultInternalReplication)
}
