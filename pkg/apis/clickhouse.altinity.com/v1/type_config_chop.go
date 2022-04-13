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

package v1

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	// log "k8s.io/klog"
	log "github.com/golang/glog"
	"github.com/imdario/mergo"
	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/altinity/clickhouse-operator/pkg/util"
)

const (
	// Default values for update timeout and polling period in seconds
	defaultStatefulSetUpdateTimeout      = 300
	defaultStatefulSetUpdatePollInterval = 15

	// Default values for ClickHouse user configuration
	// 1. user/profile
	// 2. user/quota
	// 3. user/networks/ip
	// 4. user/password
	defaultChConfigUserDefaultProfile   = "default"
	defaultChConfigUserDefaultQuota     = "default"
	defaultChConfigUserDefaultNetworkIP = "::/0"
	defaultChConfigUserDefaultPassword  = "default"

	// Username and Password to be used by operator to connect to ClickHouse instances for
	// 1. Metrics requests
	// 2. Schema maintenance
	// User credentials can be specified in additional ClickHouse config files located in `chUsersConfigsPath` folder
	defaultChScheme   = "http"
	defaultChUsername = ""
	defaultChPassword = ""
	defaultChPort     = 8123

	// defaultReconcileThreadsNumber specifies default number of controller threads running concurrently.
	// Used in case no other specified in config
	defaultReconcileThreadsNumber = 1

	// DefaultReconcileThreadsWarmup specifies default reconcile threads warmup time
	DefaultReconcileThreadsWarmup = 10 * time.Second

	// DefaultReconcileSystemThreadsNumber specifies default number of system controller threads running concurrently.
	// Used in case no other specified in config
	DefaultReconcileSystemThreadsNumber = 1

	// defaultTerminationGracePeriod specifies default value for TerminationGracePeriod
	defaultTerminationGracePeriod = 30
	// defaultRevisionHistoryLimit specifies default value for RevisionHistoryLimit
	defaultRevisionHistoryLimit = 10
)

// OperatorConfig specifies operator configuration
// !!! IMPORTANT !!!
// !!! IMPORTANT !!!
// !!! IMPORTANT !!!
// !!! IMPORTANT !!!
// !!! IMPORTANT !!!
// Do not forget to update func (config *OperatorConfig) String()
// Do not forget to update CRD spec

// OperatorConfigWatch specifies watch section
type OperatorConfigWatch struct {
	// Namespaces where operator watches for events
	Namespaces []string `json:"namespaces" yaml:"namespaces"`
}

// OperatorConfigConfig specifies Config section
type OperatorConfigConfig struct {
	File OperatorConfigFile `json:"file" yaml:"file"`

	User OperatorConfigUser `json:"user" yaml:"user"`

	Network struct {
		HostRegexpTemplate string `json:"hostRegexpTemplate" yaml:"hostRegexpTemplate"`
	} `json:"network" yaml:"network"`
}

// OperatorConfigFile specifies File section
type OperatorConfigFile struct {
	Path struct {
		// Paths where to look for additional ClickHouse config .xml files to be mounted into Pod
		Common string `json:"common" yaml:"common"`
		Host   string `json:"host"   yaml:"host"`
		User   string `json:"user"   yaml:"user"`
	} `json:"path" yaml:"path"`

	Runtime OperatorConfigFileRuntime
}

// OperatorConfigFileRuntime specifies runtime section
type OperatorConfigFileRuntime struct {
	// OperatorConfig files fetched from paths specified above. Maps "file name->file content"
	CommonConfigFiles map[string]string
	HostConfigFiles   map[string]string
	UsersConfigFiles  map[string]string
}

// OperatorConfigUser specifies User section
type OperatorConfigUser struct {
	Default OperatorConfigDefault `json:"default" yaml:"default"`
}

// OperatorConfigDefault specifies user-default section
type OperatorConfigDefault struct {
	// Default values for ClickHouse user configuration
	// 1. user/profile - string
	// 2. user/quota - string
	// 3. user/networks/ip - multiple strings
	// 4. user/password - string
	Profile    string   `json:"profile"   yaml:"profile"`
	Quota      string   `json:"quota"     yaml:"quota"`
	NetworksIP []string `json:"networksIP" yaml:"networksIP"`
	Password   string   `json:"password"  yaml:"password"`
}

// OperatorConfigClickHouse specifies ClickHouse section
type OperatorConfigClickHouse struct {
	Config OperatorConfigConfig `json:"configuration" yaml:"configuration"`

	Access struct {
		// Username and Password to be used by operator to connect to ClickHouse instances
		// for
		// 1. Metrics requests
		// 2. Schema maintenance
		// User credentials can be specified in additional ClickHouse config files located in `chUsersConfigsPath` folder
		Scheme   string `json:"scheme" yaml:"scheme"`
		Username string `json:"username" yaml:"username"`
		Password string `json:"password" yaml:"password"`

		// Location of k8s Secret with username and password to be used by the operator to connect to ClickHouse instances
		// Can be used instead of explicitly specified (above) username and password
		Secret struct {
			Namespace string `json:"namespace" yaml:"namespace"`
			Name      string `json:"name"      yaml:"name"`

			Runtime struct {
				// Username and Password to be used by operator to connect to ClickHouse instances
				// extracted from k8s secret specified above.
				Username string
				Password string
			}
		} `json:"secret" yaml:"secret"`

		// Port where to connect to ClickHouse instances to
		Port int `json:"port" yaml:"port"`
	} `json:"access" yaml:"access"`
}

// OperatorConfigTemplate specifies template section
type OperatorConfigTemplate struct {
	CHI OperatorConfigCHI `json:"chi" yaml:"chi"`
}

// OperatorConfigCHI specifies template CHI section
type OperatorConfigCHI struct {
	// Path where to look for ClickHouseInstallation templates .yaml files
	Path string `json:"path" yaml:"path"`

	Runtime OperatorConfigCHIRuntime
}

// OperatorConfigCHIRuntime specifies chi runtime section
type OperatorConfigCHIRuntime struct {
	// CHI template files fetched from the path specified above. Maps "file name->file content"
	TemplateFiles map[string]string
	// CHI template objects unmarshalled from CHITemplateFiles. Maps "metadata.name->object"
	Templates []*ClickHouseInstallation `json:"-" yaml:"-"`
	// ClickHouseInstallation template
	Template *ClickHouseInstallation `json:"-" yaml:"-"`
}

// OperatorConfigReconcile specifies reconcile section
type OperatorConfigReconcile struct {
	Runtime struct {
		ThreadsNumber int `json:"threadsNumber" yaml:"threadsNumber"`
	} `json:"runtime" yaml:"runtime"`

	StatefulSet struct {
		Create struct {
			OnFailure string `json:"onFailure" yaml:"onFailure"`
		} `json:"create" yaml:"create"`

		Update struct {
			Timeout      uint64 `json:"timeout" yaml:"timeout"`
			PollInterval uint64 `json:"pollInterval" yaml:"pollInterval"`
			OnFailure    string `json:"onFailure" yaml:"onFailure"`
		} `json:"update" yaml:"update"`
	} `json:"statefulSet" yaml:"statefulSet"`

	Host struct {
		Wait struct {
			Exclude bool `json:"exclude" yaml:"exclude"`
			Include bool `json:"include" yaml:"include"`
		} `json:"host" yaml:"host"`
	} `json:"host" yaml:"host"`
}

// OperatorConfigAnnotation specifies annotation section
type OperatorConfigAnnotation struct {
	// When transferring annotations from the chi/chit.metadata to CHI objects, use these filters.
	Include []string `json:"include" yaml:"include"`
	Exclude []string `json:"exclude" yaml:"exclude"`
}

// OperatorConfigLabel specifies label section
type OperatorConfigLabel struct {
	// When transferring labels from the chi/chit.metadata to child objects, use these filters.
	Include []string `json:"include" yaml:"include"`
	Exclude []string `json:"exclude" yaml:"exclude"`

	// Whether to append *Scope* labels to StatefulSet and Pod.
	AppendScopeString string `json:"appendScope" yaml:"appendScope"`

	Runtime struct {
		AppendScope bool
	}
}

// OperatorConfig specifies operator config
type OperatorConfig struct {
	Runtime struct {
		// Full path to the config file and folder where this OperatorConfig originates from
		ConfigFilePath   string
		ConfigFolderPath string
		// Namespace specifies namespace where operator runs
		Namespace string
	}
	Watch       OperatorConfigWatch      `json:"watch" yaml:"watch"`
	ClickHouse  OperatorConfigClickHouse `json:"clickhouse" yaml:"clickhouse"`
	Template    OperatorConfigTemplate   `json:"template" yaml:"template"`
	Reconcile   OperatorConfigReconcile  `json:"reconcile" yaml:"reconcile"`
	Annotation  OperatorConfigAnnotation `json:"annotation" yaml:"annotation"`
	Label       OperatorConfigLabel      `json:"label" yaml:"label"`
	StatefulSet struct {
		// Revision history limit
		RevisionHistoryLimit int `json:"revisionHistoryLimit" yaml:"revisionHistoryLimit"`
	} `json:"statefulSet" yaml:"statefulSet"`
	Pod struct {
		// Grace period for Pod termination.
		TerminationGracePeriod int `json:"terminationGracePeriod" yaml:"terminationGracePeriod"`
	} `json:"pod" yaml:"pod"`
	Logger struct {
		// Logger section
		LogToStderr     string `json:"logtostderr"      yaml:"logtostderr"`
		AlsoLogToStderr string `json:"alsologtostderr"  yaml:"alsologtostderr"`
		V               string `json:"v"                yaml:"v"`
		StderrThreshold string `json:"stderrthreshold"  yaml:"stderrthreshold"`
		VModule         string `json:"vmodule"          yaml:"vmodule"`
		LogBacktraceAt  string `json:"log_backtrace_at" yaml:"log_backtrace_at"`
	} `json:"logger" yaml:"logger"`

	//
	// The end of OperatorConfig
	//
	// !!! IMPORTANT !!!
	// !!! IMPORTANT !!!
	// !!! IMPORTANT !!!
	// !!! IMPORTANT !!!
	// !!! IMPORTANT !!!
	// Do not forget to update func (config *OperatorConfig) String()
	// Do not forget to update CRD spec

	////////////////////
	// DEPRECATED!
	// TO BE REMOVED!
	////////////////////

	// WatchNamespaces where operator watches for events
	WatchNamespaces []string `json:"watchNamespaces" yaml:"watchNamespaces"`
	// Paths where to look for additional ClickHouse config .xml files to be mounted into Pod
	CHCommonConfigsPath string `json:"chCommonConfigsPath" yaml:"chCommonConfigsPath"`
	CHHostConfigsPath   string `json:"chHostConfigsPath"   yaml:"chHostConfigsPath"`
	CHUsersConfigsPath  string `json:"chUsersConfigsPath"  yaml:"chUsersConfigsPath"`

	// Path where to look for ClickHouseInstallation templates .yaml files
	CHITemplatesPath string `json:"chiTemplatesPath" yaml:"chiTemplatesPath"`
	// Create/Update StatefulSet behavior - for how long to wait for StatefulSet to reach new Generation
	StatefulSetUpdateTimeout uint64 `json:"statefulSetUpdateTimeout" yaml:"statefulSetUpdateTimeout"`
	// Create/Update StatefulSet behavior - for how long to sleep while polling StatefulSet to reach new Generation
	StatefulSetUpdatePollPeriod uint64 `json:"statefulSetUpdatePollPeriod" yaml:"statefulSetUpdatePollPeriod"`

	// Rolling Create/Update behavior
	// StatefulSet create behavior - what to do in case StatefulSet can't reach new Generation
	OnStatefulSetCreateFailureAction string `json:"onStatefulSetCreateFailureAction" yaml:"onStatefulSetCreateFailureAction"`
	// StatefulSet update behavior - what to do in case StatefulSet can't reach new Generation
	OnStatefulSetUpdateFailureAction string `json:"onStatefulSetUpdateFailureAction" yaml:"onStatefulSetUpdateFailureAction"`

	// Default values for ClickHouse user configuration
	// 1. user/profile - string
	// 2. user/quota - string
	// 3. user/networks/ip - multiple strings
	// 4. user/password - string
	CHConfigUserDefaultProfile    string   `json:"chConfigUserDefaultProfile"    yaml:"chConfigUserDefaultProfile"`
	CHConfigUserDefaultQuota      string   `json:"chConfigUserDefaultQuota"      yaml:"chConfigUserDefaultQuota"`
	CHConfigUserDefaultNetworksIP []string `json:"chConfigUserDefaultNetworksIP" yaml:"chConfigUserDefaultNetworksIP"`
	CHConfigUserDefaultPassword   string   `json:"chConfigUserDefaultPassword"   yaml:"chConfigUserDefaultPassword"`

	CHConfigNetworksHostRegexpTemplate string `json:"chConfigNetworksHostRegexpTemplate" yaml:"chConfigNetworksHostRegexpTemplate"`

	// Username and Password to be used by operator to connect to ClickHouse instances
	// for
	// 1. Metrics requests
	// 2. Schema maintenance
	// User credentials can be specified in additional ClickHouse config files located in `chUsersConfigsPath` folder
	CHScheme   string `json:"chScheme" yaml:"chScheme"`
	CHUsername string `json:"chUsername" yaml:"chUsername"`
	CHPassword string `json:"chPassword" yaml:"chPassword"`
	// Location of k8s Secret with username and password to be used by operator to connect to ClickHouse instances
	// Can be used instead of explicitly specified username and password
	CHCredentialsSecretNamespace string `json:"chCredentialsSecretNamespace" yaml:"chCredentialsSecretNamespace"`
	CHCredentialsSecretName      string `json:"chCredentialsSecretName"      yaml:"chCredentialsSecretName"`

	// Port where to connect to ClickHouse instances to
	CHPort int `json:"chPort"     yaml:"chPort"`

	// Logger section
	LogToStderr     string `json:"logtostderr"      yaml:"logtostderr"`
	AlsoLogToStderr string `json:"alsologtostderr"  yaml:"alsologtostderr"`
	V               string `json:"v"                yaml:"v"`
	StderrThreshold string `json:"stderrthreshold"  yaml:"stderrthreshold"`
	VModule         string `json:"vmodule"          yaml:"vmodule"`
	LogBacktraceAt  string `json:"log_backtrace_at" yaml:"log_backtrace_at"`
	// Max number of concurrent reconciles in progress
	ReconcileThreadsNumber int  `json:"reconcileThreadsNumber" yaml:"reconcileThreadsNumber"`
	ReconcileWaitExclude   bool `json:"reconcileWaitExclude"   yaml:"reconcileWaitExclude"`
	ReconcileWaitInclude   bool `json:"reconcileWaitInclude"   yaml:"reconcileWaitInclude"`

	// When transferring annotations from the chi/chit.metadata to CHI objects, use these filters.
	IncludeIntoPropagationAnnotations []string `json:"includeIntoPropagationAnnotations" yaml:"includeIntoPropagationAnnotations"`
	ExcludeFromPropagationAnnotations []string `json:"excludeFromPropagationAnnotations" yaml:"excludeFromPropagationAnnotations"`

	// When transferring labels from the chi/chit.metadata to child objects, use these filters.
	IncludeIntoPropagationLabels []string `json:"includeIntoPropagationLabels" yaml:"includeIntoPropagationLabels"`
	ExcludeFromPropagationLabels []string `json:"excludeFromPropagationLabels" yaml:"excludeFromPropagationLabels"`

	// Whether to append *Scope* labels to StatefulSet and Pod.
	AppendScopeLabelsString string `json:"appendScopeLabels" yaml:"appendScopeLabels"`

	// Grace period for Pod termination.
	TerminationGracePeriod int `json:"terminationGracePeriod" yaml:"terminationGracePeriod"`
	// Revision history limit
	RevisionHistoryLimit int `json:"revisionHistoryLimit" yaml:"revisionHistoryLimit"`
}

// MergeFrom merges
func (c *OperatorConfig) MergeFrom(from *OperatorConfig, _type MergeType) error {
	if from == nil {
		return nil
	}

	switch _type {
	case MergeTypeFillEmptyValues:
		if err := mergo.Merge(c, *from); err != nil {
			return fmt.Errorf("FAIL merge config Error: %q", err)
		}
	case MergeTypeOverrideByNonEmptyValues:
		if err := mergo.Merge(c, *from, mergo.WithOverride); err != nil {
			return fmt.Errorf("FAIL merge config Error: %q", err)
		}
	}

	return nil
}

// readCHITemplates build OperatorConfig.CHITemplate from template files content
func (c *OperatorConfig) readCHITemplates() (errs []error) {
	// Read CHI template files
	c.Template.CHI.Runtime.TemplateFiles = util.ReadFilesIntoMap(c.Template.CHI.Path, c.isCHITemplateExt)

	// Produce map of CHI templates out of CHI template files
	for filename := range c.Template.CHI.Runtime.TemplateFiles {
		template := new(ClickHouseInstallation)
		if err := yaml.Unmarshal([]byte(c.Template.CHI.Runtime.TemplateFiles[filename]), template); err != nil {
			// Unable to unmarshal - skip incorrect template
			errs = append(errs, fmt.Errorf("FAIL readCHITemplates() unable to unmarshal file %s Error: %q", filename, err))
			continue
		}
		c.enlistCHITemplate(template)
	}

	return
}

// enlistCHITemplate inserts template into templates catalog
func (c *OperatorConfig) enlistCHITemplate(template *ClickHouseInstallation) {
	if c.Template.CHI.Runtime.Templates == nil {
		c.Template.CHI.Runtime.Templates = make([]*ClickHouseInstallation, 0)
	}
	c.Template.CHI.Runtime.Templates = append(c.Template.CHI.Runtime.Templates, template)
}

// unlistCHITemplate removes template from templates catalog
func (c *OperatorConfig) unlistCHITemplate(template *ClickHouseInstallation) {
	if c.Template.CHI.Runtime.Templates == nil {
		return
	}

	// Nullify found template entry
	for _, _template := range c.Template.CHI.Runtime.Templates {
		if (_template.Name == template.Name) && (_template.Namespace == template.Namespace) {
			// TODO normalize
			//config.CHITemplates[i] = nil
			_template.Name = ""
			_template.Namespace = ""
		}
	}
	// Compact the slice
	// TODO compact the slice
}

// FindTemplate finds specified template
func (c *OperatorConfig) FindTemplate(use *ChiUseTemplate, namespace string) *ClickHouseInstallation {
	// Try to find direct match
	for _, _template := range c.Template.CHI.Runtime.Templates {
		if _template.MatchFullName(use.Namespace, use.Name) {
			// Direct match, found result
			return _template
		}
	}

	// Direct match is not possible.

	if use.Namespace != "" {
		// With fully-specified use template direct (full name) only match is applicable, and it is not possible
		// This is strange situation, however
		return nil
	}

	// Improvise with use.Namespace

	for _, _template := range c.Template.CHI.Runtime.Templates {
		if _template.MatchFullName(namespace, use.Name) {
			// Found template with searched name in specified namespace
			return _template
		}
	}

	return nil
}

// GetAutoTemplates gets all auto templates.
// Auto templates are sorted alphabetically by tuple: namespace, name
func (c *OperatorConfig) GetAutoTemplates() []*ClickHouseInstallation {
	// Extract auto-templates from all templates listed
	var auto []*ClickHouseInstallation
	for _, _template := range c.Template.CHI.Runtime.Templates {
		if _template.IsAuto() {
			auto = append(auto, _template)
		}
	}

	// Sort namespaces
	var namespaces []string
	for _, _template := range auto {
		found := false
		for _, namespace := range namespaces {
			if namespace == _template.Namespace {
				// Already has it
				found = true
				break
			}
		}
		if !found {
			namespaces = append(namespaces, _template.Namespace)
		}
	}
	sort.Strings(namespaces)

	var res []*ClickHouseInstallation
	for _, namespace := range namespaces {
		// Sort names
		var names []string
		for _, _template := range auto {
			if _template.Namespace == namespace {
				names = append(names, _template.Name)
			}
		}
		sort.Strings(names)

		for _, name := range names {
			for _, _template := range auto {
				if (_template.Namespace == namespace) && (_template.Name == name) {
					res = append(res, _template)
				}
			}
		}
	}

	return res
}

// buildUnifiedCHITemplate builds combined CHI Template from templates catalog
func (c *OperatorConfig) buildUnifiedCHITemplate() {

	return
	/*
		// Build unified template in case there are templates to build from only
		if len(config.CHITemplates) == 0 {
			return
		}

		// Sort CHI templates by their names and in order to apply orderly
		// Extract file names into slice and sort it
		// Then we'll loop over templates in sorted order (by filenames) and apply them one-by-one
		var sortedTemplateNames []string
		for name := range config.CHITemplates {
			// Convenience wrapper
			template := config.CHITemplates[name]
			sortedTemplateNames = append(sortedTemplateNames, template.Name)
		}
		sort.Strings(sortedTemplateNames)

		// Create final combined template
		config.CHITemplate = new(ClickHouseInstallation)

		// Extract templates in sorted order - according to sorted template names
		for _, name := range sortedTemplateNames {
			// Convenience wrapper
			template := config.CHITemplates[name]
			// Merge into accumulated target template from current template
			config.CHITemplate.MergeFrom(template)
		}

		// Log final CHI template obtained
		// Marshaling is done just to print nice yaml
		if bytes, err := yaml.Marshal(config.CHITemplate); err == nil {
			log.V(1).Infof("Unified CHITemplate:\n%s\n", string(bytes))
		} else {
			log.V(1).Infof("FAIL unable to Marshal Unified CHITemplate")
		}
	*/
}

// AddCHITemplate adds CHI template
func (c *OperatorConfig) AddCHITemplate(template *ClickHouseInstallation) {
	c.enlistCHITemplate(template)
	c.buildUnifiedCHITemplate()
}

// UpdateCHITemplate updates CHI template
func (c *OperatorConfig) UpdateCHITemplate(template *ClickHouseInstallation) {
	c.enlistCHITemplate(template)
	c.buildUnifiedCHITemplate()
}

// DeleteCHITemplate deletes CHI template
func (c *OperatorConfig) DeleteCHITemplate(template *ClickHouseInstallation) {
	c.unlistCHITemplate(template)
	c.buildUnifiedCHITemplate()
}

// Postprocess runs all postprocessors
func (c *OperatorConfig) Postprocess() {
	c.normalize()
	c.readClickHouseCustomConfigFiles()
	c.readCHITemplates()
	c.buildUnifiedCHITemplate()
	c.applyEnvVarParams()
	c.applyDefaultWatchNamespace()
}

func (c *OperatorConfig) normalizeConfigurationFilesSection() {
	// Process ClickHouse configuration files section
	// Apply default paths in case nothing specified
	util.PreparePath(&c.ClickHouse.Config.File.Path.Common, c.Runtime.ConfigFolderPath, CommonConfigDir)
	util.PreparePath(&c.ClickHouse.Config.File.Path.Host, c.Runtime.ConfigFolderPath, HostConfigDir)
	util.PreparePath(&c.ClickHouse.Config.File.Path.User, c.Runtime.ConfigFolderPath, UsersConfigDir)

	// Process ClickHouseInstallation templates section
	util.PreparePath(&c.Template.CHI.Path, c.Runtime.ConfigFolderPath, TemplatesDir)
}

func (c *OperatorConfig) normalizeUpdateSection() {
	// Process Create/Update section

	// Timeouts
	if c.Reconcile.StatefulSet.Update.Timeout == 0 {
		// Default update timeout in seconds
		c.Reconcile.StatefulSet.Update.Timeout = defaultStatefulSetUpdateTimeout
	}

	if c.Reconcile.StatefulSet.Update.PollInterval == 0 {
		// Default polling period in seconds
		c.Reconcile.StatefulSet.Update.PollInterval = defaultStatefulSetUpdatePollInterval
	}

	// Default action on Create/Update failure - to keep system in previous state

	// Default Create Failure action - delete
	if c.Reconcile.StatefulSet.Create.OnFailure == "" {
		c.Reconcile.StatefulSet.Create.OnFailure = OnStatefulSetCreateFailureActionDelete
	}

	// Default Updated Failure action - revert
	if c.Reconcile.StatefulSet.Update.OnFailure == "" {
		c.Reconcile.StatefulSet.Update.OnFailure = OnStatefulSetUpdateFailureActionRollback
	}
}

func (c *OperatorConfig) normalizeSettingsSection() {
	// Default values for ClickHouse user configuration
	// 1. user/profile
	// 2. user/quota
	// 3. user/networks/ip
	// 4. user/password
	if c.ClickHouse.Config.User.Default.Profile == "" {
		c.ClickHouse.Config.User.Default.Profile = defaultChConfigUserDefaultProfile
	}
	if c.ClickHouse.Config.User.Default.Quota == "" {
		c.ClickHouse.Config.User.Default.Quota = defaultChConfigUserDefaultQuota
	}
	if len(c.ClickHouse.Config.User.Default.NetworksIP) == 0 {
		c.ClickHouse.Config.User.Default.NetworksIP = []string{defaultChConfigUserDefaultNetworkIP}
	}
	if c.ClickHouse.Config.User.Default.Password == "" {
		c.ClickHouse.Config.User.Default.Password = defaultChConfigUserDefaultPassword
	}

	// chConfigNetworksHostRegexpTemplate
}

func (c *OperatorConfig) normalizeAccessSection() {
	// Username and Password to be used by operator to connect to ClickHouse instances for
	// 1. Metrics requests
	// 2. Schema maintenance
	// User credentials can be specified in additional ClickHouse config files located in `chUsersConfigsPath` folder
	if c.ClickHouse.Access.Scheme == "" {
		c.ClickHouse.Access.Scheme = defaultChScheme
	}
	if c.ClickHouse.Access.Username == "" {
		c.ClickHouse.Access.Username = defaultChUsername
	}
	if c.ClickHouse.Access.Password == "" {
		c.ClickHouse.Access.Password = defaultChPassword
	}

	// config.CHCredentialsSecretNamespace
	// config.CHCredentialsSecretName

	// Overwrite credentials with data from the secret (if both username and password provided)
	if (c.ClickHouse.Access.Secret.Runtime.Username != "") && (c.ClickHouse.Access.Secret.Runtime.Password != "") {
		c.ClickHouse.Access.Username = c.ClickHouse.Access.Secret.Runtime.Username
		c.ClickHouse.Access.Password = c.ClickHouse.Access.Secret.Runtime.Password
	}

	if c.ClickHouse.Access.Port == 0 {
		c.ClickHouse.Access.Port = defaultChPort
	}
}

func (c *OperatorConfig) normalizeLogSection() {
	// Logtostderr      string `json:"logtostderr"      yaml:"logtostderr"`
	// Alsologtostderr  string `json:"alsologtostderr"  yaml:"alsologtostderr"`
	// V                string `json:"v"                yaml:"v"`
	// Stderrthreshold  string `json:"stderrthreshold"  yaml:"stderrthreshold"`
	// Vmodule          string `json:"vmodule"          yaml:"vmodule"`
	// Log_backtrace_at string `json:"log_backtrace_at" yaml:"log_backtrace_at"`
}

func (c *OperatorConfig) normalizeRuntimeSection() {
	if c.Reconcile.Runtime.ThreadsNumber == 0 {
		c.Reconcile.Runtime.ThreadsNumber = defaultReconcileThreadsNumber
	}

	//reconcileWaitExclude: true
	//reconcileWaitInclude: false
}

func (c *OperatorConfig) normalizeLabelsSection() {
	//config.IncludeIntoPropagationAnnotations
	//config.ExcludeFromPropagationAnnotations
	//config.IncludeIntoPropagationLabels
	//config.ExcludeFromPropagationLabels
	// Whether to append *Scope* labels to StatefulSet and Pod.
	c.Label.Runtime.AppendScope = util.IsStringBoolTrue(c.Label.AppendScopeString)
}

func (c *OperatorConfig) normalizePodManagementSection() {
	if c.Pod.TerminationGracePeriod == 0 {
		c.Pod.TerminationGracePeriod = defaultTerminationGracePeriod
	}
	if c.StatefulSet.RevisionHistoryLimit == 0 {
		c.StatefulSet.RevisionHistoryLimit = defaultRevisionHistoryLimit
	}
}

// normalize() makes fully-and-correctly filled OperatorConfig
func (c *OperatorConfig) normalize() {
	c.move()
	c.Runtime.Namespace = os.Getenv(OPERATOR_POD_NAMESPACE)

	c.normalizeConfigurationFilesSection()
	c.normalizeUpdateSection()
	c.normalizeSettingsSection()
	c.normalizeAccessSection()
	c.normalizeLogSection()
	c.normalizeRuntimeSection()
	c.normalizeLabelsSection()
	c.normalizePodManagementSection()
}

// applyEnvVarParams applies ENV VARS over config
func (c *OperatorConfig) applyEnvVarParams() {
	if ns := os.Getenv(WATCH_NAMESPACE); len(ns) > 0 {
		// We have WATCH_NAMESPACE explicitly specified
		c.Watch.Namespaces = []string{ns}
	}

	if nss := os.Getenv(WATCH_NAMESPACES); len(nss) > 0 {
		// We have WATCH_NAMESPACES explicitly specified
		namespaces := strings.FieldsFunc(nss, func(r rune) bool {
			return r == ':' || r == ','
		})
		c.Watch.Namespaces = []string{}
		for i := range namespaces {
			if len(namespaces[i]) > 0 {
				c.Watch.Namespaces = append(c.Watch.Namespaces, namespaces[i])
			}
		}
	}
}

// applyDefaultWatchNamespace applies default watch namespace in case none specified earlier
func (c *OperatorConfig) applyDefaultWatchNamespace() {
	// In case we have watched namespaces specified, all is fine
	// In case we do not have watched namespaces specified, we need to decide, what namespace to watch.
	// In this case, there are two options:
	// 1. Operator runs in kube-system namespace - assume this is global installation, need to watch ALL namespaces
	// 2. Operator runs in other (non kube-system) namespace - assume this is local installation, watch this namespace only
	// Watch in own namespace only in case no other specified earlier

	if len(c.Watch.Namespaces) > 0 {
		// We have namespace(s) specified already
		return
	}

	// No namespaces specified

	if c.Runtime.Namespace == "kube-system" {
		// Operator is running in system namespace
		// Do nothing, we already have len(config.WatchNamespaces) == 0
	} else {
		// Operator is running is explicit namespace. Watch in it
		c.Watch.Namespaces = []string{
			c.Runtime.Namespace,
		}
	}
}

// readClickHouseCustomConfigFiles reads all extra user-specified ClickHouse config files
func (c *OperatorConfig) readClickHouseCustomConfigFiles() {
	c.ClickHouse.Config.File.Runtime.CommonConfigFiles = util.ReadFilesIntoMap(c.ClickHouse.Config.File.Path.Common, c.isCHConfigExt)
	c.ClickHouse.Config.File.Runtime.HostConfigFiles = util.ReadFilesIntoMap(c.ClickHouse.Config.File.Path.Host, c.isCHConfigExt)
	c.ClickHouse.Config.File.Runtime.UsersConfigFiles = util.ReadFilesIntoMap(c.ClickHouse.Config.File.Path.User, c.isCHConfigExt)
}

// isCHConfigExt returns true in case specified file has proper extension for a ClickHouse config file
func (c *OperatorConfig) isCHConfigExt(file string) bool {
	switch util.ExtToLower(file) {
	case ".xml":
		return true
	}
	return false
}

// isCHITemplateExt returns true in case specified file has proper extension for a CHI template config file
func (c *OperatorConfig) isCHITemplateExt(file string) bool {
	switch util.ExtToLower(file) {
	case ".yaml":
		return true
	case ".json":
		return true
	}
	return false
}

// String returns string representation of a OperatorConfig
func (c *OperatorConfig) String(hideCredentials bool) string {
	conf := c
	if hideCredentials {
		conf = c.DeepCopy()
		conf.ClickHouse.Config.User.Default.Password = PasswordReplacer
		conf.ClickHouse.Access.Username = UsernameReplacer
		conf.ClickHouse.Access.Password = PasswordReplacer
		conf.ClickHouse.Access.Secret.Runtime.Username = UsernameReplacer
		conf.ClickHouse.Access.Secret.Runtime.Password = PasswordReplacer

		// DEPRECATED
		conf.CHConfigUserDefaultPassword = PasswordReplacer
		conf.CHUsername = UsernameReplacer
		conf.CHPassword = PasswordReplacer
	}
	if bytes, err := yaml.Marshal(conf); err == nil {
		return string(bytes)
	}

	return ""
}

// IsWatchedNamespace returns whether specified namespace is in a list of watched
// TODO unify with GetInformerNamespace
func (c *OperatorConfig) IsWatchedNamespace(namespace string) bool {
	// In case no namespaces specified - watch all namespaces
	if len(c.Watch.Namespaces) == 0 {
		return true
	}

	return util.InArrayWithRegexp(namespace, c.Watch.Namespaces)
}

// GetInformerNamespace is a TODO stub
// Namespace where informers would watch notifications from
// The thing is that InformerFactory can accept only one parameter as watched namespace,
// be it explicitly specified namespace or empty line for "all namespaces".
// That's what conflicts with CHOp's approach to 'specify list of namespaces to watch in', having
// slice of namespaces (CHOp's approach) incompatible with "one namespace name" approach
// TODO unify with IsWatchedNamespace
// TODO unify approaches to multiple namespaces support
func (c *OperatorConfig) GetInformerNamespace() string {
	// Namespace where informers would watch notifications from
	namespace := metav1.NamespaceAll
	if len(c.Watch.Namespaces) == 1 {
		// We have exactly one watch namespace specified
		// This scenario is implemented in go-client
		// In any other case, just keep metav1.NamespaceAll

		// This contradicts current implementation of multiple namespaces in config's watchNamespaces field,
		// but k8s has possibility to specify one/all namespaces only, no 'multiple namespaces' option
		var labelRegexp = regexp.MustCompile("^[a-z]([-a-z0-9]*[a-z0-9])?$")
		if labelRegexp.MatchString(c.Watch.Namespaces[0]) {
			namespace = c.Watch.Namespaces[0]
		}
	}

	return namespace
}

// GetLogLevel gets logger level
func (c *OperatorConfig) GetLogLevel() (log.Level, error) {
	if i, err := strconv.Atoi(c.Logger.V); err == nil {
		return log.Level(i), nil
	}
	return 0, fmt.Errorf("incorrect V value")
}

// GetTerminationGracePeriod gets pointer to terminationGracePeriod, as expected by
// statefulSet.Spec.Template.Spec.TerminationGracePeriodSeconds
func (c *OperatorConfig) GetTerminationGracePeriod() *int64 {
	terminationGracePeriod := int64(c.Pod.TerminationGracePeriod)
	return &terminationGracePeriod
}

// GetRevisionHistoryLimit gets pointer to revisionHistoryLimit, as expected by
// statefulSet.Spec.Template.Spec.RevisionHistoryLimit
func (c *OperatorConfig) GetRevisionHistoryLimit() *int32 {
	revisionHistoryLimit := int32(c.StatefulSet.RevisionHistoryLimit)
	return &revisionHistoryLimit
}

func (c *OperatorConfig) move() {
	// WatchNamespaces where operator watches for events
	if len(c.WatchNamespaces) > 0 {
		c.Watch.Namespaces = c.WatchNamespaces
	}

	if c.CHCommonConfigsPath != "" {
		c.ClickHouse.Config.File.Path.Common = c.CHCommonConfigsPath
	}
	if c.CHHostConfigsPath != "" {
		c.ClickHouse.Config.File.Path.Host = c.CHHostConfigsPath
	}
	if c.CHUsersConfigsPath != "" {
		c.ClickHouse.Config.File.Path.User = c.CHUsersConfigsPath
	}

	// Path where to look for ClickHouseInstallation templates .yaml files
	if c.CHITemplatesPath != "" {
		c.Template.CHI.Path = c.CHITemplatesPath
	}
	// Create/Update StatefulSet behavior - for how long to wait for StatefulSet to reach new Generation
	if c.StatefulSetUpdateTimeout != 0 {
		c.Reconcile.StatefulSet.Update.Timeout = c.StatefulSetUpdateTimeout
	}
	// Create/Update StatefulSet behavior - for how long to sleep while polling StatefulSet to reach new Generation
	if c.StatefulSetUpdatePollPeriod != 0 {
		c.Reconcile.StatefulSet.Update.PollInterval = c.StatefulSetUpdatePollPeriod
	}

	// Rolling Create/Update behavior
	// StatefulSet create behavior - what to do in case StatefulSet can't reach new Generation
	if c.OnStatefulSetCreateFailureAction != "" {
		c.Reconcile.StatefulSet.Create.OnFailure = c.OnStatefulSetCreateFailureAction
	}
	// StatefulSet update behavior - what to do in case StatefulSet can't reach new Generation
	if c.OnStatefulSetUpdateFailureAction != "" {
		c.Reconcile.StatefulSet.Update.OnFailure = c.OnStatefulSetUpdateFailureAction
	}

	// Default values for ClickHouse user configuration
	// 1. user/profile - string
	// 2. user/quota - string
	// 3. user/networks/ip - multiple strings
	// 4. user/password - string
	if c.CHConfigUserDefaultProfile != "" {
		c.ClickHouse.Config.User.Default.Profile = c.CHConfigUserDefaultProfile
	}
	if c.CHConfigUserDefaultQuota != "" {
		c.ClickHouse.Config.User.Default.Quota = c.CHConfigUserDefaultQuota
	}
	if len(c.CHConfigUserDefaultNetworksIP) > 0 {
		c.ClickHouse.Config.User.Default.NetworksIP = c.CHConfigUserDefaultNetworksIP
	}
	if c.CHConfigUserDefaultPassword != "" {
		c.ClickHouse.Config.User.Default.Password = c.CHConfigUserDefaultPassword
	}

	if c.CHConfigNetworksHostRegexpTemplate != "" {
		c.ClickHouse.Config.Network.HostRegexpTemplate = c.CHConfigNetworksHostRegexpTemplate
	}

	// Username and Password to be used by operator to connect to ClickHouse instances
	// for
	// 1. Metrics requests
	// 2. Schema maintenance
	// User credentials can be specified in additional ClickHouse config files located in `chUsersConfigsPath` folder
	if c.CHScheme != "" {
		c.ClickHouse.Access.Password = c.CHScheme
	}
	if c.CHUsername != "" {
		c.ClickHouse.Access.Username = c.CHUsername
	}
	if c.CHPassword != "" {
		c.ClickHouse.Access.Password = c.CHPassword
	}
	// Location of k8s Secret with username and password to be used by operator to connect to ClickHouse instances
	// Can be used instead of explicitly specified username and password
	if c.CHCredentialsSecretNamespace != "" {
		c.ClickHouse.Access.Secret.Namespace = c.CHCredentialsSecretNamespace
	}
	if c.CHCredentialsSecretName != "" {
		c.ClickHouse.Access.Secret.Name = c.CHCredentialsSecretName
	}

	// Port where to connect to ClickHouse instances to
	if c.CHPort != 0 {
		c.ClickHouse.Access.Port = c.CHPort
	}

	// Logger section
	if c.LogToStderr != "" {
		c.Logger.LogToStderr = c.LogToStderr
	}
	if c.AlsoLogToStderr != "" {
		c.Logger.AlsoLogToStderr = c.AlsoLogToStderr
	}
	if c.V != "" {
		c.Logger.V = c.V
	}
	if c.StderrThreshold != "" {
		c.Logger.StderrThreshold = c.StderrThreshold
	}
	if c.VModule != "" {
		c.Logger.VModule = c.VModule
	}
	if c.LogBacktraceAt != "" {
		c.Logger.LogBacktraceAt = c.LogBacktraceAt
	}
	// Max number of concurrent reconciles in progress
	if c.ReconcileThreadsNumber != 0 {
		c.Reconcile.Runtime.ThreadsNumber = c.ReconcileThreadsNumber
	}
	if c.ReconcileWaitExclude {
		c.Reconcile.Host.Wait.Exclude = c.ReconcileWaitExclude
	}
	if c.ReconcileWaitInclude {
		c.Reconcile.Host.Wait.Include = c.ReconcileWaitInclude
	}

	// When transferring annotations from the chi/chit.metadata to CHI objects, use these filters.
	if len(c.IncludeIntoPropagationAnnotations) > 0 {
		c.Annotation.Include = c.IncludeIntoPropagationAnnotations
	}
	if len(c.ExcludeFromPropagationAnnotations) > 0 {
		c.Annotation.Exclude = c.ExcludeFromPropagationAnnotations
	}

	// When transferring labels from the chi/chit.metadata to child objects, use these filters.
	if len(c.IncludeIntoPropagationLabels) > 0 {
		c.Label.Include = c.IncludeIntoPropagationLabels
	}
	if len(c.ExcludeFromPropagationLabels) > 0 {
		c.Label.Exclude = c.ExcludeFromPropagationLabels
	}

	// Whether to append *Scope* labels to StatefulSet and Pod.
	if c.AppendScopeLabelsString != "" {
		c.Label.AppendScopeString = c.AppendScopeLabelsString
	}

	// Grace period for Pod termination.
	if c.TerminationGracePeriod != 0 {
		c.Pod.TerminationGracePeriod = c.TerminationGracePeriod
	}
	// Revision history limit
	if c.RevisionHistoryLimit != 0 {
		c.StatefulSet.RevisionHistoryLimit = c.RevisionHistoryLimit
	}

}
