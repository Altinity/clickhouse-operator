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

	// Possible values for ClickHouse scheme

	// ChSchemeHTTP specifies HTTP access scheme
	ChSchemeHTTP = "http"
	// ChSchemeHTTPS specifies HTTPS access scheme
	ChSchemeHTTPS = "https"
	// ChSchemeAuto specifies that operator has to decide itself should https or http be used
	ChSchemeAuto = "auto"

	// Username and Password to be used by operator to connect to ClickHouse instances for
	// 1. Metrics requests
	// 2. Schema maintenance
	// User credentials can be specified in additional ClickHouse config files located in `chUsersConfigsPath` folder
	defaultChScheme   = ChSchemeAuto
	defaultChUsername = ""
	defaultChPassword = ""
	defaultChPort     = 8123
	defaultChRootCA   = ""

	// Timeouts used to limit connection and queries from the operator to ClickHouse instances. In seconds
	// defaultTimeoutConnect specifies default timeout to connect to the ClickHouse instance. In seconds
	defaultTimeoutConnect = 2
	// defaultTimeoutQuery specifies default timeout to query the CLickHouse instance. In seconds
	defaultTimeoutQuery = 5
	// defaultTimeoutCollect specifies default timeout to collect metrics from the ClickHouse instance. In seconds
	defaultTimeoutCollect = 8

	// defaultReconcileCHIsThreadsNumber specifies default number of controller threads running concurrently.
	// Used in case no other specified in config
	defaultReconcileCHIsThreadsNumber = 1

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

// Username/password replacers
const (
	UsernameReplacer = "***"
	PasswordReplacer = "***"
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

// OperatorConfigRestartPolicyRuleSet specifies set of rules
type OperatorConfigRestartPolicyRuleSet map[Matchable]StringBool

// OperatorConfigRestartPolicyRule specifies ClickHouse version and rules for this version
type OperatorConfigRestartPolicyRule struct {
	Version string
	Rules   []OperatorConfigRestartPolicyRuleSet
}

// OperatorConfigRestartPolicy specifies operator's configuration changes restart policy
type OperatorConfigRestartPolicy struct {
	Rules []OperatorConfigRestartPolicyRule
}

// OperatorConfigFile specifies File section
type OperatorConfigFile struct {
	Path struct {
		// Paths where to look for additional ClickHouse config .xml files to be mounted into Pod
		Common string `json:"common" yaml:"common"`
		Host   string `json:"host"   yaml:"host"`
		User   string `json:"user"   yaml:"user"`
	} `json:"path" yaml:"path"`

	Runtime OperatorConfigFileRuntime `json:"runtime,omitempty" yaml:"runtime,omitempty"`
}

// OperatorConfigFileRuntime specifies runtime section
type OperatorConfigFileRuntime struct {
	// OperatorConfig files fetched from paths specified above. Maps "file name->file content"
	CommonConfigFiles map[string]string `json:"commonConfigFiles,omitempty" yaml:"commonConfigFiles,omitempty"`
	HostConfigFiles   map[string]string `json:"hostConfigFiles,omitempty"   yaml:"hostConfigFiles,omitempty"`
	UsersConfigFiles  map[string]string `json:"usersConfigFiles,omitempty"  yaml:"usersConfigFiles,omitempty"`
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
	Profile    string   `json:"profile"    yaml:"profile"`
	Quota      string   `json:"quota"      yaml:"quota"`
	NetworksIP []string `json:"networksIP" yaml:"networksIP"`
	Password   string   `json:"password"   yaml:"password"`
}

// type RestartPolicy map[Matchable]StringBool

// OperatorConfigClickHouse specifies ClickHouse section
type OperatorConfigClickHouse struct {
	Config              OperatorConfigConfig        `json:"configuration" yaml:"configuration"`
	ConfigRestartPolicy OperatorConfigRestartPolicy `json:"configurationRestartPolicy" yaml:"configurationRestartPolicy"`

	Access struct {
		// Username and Password to be used by operator to connect to ClickHouse instances
		// for
		// 1. Metrics requests
		// 2. Schema maintenance
		// User credentials can be specified in additional ClickHouse config files located in `chUsersConfigsPath` folder
		Scheme   string `json:"scheme,omitempty"   yaml:"scheme,omitempty"`
		Username string `json:"username,omitempty" yaml:"username,omitempty"`
		Password string `json:"password,omitempty" yaml:"password,omitempty"`
		RootCA   string `json:"rootCA,omitempty"   yaml:"rootCA,omitempty"`

		// Location of k8s Secret with username and password to be used by the operator to connect to ClickHouse instances
		// Can be used instead of explicitly specified (above) username and password
		Secret struct {
			Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
			Name      string `json:"name,omitempty"      yaml:"name,omitempty"`

			Runtime struct {
				// Username and Password to be used by operator to connect to ClickHouse instances
				// extracted from k8s secret specified above.
				Username string
				Password string
				Fetched  bool
				Error    string
			}
		} `json:"secret" yaml:"secret"`

		// Port where to connect to ClickHouse instances to
		Port int `json:"port" yaml:"port"`

		// Timeouts used to limit connection and queries from the operator to ClickHouse instances
		Timeouts struct {
			Connect time.Duration `json:"connect" yaml:"connect"`
			Query   time.Duration `json:"query"   yaml:"query"`
		} `json:"timeouts" yaml:"timeouts"`
	} `json:"access" yaml:"access"`

	// Metrics used to specify how the operator fetches metrics from ClickHouse instances
	Metrics struct {
		Timeouts struct {
			Collect time.Duration `json:"collect" yaml:"collect"`
		} `json:"timeouts" yaml:"timeouts"`
	} `json:"metrics" yaml:"metrics"`
}

// OperatorConfigTemplate specifies template section
type OperatorConfigTemplate struct {
	CHI OperatorConfigCHI `json:"chi" yaml:"chi"`
}

// OperatorConfigCHIPolicy specifies string value of .template.chi.policy
type OperatorConfigCHIPolicy string

// String is a stringifier
func (p OperatorConfigCHIPolicy) String() string {
	return string(p)
}

// ToLower provides the same functionality as strings.ToLower()
func (p OperatorConfigCHIPolicy) ToLower() string {
	return strings.ToLower(p.String())
}

// Equals checks whether OperatorConfigCHIPolicy is equal to another one
func (p OperatorConfigCHIPolicy) Equals(another OperatorConfigCHIPolicy) bool {
	return p.ToLower() == another.ToLower()
}

// Possible values for OperatorConfigCHIPolicy
const (
	OperatorConfigCHIPolicyReadOnStart          OperatorConfigCHIPolicy = "ReadOnStart"
	OperatorConfigCHIPolicyApplyOnNextReconcile OperatorConfigCHIPolicy = "ApplyOnNextReconcile"
	defaultOperatorConfigCHIPolicy              OperatorConfigCHIPolicy = OperatorConfigCHIPolicyApplyOnNextReconcile
)

// OperatorConfigCHI specifies template CHI section
type OperatorConfigCHI struct {
	// Policy specifies how to handle CHITs
	Policy OperatorConfigCHIPolicy `json:"policy" yaml:"policy"`
	// Path where to look for ClickHouseInstallation templates .yaml files
	Path string `json:"path" yaml:"path"`

	Runtime OperatorConfigCHIRuntime `json:"runtime,omitempty" yaml:"runtime,omitempty"`
}

// OperatorConfigCHIRuntime specifies chi runtime section
type OperatorConfigCHIRuntime struct {
	// CHI template files fetched from the path specified above. Maps "file name->file content"
	TemplateFiles map[string]string `json:"templateFiles,omitempty" yaml:"templateFiles,omitempty"`
	// CHI template objects unmarshalled from CHITemplateFiles. Maps "metadata.name->object"
	Templates []*ClickHouseInstallation `json:"-" yaml:"-"`
	// ClickHouseInstallation template
	Template *ClickHouseInstallation `json:"-" yaml:"-"`
}

// OperatorConfigReconcile specifies reconcile section
type OperatorConfigReconcile struct {
	Runtime struct {
		ReconcileCHIsThreadsNumber           int `json:"reconcileCHIsThreadsNumber"           yaml:"reconcileCHIsThreadsNumber"`
		ReconcileShardsThreadsNumber         int `json:"reconcileShardsThreadsNumber"         yaml:"reconcileShardsThreadsNumber"`
		ReconcileShardsMaxConcurrencyPercent int `json:"reconcileShardsMaxConcurrencyPercent" yaml:"reconcileShardsMaxConcurrencyPercent"`

		// DEPRECATED, is replaced with reconcileCHIsThreadsNumber
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

	Host OperatorConfigReconcileHost `json:"host" yaml:"host"`
}

// OperatorConfigReconcileHost defines reconcile host config
type OperatorConfigReconcileHost struct {
	Wait OperatorConfigReconcileHostWait `json:"wait" yaml:"wait"`
}

// OperatorConfigReconcileHostWait defines reconcile host wait config
type OperatorConfigReconcileHostWait struct {
	Exclude *StringBool `json:"exclude,omitempty" yaml:"exclude,omitempty"`
	Queries *StringBool `json:"queries,omitempty" yaml:"queries,omitempty"`
	Include *StringBool `json:"include,omitempty" yaml:"include,omitempty"`
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
	AppendScopeString StringBool `json:"appendScope" yaml:"appendScope"`

	Runtime struct {
		AppendScope bool `json:"appendScope" yaml:"appendScope"`
	} `json:"runtime" yaml:"runtime"`
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
	Watch       OperatorConfigWatch      `json:"watch"      yaml:"watch"`
	ClickHouse  OperatorConfigClickHouse `json:"clickhouse" yaml:"clickhouse"`
	Template    OperatorConfigTemplate   `json:"template"   yaml:"template"`
	Reconcile   OperatorConfigReconcile  `json:"reconcile"  yaml:"reconcile"`
	Annotation  OperatorConfigAnnotation `json:"annotation" yaml:"annotation"`
	Label       OperatorConfigLabel      `json:"label"      yaml:"label"`
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
	AppendScopeLabelsString StringBool `json:"appendScopeLabels" yaml:"appendScopeLabels"`

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
			continue // skip to the next template
		}
		// Template read successfully, let's append it to the list
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
	var autoTemplates []*ClickHouseInstallation
	for _, _template := range c.Template.CHI.Runtime.Templates {
		if _template.IsAuto() {
			autoTemplates = append(autoTemplates, _template)
		}
	}

	// Prepare sorted list of namespaces
	var namespaces []string
	for _, _template := range autoTemplates {
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

	var sortedTemplates []*ClickHouseInstallation
	for _, namespace := range namespaces {
		// Prepare sorted list of names within this namespace
		var names []string
		for _, _template := range autoTemplates {
			if _template.Namespace == namespace {
				names = append(names, _template.Name)
			}
		}
		sort.Strings(names)

		// Walk over sorted list of names within this namespace and append to the result list of templates
		for _, name := range names {
			for _, _template := range autoTemplates {
				if (_template.Namespace == namespace) && (_template.Name == name) {
					sortedTemplates = append(sortedTemplates, _template)
				}
			}
		}
	}

	return sortedTemplates
}

// AddCHITemplate adds CHI template
func (c *OperatorConfig) AddCHITemplate(template *ClickHouseInstallation) {
	c.enlistCHITemplate(template)
}

// UpdateCHITemplate updates CHI template
func (c *OperatorConfig) UpdateCHITemplate(template *ClickHouseInstallation) {
	c.enlistCHITemplate(template)
}

// DeleteCHITemplate deletes CHI template
func (c *OperatorConfig) DeleteCHITemplate(template *ClickHouseInstallation) {
	c.unlistCHITemplate(template)
}

// Postprocess runs all postprocessors
func (c *OperatorConfig) Postprocess() {
	c.normalize()
	c.readClickHouseCustomConfigFiles()
	c.readCHITemplates()
	c.applyEnvVarParams()
	c.applyDefaultWatchNamespace()
}

func (c *OperatorConfig) normalizeSectionClickHouseConfigurationFile() {
	// Process ClickHouse configuration files section
	// Apply default paths in case nothing specified
	util.PreparePath(&c.ClickHouse.Config.File.Path.Common, c.Runtime.ConfigFolderPath, CommonConfigDir)
	util.PreparePath(&c.ClickHouse.Config.File.Path.Host, c.Runtime.ConfigFolderPath, HostConfigDir)
	util.PreparePath(&c.ClickHouse.Config.File.Path.User, c.Runtime.ConfigFolderPath, UsersConfigDir)

}

func (c *OperatorConfig) normalizeSectionTemplate() {
	p := c.Template.CHI.Policy
	switch {
	case p.Equals(OperatorConfigCHIPolicyReadOnStart):
		c.Template.CHI.Policy = OperatorConfigCHIPolicyReadOnStart
	case p.Equals(OperatorConfigCHIPolicyApplyOnNextReconcile):
		c.Template.CHI.Policy = OperatorConfigCHIPolicyApplyOnNextReconcile
	default:
		c.Template.CHI.Policy = defaultOperatorConfigCHIPolicy
	}

	// Process ClickHouseInstallation templates section
	util.PreparePath(&c.Template.CHI.Path, c.Runtime.ConfigFolderPath, TemplatesDir)
}

func (c *OperatorConfig) normalizeSectionReconcileStatefulSet() {
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

func (c *OperatorConfig) normalizeSectionClickHouseConfigurationUserDefault() {
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

func (c *OperatorConfig) normalizeSectionClickHouseAccess() {
	// Username and Password to be used by operator to connect to ClickHouse instances for
	// 1. Metrics requests
	// 2. Schema maintenance
	// User credentials can be specified in additional ClickHouse config files located in `chUsersConfigsPath` folder
	switch strings.ToLower(c.ClickHouse.Access.Scheme) {
	case ChSchemeHTTP:
		c.ClickHouse.Access.Scheme = ChSchemeHTTP
	case ChSchemeHTTPS:
		c.ClickHouse.Access.Scheme = ChSchemeHTTPS
	case ChSchemeAuto:
		c.ClickHouse.Access.Scheme = ChSchemeAuto
	default:
		c.ClickHouse.Access.Scheme = defaultChScheme
	}
	if c.ClickHouse.Access.Username == "" {
		c.ClickHouse.Access.Username = defaultChUsername
	}
	if c.ClickHouse.Access.Password == "" {
		c.ClickHouse.Access.Password = defaultChPassword
	}
	if c.ClickHouse.Access.RootCA == "" {
		c.ClickHouse.Access.RootCA = defaultChRootCA
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

	// Timeouts

	if c.ClickHouse.Access.Timeouts.Connect == 0 {
		c.ClickHouse.Access.Timeouts.Connect = defaultTimeoutConnect
	}
	// Adjust seconds to time.Duration
	c.ClickHouse.Access.Timeouts.Connect = c.ClickHouse.Access.Timeouts.Connect * time.Second

	if c.ClickHouse.Access.Timeouts.Query == 0 {
		c.ClickHouse.Access.Timeouts.Query = defaultTimeoutQuery
	}
	// Adjust seconds to time.Duration
	c.ClickHouse.Access.Timeouts.Query = c.ClickHouse.Access.Timeouts.Query * time.Second

}

func (c *OperatorConfig) normalizeSectionClickHouseMetrics() {
	if c.ClickHouse.Metrics.Timeouts.Collect == 0 {
		c.ClickHouse.Metrics.Timeouts.Collect = defaultTimeoutCollect
	}
	// Adjust seconds to time.Duration
	c.ClickHouse.Metrics.Timeouts.Collect = c.ClickHouse.Metrics.Timeouts.Collect * time.Second
}

func (c *OperatorConfig) normalizeSectionLogger() {
	// Logtostderr      string `json:"logtostderr"      yaml:"logtostderr"`
	// Alsologtostderr  string `json:"alsologtostderr"  yaml:"alsologtostderr"`
	// V                string `json:"v"                yaml:"v"`
	// Stderrthreshold  string `json:"stderrthreshold"  yaml:"stderrthreshold"`
	// Vmodule          string `json:"vmodule"          yaml:"vmodule"`
	// Log_backtrace_at string `json:"log_backtrace_at" yaml:"log_backtrace_at"`
}

func (c *OperatorConfig) normalizeSectionReconcileRuntime() {
	if c.Reconcile.Runtime.ThreadsNumber == 0 {
		c.Reconcile.Runtime.ThreadsNumber = defaultReconcileCHIsThreadsNumber
	}
	if c.Reconcile.Runtime.ReconcileCHIsThreadsNumber == 0 {
		c.Reconcile.Runtime.ReconcileCHIsThreadsNumber = defaultReconcileCHIsThreadsNumber
	}
	if c.Reconcile.Runtime.ReconcileShardsThreadsNumber == 0 {
		c.Reconcile.Runtime.ReconcileShardsThreadsNumber = defaultReconcileShardsThreadsNumber
	}
	if c.Reconcile.Runtime.ReconcileShardsMaxConcurrencyPercent == 0 {
		c.Reconcile.Runtime.ReconcileShardsMaxConcurrencyPercent = defaultReconcileShardsMaxConcurrencyPercent
	}

	//reconcileWaitExclude: true
	//reconcileWaitInclude: false
}

func (c *OperatorConfig) normalizeSectionLabel() {
	//config.IncludeIntoPropagationAnnotations
	//config.ExcludeFromPropagationAnnotations
	//config.IncludeIntoPropagationLabels
	//config.ExcludeFromPropagationLabels
	// Whether to append *Scope* labels to StatefulSet and Pod.
	c.Label.Runtime.AppendScope = c.Label.AppendScopeString.Value()
}

func (c *OperatorConfig) normalizeSectionStatefulSet() {
	if c.StatefulSet.RevisionHistoryLimit == 0 {
		c.StatefulSet.RevisionHistoryLimit = defaultRevisionHistoryLimit
	}
}

func (c *OperatorConfig) normalizeSectionPod() {
	if c.Pod.TerminationGracePeriod == 0 {
		c.Pod.TerminationGracePeriod = defaultTerminationGracePeriod
	}
}

// normalize() makes fully-and-correctly filled OperatorConfig
func (c *OperatorConfig) normalize() {
	c.move()
	c.Runtime.Namespace = os.Getenv(OPERATOR_POD_NAMESPACE)

	c.normalizeSectionClickHouseConfigurationFile()
	c.normalizeSectionClickHouseConfigurationUserDefault()
	c.normalizeSectionClickHouseAccess()
	c.normalizeSectionClickHouseMetrics()
	c.normalizeSectionTemplate()
	c.normalizeSectionReconcileStatefulSet()
	c.normalizeSectionReconcileRuntime()
	c.normalizeSectionLogger()
	c.normalizeSectionLabel()
	c.normalizeSectionStatefulSet()
	c.normalizeSectionPod()
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
		if conf.ClickHouse.Config.User.Default.Password != "" {
			conf.ClickHouse.Config.User.Default.Password = PasswordReplacer
		}
		//conf.ClickHouse.Access.Username = UsernameReplacer
		if conf.ClickHouse.Access.Password != "" {
			conf.ClickHouse.Access.Password = PasswordReplacer
		}
		//conf.ClickHouse.Access.Secret.Runtime.Username = UsernameReplacer
		if conf.ClickHouse.Access.Secret.Runtime.Password != "" {
			conf.ClickHouse.Access.Secret.Runtime.Password = PasswordReplacer
		}

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
		var labelRegexp = regexp.MustCompile("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
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
		c.Reconcile.Host.Wait.Exclude = c.Reconcile.Host.Wait.Exclude.From(c.ReconcileWaitExclude)
	}
	if c.ReconcileWaitInclude {
		c.Reconcile.Host.Wait.Include = c.Reconcile.Host.Wait.Include.From(c.ReconcileWaitInclude)
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
