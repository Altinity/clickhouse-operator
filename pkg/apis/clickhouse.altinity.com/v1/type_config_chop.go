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
	"bytes"
	"os"
	"strings"

	log "github.com/golang/glog"
	// log "k8s.io/klog"

	"github.com/imdario/mergo"
	"github.com/kubernetes-sigs/yaml"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/altinity/clickhouse-operator/pkg/util"
)

const (
	// Default values for update timeout and polling period in seconds
	defaultStatefulSetUpdateTimeout    = 300
	defaultStatefulSetUpdatePollPeriod = 15

	// Default values for ClickHouse user configuration
	// 1. user/profile
	// 2. user/quota
	// 3. user/networks/ip
	// 4. user/password
	defaultChConfigUserDefaultProfile    = "default"
	defaultChConfigUserDefaultQuota      = "default"
	defaultChConfigUserDefaultNetworksIP = "::/0"
	defaultChConfigUserDefaultPassword   = "default"

	// Username and Password to be used by operator to connect to ClickHouse instances for
	// 1. Metrics requests
	// 2. Schema maintenance
	// User credentials can be specified in additional ClickHouse config files located in `chUsersConfigsPath` folder
	defaultChUsername = ""
	defaultChPassword = ""
	defaultChPort     = 8123

	// Default number of controller threads running concurrently (used in case no other specified in config)
	defaultReconcileThreadsNumber = 1
)

// !!! IMPORTANT !!!
// !!! IMPORTANT !!!
// !!! IMPORTANT !!!
// !!! IMPORTANT !!!
// !!! IMPORTANT !!!
// Do not forget to update func (config *OperatorConfig) String()
// Do not forget to update CRD spec
type OperatorConfig struct {
	// Full path to the config file and folder where this OperatorConfig originates from
	ConfigFilePath   string
	ConfigFolderPath string

	// WatchNamespaces where operator watches for events
	WatchNamespaces []string `json:"watchNamespaces" yaml:"watchNamespaces"`

	// Paths where to look for additional ClickHouse config .xml files to be mounted into Pod
	CHCommonConfigsPath string `json:"chCommonConfigsPath" yaml:"chCommonConfigsPath"`
	CHHostConfigsPath   string `json:"chHostConfigsPath"   yaml:"chHostConfigsPath"`
	CHUsersConfigsPath  string `json:"chUsersConfigsPath"  yaml:"chUsersConfigsPath"`
	// OperatorConfig files fetched from these paths. Maps "file name->file content"
	CHCommonConfigs map[string]string
	CHHostConfigs   map[string]string
	CHUsersConfigs  map[string]string

	// Path where to look for ClickHouseInstallation templates .yaml files
	CHITemplatesPath string `json:"chiTemplatesPath" yaml:"chiTemplatesPath"`
	// CHI template files fetched from this path. Maps "file name->file content"
	CHITemplateFiles map[string]string
	// CHI template objects unmarshalled from CHITemplateFiles. Maps "metadata.name->object"
	CHITemplates []*ClickHouseInstallation
	// ClickHouseInstallation template
	CHITemplate *ClickHouseInstallation

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
	// Username and Password to be used by operator to connect to ClickHouse instances for
	// 1. Metrics requests
	// 2. Schema maintenance
	// User credentials can be specified in additional ClickHouse config files located in `chUsersConfigsPath` folder
	CHUsername string `json:"chUsername" yaml:"chUsername"`
	CHPassword string `json:"chPassword" yaml:"chPassword"`
	CHPort     int    `json:"chPort"     yaml:"chPort"`

	Logtostderr      string `json:"logtostderr"      yaml:"logtostderr"`
	Alsologtostderr  string `json:"alsologtostderr"  yaml:"alsologtostderr"`
	V                string `json:"v"                yaml:"v"`
	Stderrthreshold  string `json:"stderrthreshold"  yaml:"stderrthreshold"`
	Vmodule          string `json:"vmodule"          yaml:"vmodule"`
	Log_backtrace_at string `json:"log_backtrace_at" yaml:"log_backtrace_at"`

	// Max number of concurrent reconciles in progress
	ReconcileThreadsNumber int `json:"reconcileThreadsNumber" yaml:"reconcileThreadsNumber"`

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
}

// MergeFrom merges
func (config *OperatorConfig) MergeFrom(from *OperatorConfig, _type MergeType) {
	switch _type {
	case MergeTypeFillEmptyValues:
		if err := mergo.Merge(config, *from); err != nil {
			log.V(1).Infof("FAIL merge config Error: %q", err)
		}
	case MergeTypeOverrideByNonEmptyValues:
		if err := mergo.Merge(config, *from, mergo.WithOverride); err != nil {
			log.V(1).Infof("FAIL merge config Error: %q", err)
		}
	}
}

// readCHITemplates build OperatorConfig.CHITemplate from template files content
func (config *OperatorConfig) readCHITemplates() {
	// Read CHI template files
	config.CHITemplateFiles = util.ReadFilesIntoMap(config.CHITemplatesPath, config.isCHITemplateExt)

	// Produce map of CHI templates out of CHI template files
	for filename := range config.CHITemplateFiles {
		template := new(ClickHouseInstallation)
		if err := yaml.Unmarshal([]byte(config.CHITemplateFiles[filename]), template); err != nil {
			// Unable to unmarshal - skip incorrect template
			log.V(1).Infof("FAIL readCHITemplates() unable to unmarshal file %s Error: %q", filename, err)
			continue
		}
		config.enlistCHITemplate(template)
	}
}

// enlistCHITemplate inserts template into templates catalog
func (config *OperatorConfig) enlistCHITemplate(template *ClickHouseInstallation) {
	if config.CHITemplates == nil {
		config.CHITemplates = make([]*ClickHouseInstallation, 0)
	}
	config.CHITemplates = append(config.CHITemplates, template)
	log.V(1).Infof("enlistCHITemplate(%s/%s)", template.Namespace, template.Name)
}

// unlistCHITemplate removes template from templates catalog
func (config *OperatorConfig) unlistCHITemplate(template *ClickHouseInstallation) {
	if config.CHITemplates == nil {
		return
	}

	log.V(1).Infof("unlistCHITemplate(%s/%s)", template.Namespace, template.Name)
	// Nullify found template entry
	for _, _template := range config.CHITemplates {
		if (_template.Name == template.Name) && (_template.Namespace == template.Namespace) {
			log.V(1).Infof("unlistCHITemplate(%s/%s) - found, unlisting", template.Namespace, template.Name)
			// TODO normalize
			//config.CHITemplates[i] = nil
			_template.Name = ""
			_template.Namespace = ""
		}
	}
	// Compact the slice
	// TODO compact the slice
}

// FindTemplate
func (config *OperatorConfig) FindTemplate(use *ChiUseTemplate, namespace string) *ClickHouseInstallation {
	// Try to find direct match
	for _, _template := range config.CHITemplates {
		if _template == nil {
			// Skip
		} else if _template.MatchFullName(use.Namespace, use.Name) {
			// Direct match, found result
			return _template
		}
	}

	// Direct match is not possible.

	if use.Namespace != "" {
		// With fully-specified use template direct (full name) only match is applicable, and it is not possible
		// This is strange situation, however
		log.V(1).Infof("STRANGE FindTemplate(%s/%s) - unexpected position", use.Namespace, use.Name)
		return nil
	}

	// Improvise with use.Namespace

	for _, _template := range config.CHITemplates {
		if _template == nil {
			// Skip
		} else if _template.MatchFullName(namespace, use.Name) {
			// Found template with searched name in specified namespace
			return _template
		}
	}

	return nil
}

// buildUnifiedCHITemplate builds combined CHI Template from templates catalog
func (config *OperatorConfig) buildUnifiedCHITemplate() {

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

// AddCHITemplate
func (config *OperatorConfig) AddCHITemplate(template *ClickHouseInstallation) {
	config.enlistCHITemplate(template)
	config.buildUnifiedCHITemplate()
}

// UpdateCHITemplate
func (config *OperatorConfig) UpdateCHITemplate(template *ClickHouseInstallation) {
	config.enlistCHITemplate(template)
	config.buildUnifiedCHITemplate()
}

// DeleteCHITemplate
func (config *OperatorConfig) DeleteCHITemplate(template *ClickHouseInstallation) {
	config.unlistCHITemplate(template)
	config.buildUnifiedCHITemplate()
}

// Postprocess
func (config *OperatorConfig) Postprocess() {
	config.normalize()
	config.readClickHouseCustomConfigFiles()
	config.readCHITemplates()
	config.buildUnifiedCHITemplate()
	config.applyEnvVarParams()
	config.applyDefaultWatchNamespace()
}

// normalize() makes fully-and-correctly filled OperatorConfig
func (config *OperatorConfig) normalize() {

	// Process ClickHouse configuration files section
	// Apply default paths in case nothing specified
	util.PreparePath(&config.CHCommonConfigsPath, config.ConfigFolderPath, CommonConfigDir)
	util.PreparePath(&config.CHHostConfigsPath, config.ConfigFolderPath, HostConfigDir)
	util.PreparePath(&config.CHUsersConfigsPath, config.ConfigFolderPath, UsersConfigDir)

	// Process ClickHouseInstallation templates section
	util.PreparePath(&config.CHITemplatesPath, config.ConfigFolderPath, TemplatesDir)

	// Process Create/Update section

	// Timeouts
	if config.StatefulSetUpdateTimeout == 0 {
		// Default update timeout in seconds
		config.StatefulSetUpdateTimeout = defaultStatefulSetUpdateTimeout
	}

	if config.StatefulSetUpdatePollPeriod == 0 {
		// Default polling period in seconds
		config.StatefulSetUpdatePollPeriod = defaultStatefulSetUpdatePollPeriod
	}

	// Default action on Create/Update failure - to keep system in previous state

	// Default Create Failure action - delete
	if config.OnStatefulSetCreateFailureAction == "" {
		config.OnStatefulSetCreateFailureAction = OnStatefulSetCreateFailureActionDelete
	}

	// Default Updated Failure action - revert
	if config.OnStatefulSetUpdateFailureAction == "" {
		config.OnStatefulSetUpdateFailureAction = OnStatefulSetUpdateFailureActionRollback
	}

	// Default values for ClickHouse user configuration
	// 1. user/profile
	// 2. user/quota
	// 3. user/networks/ip
	// 4. user/password
	if config.CHConfigUserDefaultProfile == "" {
		config.CHConfigUserDefaultProfile = defaultChConfigUserDefaultProfile
	}
	if config.CHConfigUserDefaultQuota == "" {
		config.CHConfigUserDefaultQuota = defaultChConfigUserDefaultQuota
	}
	if len(config.CHConfigUserDefaultNetworksIP) == 0 {
		config.CHConfigUserDefaultNetworksIP = []string{defaultChConfigUserDefaultNetworksIP}
	}
	if config.CHConfigUserDefaultPassword == "" {
		config.CHConfigUserDefaultPassword = defaultChConfigUserDefaultPassword
	}

	// Username and Password to be used by operator to connect to ClickHouse instances for
	// 1. Metrics requests
	// 2. Schema maintenance
	// User credentials can be specified in additional ClickHouse config files located in `chUsersConfigsPath` folder
	if config.CHUsername == "" {
		config.CHUsername = defaultChUsername
	}
	if config.CHPassword == "" {
		config.CHPassword = defaultChPassword
	}
	if config.CHPort == 0 {
		config.CHPort = defaultChPort
	}

	// Logtostderr      string `json:"logtostderr"      yaml:"logtostderr"`
	// Alsologtostderr  string `json:"alsologtostderr"  yaml:"alsologtostderr"`
	// V                string `json:"v"                yaml:"v"`
	// Stderrthreshold  string `json:"stderrthreshold"  yaml:"stderrthreshold"`
	// Vmodule          string `json:"vmodule"          yaml:"vmodule"`
	// Log_backtrace_at string `json:"log_backtrace_at" yaml:"log_backtrace_at"`

	if config.ReconcileThreadsNumber == 0 {
		config.ReconcileThreadsNumber = defaultReconcileThreadsNumber
	}
}

// applyEnvVarParams applies ENV VARS over config
func (config *OperatorConfig) applyEnvVarParams() {
	if ns := os.Getenv(WATCH_NAMESPACE); len(ns) > 0 {
		// We have WATCH_NAMESPACE explicitly specified
		config.WatchNamespaces = []string{ns}
	}

	if nss := os.Getenv(WATCH_NAMESPACES); len(nss) > 0 {
		// We have WATCH_NAMESPACES explicitly specified
		namespaces := strings.FieldsFunc(nss, func(r rune) bool {
			return r == ':' || r == ','
		})
		config.WatchNamespaces = []string{}
		for i := range namespaces {
			if len(namespaces[i]) > 0 {
				config.WatchNamespaces = append(config.WatchNamespaces, namespaces[i])
			}
		}
	}
}

// applyDefaultWatchNamespace applies default watch namespace in case none specified earlier
func (config *OperatorConfig) applyDefaultWatchNamespace() {
	// In case we have watched namespaces specified, all is fine
	// In case we do not have watched namespaces specified, we need to decide, what namespace to watch.
	// In this case, there are two options:
	// 1. Operator runs in kube-system namespace - assume this is global installation, need to watch ALL namespaces
	// 2. Operator runs in other (non kube-system) namespace - assume this is local installation, watch this namespace only
	// Watch in own namespace only in case no other specified earlier

	if len(config.WatchNamespaces) > 0 {
		// We have namespace(s) specified already
		return
	}

	// No namespaces specified

	namespace := os.Getenv(OPERATOR_POD_NAMESPACE)
	if namespace == "kube-system" {
		// Do nothing, we already have len(config.WatchNamespaces) == 0
	} else {
		// We have WATCH_NAMESPACE specified
		config.WatchNamespaces = []string{namespace}
	}
}

// readClickHouseCustomConfigFiles reads all extra user-specified ClickHouse config files
func (config *OperatorConfig) readClickHouseCustomConfigFiles() {
	log.V(0).Infof("Read Common Config files from folder: %s", config.CHCommonConfigsPath)
	log.V(0).Infof("Read Host Config files from folder: %s", config.CHHostConfigsPath)
	log.V(0).Infof("Read Users Config files from folder: %s", config.CHUsersConfigsPath)

	config.CHCommonConfigs = util.ReadFilesIntoMap(config.CHCommonConfigsPath, config.isCHConfigExt)
	config.CHHostConfigs = util.ReadFilesIntoMap(config.CHHostConfigsPath, config.isCHConfigExt)
	config.CHUsersConfigs = util.ReadFilesIntoMap(config.CHUsersConfigsPath, config.isCHConfigExt)
}

// isCHConfigExt returns true in case specified file has proper extension for a ClickHouse config file
func (config *OperatorConfig) isCHConfigExt(file string) bool {
	switch util.ExtToLower(file) {
	case ".xml":
		return true
	}
	return false
}

// isCHITemplateExt returns true in case specified file has proper extension for a CHI template config file
func (config *OperatorConfig) isCHITemplateExt(file string) bool {
	switch util.ExtToLower(file) {
	case ".yaml":
		return true
	case ".json":
		return true
	}
	return false
}

// String returns string representation of a OperatorConfig
func (config *OperatorConfig) String(hideCredentials bool) string {
	var username string
	var password string

	b := &bytes.Buffer{}

	util.Fprintf(b, "ConfigFilePath: %s\n", config.ConfigFilePath)
	util.Fprintf(b, "ConfigFolderPath: %s\n", config.ConfigFolderPath)

	util.Fprintf(b, "%s", util.Slice2String("WatchNamespaces", config.WatchNamespaces))

	util.Fprintf(b, "CHCommonConfigsPath: %s\n", config.CHCommonConfigsPath)
	util.Fprintf(b, "CHHostConfigsPath: %s\n", config.CHHostConfigsPath)
	util.Fprintf(b, "CHUsersConfigsPath: %s\n", config.CHUsersConfigsPath)

	util.Fprintf(b, "%s", util.Map2String("CHCommonConfigs", config.CHCommonConfigs))
	util.Fprintf(b, "%s", util.Map2String("CHHostConfigs", config.CHHostConfigs))
	util.Fprintf(b, "%s", util.Map2String("CHUsersConfigs", config.CHUsersConfigs))

	util.Fprintf(b, "CHITemplatesPath: %s\n", config.CHITemplatesPath)
	util.Fprintf(b, "%s", util.Map2String("CHITemplateFiles", config.CHITemplateFiles))

	util.Fprintf(b, "StatefulSetUpdateTimeout: %d\n", config.StatefulSetUpdateTimeout)
	util.Fprintf(b, "StatefulSetUpdatePollPeriod: %d\n", config.StatefulSetUpdatePollPeriod)

	util.Fprintf(b, "OnStatefulSetCreateFailureAction: %s\n", config.OnStatefulSetCreateFailureAction)
	util.Fprintf(b, "OnStatefulSetUpdateFailureAction: %s\n", config.OnStatefulSetUpdateFailureAction)

	util.Fprintf(b, "CHConfigUserDefaultProfile: %s\n", config.CHConfigUserDefaultProfile)
	util.Fprintf(b, "CHConfigUserDefaultQuota: %s\n", config.CHConfigUserDefaultQuota)
	util.Fprintf(b, "%s", util.Slice2String("CHConfigUserDefaultNetworksIP", config.CHConfigUserDefaultNetworksIP))
	password = config.CHConfigUserDefaultPassword
	if hideCredentials {
		password = PasswordReplacer
	}
	util.Fprintf(b, "CHConfigUserDefaultPassword: %s\n", password)
	util.Fprintf(b, "CHConfigNetworksHostRegexpTemplate: %s\n", config.CHConfigNetworksHostRegexpTemplate)

	username = config.CHUsername
	password = config.CHPassword
	if hideCredentials {
		username = UsernameReplacer
		password = PasswordReplacer
	}
	util.Fprintf(b, "CHUsername: %s\n", username)
	util.Fprintf(b, "CHPassword: %s\n", password)
	util.Fprintf(b, "CHPort: %d\n", config.CHPort)

	util.Fprintf(b, "Logtostderr: %s\n", config.Logtostderr)
	util.Fprintf(b, "Alsologtostderr: %s\n", config.Alsologtostderr)
	util.Fprintf(b, "V: %s\n", config.V)
	util.Fprintf(b, "Stderrthreshold: %s\n", config.Stderrthreshold)
	util.Fprintf(b, "Vmodule: %s\n", config.Vmodule)
	util.Fprintf(b, "Log_backtrace_at string: %s\n", config.Log_backtrace_at)

	util.Fprintf(b, "ReconcileThreadsNumber: %d\n", config.ReconcileThreadsNumber)

	return b.String()
}

// WriteToLog writes OperatorConfig into log
func (config *OperatorConfig) WriteToLog() {
	log.V(1).Infof("OperatorConfig:\n%s", config.String(true))
}

// TODO unify with GetInformerNamespace
// IsWatchedNamespace returns whether specified namespace is in a list of watched
func (config *OperatorConfig) IsWatchedNamespace(namespace string) bool {
	// In case no namespaces specified - watch all namespaces
	if len(config.WatchNamespaces) == 0 {
		return true
	}

	return util.InArray(namespace, config.WatchNamespaces)
}

// TODO unify with IsWatchedNamespace
// TODO unify approaches to multiple namespaces support
// GetInformerNamespace is a TODO stub
// Namespace where informers would watch notifications from
// The thing is that InformerFactory can accept only one parameter as watched namespace,
// be it explicitly specified namespace or empty line for "all namespaces".
// That's what conflicts with CHOp's approach to 'specify list of namespaces to watch in', having
// slice of namespaces (CHOp's approach) incompatible with "one namespace name" approach
func (config *OperatorConfig) GetInformerNamespace() string {
	// Namespace where informers would watch notifications from
	namespace := metav1.NamespaceAll
	if len(config.WatchNamespaces) == 1 {
		// We have exactly one watch namespace specified
		// This scenario is implemented in go-client
		// In any other case, just keep metav1.NamespaceAll

		// This contradicts current implementation of multiple namespaces in config's watchNamespaces field,
		// but k8s has possibility to specify one/all namespaces only, no 'multiple namespaces' option
		namespace = config.WatchNamespaces[0]
	}

	return namespace
}
