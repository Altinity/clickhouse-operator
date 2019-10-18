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
	"github.com/altinity/clickhouse-operator/pkg/util"
	"github.com/golang/glog"
	"github.com/kubernetes-sigs/yaml"
	"os"
	"path/filepath"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
)

// readChiTemplates build Config.ChiTemplate from template files content
func (config *Config) readChiTemplates() {
	// Read CHI template files
	config.ChiTemplateFiles = readConfigFiles(config.ChiTemplatesPath, config.isChiTemplateExt)

	// Produce map of CHI templates out of CHI template files
	for filename := range config.ChiTemplateFiles {
		template := new(ClickHouseInstallation)
		if err := yaml.Unmarshal([]byte(config.ChiTemplateFiles[filename]), template); err != nil {
			// Unable to unmarshal - skip incorrect template
			glog.V(1).Infof("FAIL readChiTemplates() unable to unmarshal file %s Error: %q", filename, err)
			continue
		}
		config.enlistChiTemplate(template)
	}
}

// enlistChiTemplate inserts template into templates catalog
func (config *Config) enlistChiTemplate(template *ClickHouseInstallation) {
	// Insert template
	if config.ChiTemplates == nil {
		config.ChiTemplates = make(map[string]*ClickHouseInstallation)
	}
	// map template name -> template itself
	config.ChiTemplates[template.Name] = template
	glog.V(1).Infof("enlistChiTemplate(%s/%s)", template.Namespace, template.Name)
}

// unlistChiTemplate removes template from templates catalog
func (config *Config) unlistChiTemplate(template *ClickHouseInstallation) {
	// Insert template
	if config.ChiTemplates == nil {
		return
	}
	if _, ok := config.ChiTemplates[template.Name]; ok {
		glog.V(1).Infof("unlistChiTemplate(%s/%s)", template.Namespace, template.Name)
		delete(config.ChiTemplates, template.Name)
	}
}

// buildUnifiedChiTemplate builds combined CHI Template from templates catalog
func (config *Config) buildUnifiedChiTemplate() {
	// Build unified template in case there are templates to build from only
	if len(config.ChiTemplates) == 0 {
		return
	}

	// Sort CHI templates by their names and in order to apply orderly
	// Extract file names into slice and sort it
	// Then we'll loop over templates in sorted order (by filenames) and apply them one-by-one
	var sortedTemplateNames []string
	for name := range config.ChiTemplates {
		sortedTemplateNames = append(sortedTemplateNames, name)
	}
	sort.Strings(sortedTemplateNames)

	// Create final combined template
	config.ChiTemplate = new(ClickHouseInstallation)

	// Extract templates in sorted order - according to sorted template names
	for _, templateName := range sortedTemplateNames {
		// Merge into accumulated target template from current template
		config.ChiTemplate.MergeFrom(config.ChiTemplates[templateName])
	}

	// Log final CHI template obtained
	// Marshaling is done just to print nice yaml
	if bytes, err := yaml.Marshal(config.ChiTemplate); err == nil {
		glog.V(1).Infof("Unified ChiTemplate:\n%s\n", string(bytes))
	} else {
		glog.V(1).Infof("FAIL unable to Marshal Unified ChiTemplate")
	}
}

func (config *Config) AddChiTemplate(template *ClickHouseInstallation) {
	config.enlistChiTemplate(template)
	config.buildUnifiedChiTemplate()
}

func (config *Config) UpdateChiTemplate(template *ClickHouseInstallation) {
	config.enlistChiTemplate(template)
	config.buildUnifiedChiTemplate()
}

func (config *Config) DeleteChiTemplate(template *ClickHouseInstallation) {
	config.unlistChiTemplate(template)
	config.buildUnifiedChiTemplate()
}

func (config *Config) Postprocess() {
	config.normalize()
	config.readClickHouseCustomConfigFiles()
	config.readChiTemplates()
	config.buildUnifiedChiTemplate()
	config.applyEnvVarParams()
	config.applyDefaultWatchNamespace()
}

// normalize() makes fully-and-correctly filled Config
func (config *Config) normalize() {

	// Process ClickHouse configuration files section
	// Apply default paths in case nothing specified
	config.prepareConfigPath(&config.ChCommonConfigsPath, "config.d")
	config.prepareConfigPath(&config.ChHostConfigsPath, "conf.d")
	config.prepareConfigPath(&config.ChUsersConfigsPath, "users.d")

	// Process ClickHouseInstallation templates section
	config.prepareConfigPath(&config.ChiTemplatesPath, "templates.d")

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
	if strings.ToLower(config.OnStatefulSetCreateFailureAction) == OnStatefulSetCreateFailureActionAbort {
		config.OnStatefulSetCreateFailureAction = OnStatefulSetCreateFailureActionAbort
	} else {
		config.OnStatefulSetCreateFailureAction = OnStatefulSetCreateFailureActionDelete
	}

	// Default Updated Failure action - revert
	if strings.ToLower(config.OnStatefulSetUpdateFailureAction) == OnStatefulSetUpdateFailureActionAbort {
		config.OnStatefulSetUpdateFailureAction = OnStatefulSetUpdateFailureActionAbort
	} else {
		config.OnStatefulSetUpdateFailureAction = OnStatefulSetUpdateFailureActionRollback
	}

	// Default values for ClickHouse user configuration
	// 1. user/profile
	// 2. user/quota
	// 3. user/networks/ip
	// 4. user/password
	if config.ChConfigUserDefaultProfile == "" {
		config.ChConfigUserDefaultProfile = defaultChConfigUserDefaultProfile
	}
	if config.ChConfigUserDefaultQuota == "" {
		config.ChConfigUserDefaultQuota = defaultChConfigUserDefaultQuota
	}
	if len(config.ChConfigUserDefaultNetworksIP) == 0 {
		config.ChConfigUserDefaultNetworksIP = []string{defaultChConfigUserDefaultNetworksIP}
	}
	if config.ChConfigUserDefaultPassword == "" {
		config.ChConfigUserDefaultPassword = defaultChConfigUserDefaultPassword
	}

	// Username and Password to be used by operator to connect to ClickHouse instances for
	// 1. Metrics requests
	// 2. Schema maintenance
	// User credentials can be specified in additional ClickHouse config files located in `chUsersConfigsPath` folder
	if config.ChUsername == "" {
		config.ChUsername = defaultChUsername
	}
	if config.ChPassword == "" {
		config.ChPassword = defaultChPassword
	}
	if config.ChPort == 0 {
		config.ChPort = defaultChPort
	}
}

// applyEnvVarParams applies ENV VARS over config
func (config *Config) applyEnvVarParams() {
	if ns := os.Getenv("WATCH_NAMESPACE"); len(ns) > 0 {
		// We have WATCH_NAMESPACE explicitly specified
		config.WatchNamespaces = []string{ns}
	}
	if nss := os.Getenv("WATCH_NAMESPACES"); len(nss) > 0 {
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
func (config *Config) applyDefaultWatchNamespace() {
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

	ns := os.Getenv("OPERATOR_POD_NAMESPACE")
	if ns == "kube-system" {
		// Do nothing, we already have len(config.WatchNamespaces) == 0
	} else {
		// We have WATCH_NAMESPACE specified
		config.WatchNamespaces = []string{ns}
	}
}

// prepareConfigPath - prepares config path absolute/relative with default relative value
func (config *Config) prepareConfigPath(path *string, defaultRelativePath string) {
	if *path == "" {
		// Path not specified, try to build it relative to config file
		*path = config.relativeToConfigFolderPath(defaultRelativePath)
	} else if filepath.IsAbs(*path) {
		// Absolute path explicitly specified - nothing to do here
	} else {
		// Relative path specified - make relative path relative to config file itself
		*path = config.relativeToConfigFolderPath(*path)
	}

	// In case of incorrect/unavailable path - make it empty
	if (*path != "") && !util.IsDirOk(*path) {
		*path = ""
	}
}

// relativeToConfigFolderPath returns absolute path relative to ConfigFolderPath
func (config *Config) relativeToConfigFolderPath(relativePath string) string {
	if config.ConfigFolderPath == "" {
		// Relative base is not set, do nothing
		return relativePath
	}

	// Relative base is set - try to be ralative to it
	if absPath, err := filepath.Abs(config.ConfigFolderPath + "/" + relativePath); err == nil {
		return absPath
	} else {
		return ""
	}
}

// readClickHouseCustomConfigFiles reads all extra user-specified ClickHouse config files
func (config *Config) readClickHouseCustomConfigFiles() {
	config.ChCommonConfigs = readConfigFiles(config.ChCommonConfigsPath, config.isChConfigExt)
	config.ChHostConfigs = readConfigFiles(config.ChHostConfigsPath, config.isChConfigExt)
	config.ChUsersConfigs = readConfigFiles(config.ChUsersConfigsPath, config.isChConfigExt)
}

// isChConfigExt returns true in case specified file has proper extension for a ClickHouse config file
func (config *Config) isChConfigExt(file string) bool {
	switch util.ExtToLower(file) {
	case ".xml":
		return true
	}
	return false
}

// isChiTemplateExt returns true in case specified file has proper extension for a CHI template config file
func (config *Config) isChiTemplateExt(file string) bool {
	switch util.ExtToLower(file) {
	case ".yaml":
		return true
	case ".json":
		return true
	}
	return false
}

// IsWatchedNamespace returns whether specified namespace is in a list of watched
func (config *Config) IsWatchedNamespace(namespace string) bool {
	// In case no namespaces specified - watch all namespaces
	if len(config.WatchNamespaces) == 0 {
		return true
	}

	return util.InArray(namespace, config.WatchNamespaces)
}

// String returns string representation of a Config
func (config *Config) String() string {
	b := &bytes.Buffer{}

	util.Fprintf(b, "ConfigFilePath: %s\n", config.ConfigFilePath)
	util.Fprintf(b, "ConfigFolderPath: %s\n", config.ConfigFolderPath)

	util.Fprintf(b, "%s", config.stringSlice("WatchNamespaces", config.WatchNamespaces))

	util.Fprintf(b, "ChCommonConfigsPath: %s\n", config.ChCommonConfigsPath)
	util.Fprintf(b, "ChHostConfigsPath: %s\n", config.ChHostConfigsPath)
	util.Fprintf(b, "ChUsersConfigsPath: %s\n", config.ChUsersConfigsPath)

	util.Fprintf(b, "%s", config.stringMap("ChCommonConfigs", config.ChCommonConfigs))
	util.Fprintf(b, "%s", config.stringMap("ChHostConfigs", config.ChHostConfigs))
	util.Fprintf(b, "%s", config.stringMap("ChUsersConfigs", config.ChUsersConfigs))

	util.Fprintf(b, "ChiTemplatesPath: %s\n", config.ChiTemplatesPath)
	util.Fprintf(b, "%s", config.stringMap("ChiTemplateFiles", config.ChiTemplateFiles))

	util.Fprintf(b, "StatefulSetUpdateTimeout: %d\n", config.StatefulSetUpdateTimeout)
	util.Fprintf(b, "StatefulSetUpdatePollPeriod: %d\n", config.StatefulSetUpdatePollPeriod)

	util.Fprintf(b, "OnStatefulSetCreateFailureAction: %s\n", config.OnStatefulSetCreateFailureAction)
	util.Fprintf(b, "OnStatefulSetUpdateFailureAction: %s\n", config.OnStatefulSetUpdateFailureAction)

	util.Fprintf(b, "ChConfigUserDefaultProfile: %s\n", config.ChConfigUserDefaultProfile)
	util.Fprintf(b, "ChConfigUserDefaultQuota: %s\n", config.ChConfigUserDefaultQuota)
	util.Fprintf(b, "%s", config.stringSlice("ChConfigUserDefaultNetworksIP", config.ChConfigUserDefaultNetworksIP))
	util.Fprintf(b, "ChConfigUserDefaultPassword: %s\n", config.ChConfigUserDefaultPassword)

	util.Fprintf(b, "ChUsername: %s\n", config.ChUsername)
	util.Fprintf(b, "ChPassword: %s\n", config.ChPassword)
	util.Fprintf(b, "ChPort: %d\n", config.ChPort)

	return b.String()
}

// WriteToLog writes Config into log
func (config *Config) WriteToLog() {
	glog.V(1).Infof("Config:\n%s", config.String())
}

// stringSlice returns string of named []string Config param
func (config *Config) stringSlice(name string, sl []string) string {
	b := &bytes.Buffer{}
	util.Fprintf(b, "%s (%d):\n", name, len(sl))
	for i := range sl {
		util.Fprintf(b, "  - %s\n", sl[i])
	}

	return b.String()
}

// stringMap returns string of named map[string]string Config param
func (config *Config) stringMap(name string, m map[string]string) string {
	// Write params according to sorted names
	// So we need to
	// 1. Extract and sort names aka keys
	// 2. Walk over keys and log params
	// Sort names aka keys
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Walk over sorted names aka keys
	b := &bytes.Buffer{}
	util.Fprintf(b, "%s (%d):\n", name, len(m))
	for _, k := range keys {
		util.Fprintf(b, "  - [%s]=%s\n", k, m[k])
	}

	return b.String()
}

// GetInformerNamespace is a TODO stub
// Namespace where informers would watch notifications from
func (config *Config) GetInformerNamespace() string {
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

// readConfigFiles reads config files from specified path into "file name->file content" map
// path - folder where to look for files
// isChConfigExt - accepts path to file return bool whether this file has config extension
func readConfigFiles(path string, isConfigExt func(string) bool) map[string]string {
	return util.ReadFilesIntoMap(path, isConfigExt)
}
