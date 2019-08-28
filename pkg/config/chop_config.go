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

package config

import (
	"bytes"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strings"

	"github.com/golang/glog"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
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
)

// GetConfig creates Config object based on current environment
func GetConfig(configFilePath string) (*Config, error) {
	if len(configFilePath) > 0 {
		// Config file explicitly specified as CLI flag
		if conf, err := buildConfigFromFile(configFilePath); err == nil {
			return conf, nil
		} else {
			return nil, err
		}
	}

	if len(os.Getenv("CHOP_CONFIG")) > 0 {
		// Config file explicitly specified as ENV var
		if conf, err := buildConfigFromFile(os.Getenv("CHOP_CONFIG")); err == nil {
			return conf, nil
		} else {
			return nil, err
		}
	}

	// Try to find ~/.clickhouse-operator/config.yaml
	usr, err := user.Current()
	if err == nil {
		// OS user found. Parse ~/.clickhouse-operator/config.yaml file
		if conf, err := buildConfigFromFile(filepath.Join(usr.HomeDir, ".clickhouse-operator", "config.yaml")); err == nil {
			// Able to build config, all is fine
			return conf, nil
		}
	}

	// Try to find /etc/clickhouse-operator/config.yaml
	if conf, err := buildConfigFromFile("/etc/clickhouse-operator/config.yaml"); err == nil {
		// Able to build config, all is fine
		return conf, nil
	}

	// No config file found, use default one
	return buildDefaultConfig()
}

// buildConfigFromFile returns Config struct built out of specified file path
func buildConfigFromFile(configFilePath string) (*Config, error) {
	// Read config file content
	yamlText, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		return nil, err
	}

	// Parse config file content into Config struct
	config := new(Config)
	err = yaml.Unmarshal(yamlText, config)
	if err != nil {
		return nil, err
	}

	// Fill Config's paths
	config.ConfigFilePath, err = filepath.Abs(configFilePath)
	config.ConfigFolderPath = filepath.Dir(config.ConfigFilePath)

	// Normalize Config struct into fully-and-correctly filled Config struct
	config.normalize()
	config.readChConfigFiles()
	config.readChiTemplateFiles()
	config.processChiTemplateFiles()
	config.applyEnvVars()

	return config, nil

}

// buildDefaultConfig returns default Config
func buildDefaultConfig() (*Config, error) {
	config := new(Config)
	config.normalize()
	config.applyEnvVars()

	return config, nil
}

// processChiTemplateFiles build Config.ChiTemplate from template files content
func (config *Config) processChiTemplateFiles() {

	// Produce map of CHI templates out of CHI template files
	for filename := range config.ChiTemplateFiles {
		template := new(chiv1.ClickHouseInstallation)
		if err := yaml.Unmarshal([]byte(config.ChiTemplateFiles[filename]), template); err != nil {
			// Unable to unmarshal - skip incorrect template
			glog.V(1).Infof("FAIL processChiTemplateFiles() unable to unmarshal file %s %q", filename, err)
			continue
		}
		config.insertChiTemplate(template)
	}

	config.buildChiTemplate()
}

// insertChiTemplate inserts template into templates catalog
func (config *Config) insertChiTemplate(template *chiv1.ClickHouseInstallation) {
	// Insert template
	if config.ChiTemplates == nil {
		config.ChiTemplates = make(map[string]*chiv1.ClickHouseInstallation)
	}
	// map template name -> template itself
	config.ChiTemplates[template.Name] = template
}

// removeChiTemplate removes template from templates catalog
func (config *Config) removeChiTemplate(template *chiv1.ClickHouseInstallation) {
	// Insert template
	if config.ChiTemplates == nil {
		return
	}
	if _, ok := config.ChiTemplates[template.Name]; ok {
		delete(config.ChiTemplates, template.Name)
	}
}

// buildChiTemplate builds combined CHI Template from templates catalog
func (config *Config) buildChiTemplate() {
	// Sort CHI templates by their names and apply one by one
	// Extract file names into slice and sort it
	// Then we'll loop over templates in sorted order (by filenames) and apply them one-by-one
	var sortedTemplateNames []string
	for name := range config.ChiTemplates {
		sortedTemplateNames = append(sortedTemplateNames, name)
	}
	sort.Strings(sortedTemplateNames)

	// Create final combined template
	config.ChiTemplate = new(chiv1.ClickHouseInstallation)

	// Extract templates in sorted order - according to sorted template names
	for _, templateName := range sortedTemplateNames {
		// Merge into accumulated target template from current template
		config.ChiTemplate.MergeFrom(config.ChiTemplates[templateName])
	}

	if bytes, err := yaml.Marshal(config.ChiTemplate); err == nil {
		glog.V(1).Infof("ChiTemplate:\n%s\n", string(bytes))
	} else {
		glog.V(1).Infof("FAIL unable to Marshal ChiTemplate")
	}

}

func (config *Config) AddChiTemplate(template *chiv1.ClickHouseInstallation) {
	config.insertChiTemplate(template)
	config.buildChiTemplate()
}

func (config *Config) UpdateChiTemplate(template *chiv1.ClickHouseInstallation) {
	config.insertChiTemplate(template)
	config.buildChiTemplate()
}

func (config *Config) DeleteChiTemplate(template *chiv1.ClickHouseInstallation) {
	config.removeChiTemplate(template)
	config.buildChiTemplate()
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

// applyEnvVars applies ENV VARS over config
func (config *Config) applyEnvVars() {
	if ns := os.Getenv("WATCH_NAMESPACE"); len(ns) > 0 {
		// We have WATCH_NAMESPACE specified
		config.WatchNamespaces = []string{ns}
	}
	if nss := os.Getenv("WATCH_NAMESPACES"); len(nss) > 0 {
		// We have WATCH_NAMESPACES specified
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

// prepareConfigPath - prepares config path absolute/relative with default relative value
func (config *Config) prepareConfigPath(path *string, defaultRelativePath string) {
	if *path == "" {
		// Path not specified, try to build it relative to config file
		*path = config.relativeToConfigFolderPath(defaultRelativePath)
	} else if filepath.IsAbs(*path) {
		// Absolute path already specified - nothing to do here
	} else {
		// Relative path - make it relative to config file itself
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

// readChConfigFiles reads all extra user-specified ClickHouse config files
func (config *Config) readChConfigFiles() {
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

// readChConfigFiles reads all CHI templates
func (config *Config) readChiTemplateFiles() {
	config.ChiTemplateFiles = readConfigFiles(config.ChiTemplatesPath, config.isChiTemplateExt)
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

// readConfigFiles reads config files from specified path into "file name->file content" map
// path - folder where to look for files
// isChConfigExt - accepts path to file return bool whether this file has config extension
func readConfigFiles(path string, isConfigExt func(string) bool) map[string]string {
	return util.ReadFilesIntoMap(path, isConfigExt)
}
