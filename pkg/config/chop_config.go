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
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
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

	// Try to find /etc/clickhouse-oprator/config.yaml
	if conf, err := buildConfigFromFile("/etc/clickhouse-oprator/config.yaml"); err == nil {
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
	config := &Config{}
	err = yaml.Unmarshal(yamlText, config)
	if err != nil {
		return nil, err
	}

	// Fill Config/s paths
	config.ConfigFilePath, err = filepath.Abs(configFilePath)
	config.ConfigFolderPath = filepath.Dir(config.ConfigFilePath)

	// Normalize Config struct into fully-and-correctly filled Config struct
	if err = config.normalize(); err == nil {
		config.readChConfigFiles()
		config.readChiTemplateFiles()
		config.buildChiTemplate()
		return config, nil
	} else {
		return nil, err
	}
}

// buildDefaultConfig returns default Config
func buildDefaultConfig() (*Config, error) {
	config := &Config{}
	config.normalize()
	return config, nil
}

// buildChiTemplate build Config.ChiTemplate from template files content
func (config *Config) buildChiTemplate() {
	for _, yamlText := range config.ChiTemplates {
		chi := &chiv1.ClickHouseInstallation{}
		if err := yaml.Unmarshal([]byte(yamlText), chi); err != nil {
			// Skip incorrect template
			continue
		}
		config.ChiTemplate = chi
	}
}

// normalize() makes fully-and-correctly filled Config
func (config *Config) normalize() error {

	// Process ClickHouse configuration files section
	// Apply default paths in case nothing specified
	config.prepareConfigPath(&config.ChCommonConfigsPath, "config.d")
	config.prepareConfigPath(&config.ChDeploymentConfigsPath, "conf.d")
	config.prepareConfigPath(&config.ChUsersConfigsPath, "users.d")

	// Process ClickHouseInstallation templates section
	config.prepareConfigPath(&config.ChiTemplatesPath, "templates.d")

	// Process Rolling update section
	if config.StatefulSetUpdateTimeout == 0 {
		config.StatefulSetUpdateTimeout = 3600
	}

	if config.StatefulSetUpdatePollPeriod == 0 {
		config.StatefulSetUpdatePollPeriod = 1
	}

	if strings.ToLower(config.OnStatefulSetUpdateFailureAction) == "revert" {
		config.OnStatefulSetUpdateFailureAction = OnStatefulSetUpdateFailureActionRevert
	} else {
		config.OnStatefulSetUpdateFailureAction = OnStatefulSetUpdateFailureActionAbort
	}

	return nil
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
	if (*path != "") && !isDirOk(*path) {
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
	config.ChDeploymentConfigs = readConfigFiles(config.ChDeploymentConfigsPath, config.isChConfigExt)
	config.ChUsersConfigs = readConfigFiles(config.ChUsersConfigsPath, config.isChConfigExt)
}

// isChConfigExt return true in case specified file has proper extension for a ClickHouse config file
func (config *Config) isChConfigExt(file string) bool {
	switch extToLower(file) {
	case ".xml":
		return true
	}
	return false
}

// readChConfigFiles reads all CHI templates
func (config *Config) readChiTemplateFiles() {
	config.ChiTemplates = readConfigFiles(config.ChiTemplatesPath, config.isChiTemplateExt)
}

// isChiTemplateExt return true in case specified file has proper extension for a CHI template config file
func (config *Config) isChiTemplateExt(file string) bool {
	switch extToLower(file) {
	case ".yaml":
		return true
	}
	return false
}

// IsWatchedNamespace returns is specified namespace in a list of watched
func (config *Config) IsWatchedNamespace(namespace string) bool {
	// In case no namespaces specified - watch all namespaces
	if len(config.Namespaces) == 0 {
		return true
	}

	return inArray(namespace, config.Namespaces)
}
