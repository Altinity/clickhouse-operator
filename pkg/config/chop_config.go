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
	"github.com/golang/glog"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strings"
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
	yamlFile, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		return nil, err
	}

	// Parse config file content into Config struct
	config := &Config{}
	err = yaml.Unmarshal(yamlFile, config)
	if err != nil {
		return nil, err
	}

	// Fill Config/s paths
	config.ConfigFilePath, err = filepath.Abs(configFilePath)
	config.ConfigFolderPath = filepath.Dir(config.ConfigFilePath)

	// Normalize Config struct into fully-and-correctly filled Config struct
	if err = config.normalize(); err == nil {
		config.readExtraConfigFiles()
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

// normalize() makes fully-and-correctly filled Config
func (config *Config) normalize() error {

	// Apply default paths in case nothing specified
	config.prepareConfigPath(&config.CommonConfigsPath, "config.d")
	config.prepareConfigPath(&config.DeploymentConfigsPath, "conf.d")
	config.prepareConfigPath(&config.UsersConfigsPath, "users.d")

	// Rolling update section
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

// readExtraConfigFiles reads all extra user-specified ClickHouse config files
func (config *Config) readExtraConfigFiles() {
	config.CommonConfigs = config.readConfigFiles(config.CommonConfigsPath)
	config.DeploymentConfigs = config.readConfigFiles(config.DeploymentConfigsPath)
	config.UsersConfigs = config.readConfigFiles(config.UsersConfigsPath)
}

// readConfigFiles reads config files from specified path into filename->content map
func (config *Config) readConfigFiles(path string) map[string]string {
	// Look in real path only
	if path == "" {
		return nil
	}

	// Result is a filename to content map
	var files map[string]string
	// Loop over all files in folder
	if matches, err := filepath.Glob(path + "/*"); err == nil {
		for i := range matches {
			// `file` comes with `path`-prefixed.
			// So in case `path` is an absolute path, `file` will be absolute path to file
			file := matches[i]
			if config.isConfigExt(file) {
				// Pick files with propoer extensions only
				glog.Infof("CommonConfig file %s\n", file)
				if content, err := ioutil.ReadFile(file); err == nil {
					if files == nil {
						files = make(map[string]string)
					}
					files[filepath.Base(file)] = string(content)
				}
			}
		}
	}

	if len(files) > 0 {
		return files
	} else {
		return nil
	}
}

// isConfigExt return true in case specified file has proper extension for a config file
func (config *Config) isConfigExt(file string) bool {
	switch extToLower(file) {
	case ".xml":
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
