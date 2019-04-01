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
	var config Config
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		return nil, err
	}

	// Fill Config/s paths
	config.ConfigFilePath, err = filepath.Abs(configFilePath)
	config.ConfigFolderPath = filepath.Dir(config.ConfigFilePath)

	// Normalize Config struct into fully-and-correctly filled Config struct
	if err = normalizeConfig(&config); err == nil {
		return &config, nil
	} else {
		return nil, err
	}
}

// buildDefaultConfig returns default Config
func buildDefaultConfig() (*Config, error) {
	var config Config
	normalizeConfig(&config)
	return &config, nil
}

// normalizeConfig returns fully-and-correctly filled Config
func normalizeConfig(config *Config) error {
	// Apply default paths in case nothing specified
	if (config.ConfigdPath == "") && (config.ConfigFolderPath != "") {
		// Not specified, try to build it relative to config file
		config.ConfigdPath = config.ConfigFolderPath + "/config.d"
	}
	if (config.ConfdPath == "") && (config.ConfigFolderPath != "") {
		// Not specified, try to build it relative to config file
		config.ConfdPath = config.ConfigFolderPath + "/conf.d"
	}
	if (config.UsersdPath == "") && (config.ConfigFolderPath != "") {
		// Not specified, try to build it relative to config file
		config.UsersdPath = config.ConfigFolderPath + "/users.d"
	}

	// Check whether specified dirs really exist
	if (config.ConfigdPath != "") && !isDirOk(config.ConfigdPath) {
		config.ConfigdPath = ""
	}
	if (config.ConfdPath != "") && !isDirOk(config.ConfdPath) {
		config.ConfdPath = ""
	}
	if (config.UsersdPath != "") && !isDirOk(config.UsersdPath) {
		config.UsersdPath = ""
	}

	// Rolling update section
	if config.StatefulSetUpdateWaitTime <= 0 {
		config.StatefulSetUpdateWaitTime = 3600
	}

	if strings.ToLower(config.OnStatefulSetUpdateFailureAction) == "revert" {
		config.OnStatefulSetUpdateFailureAction = OnStatefulSetUpdateFailureActionRevert
	} else {
		config.OnStatefulSetUpdateFailureAction = OnStatefulSetUpdateFailureActionAbort
	}

	return nil
}

// isDirOk returns whether the given path exists and is a dir
func isDirOk(path string) bool {
	if stat, err := os.Stat(path); (err == nil) && stat.IsDir() {
		// File object Stat-ed without errors - it exists and it is a dir
		return true
	}

	// Some kind of error has happened
	return false
}
