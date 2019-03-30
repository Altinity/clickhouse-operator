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

// getConfig creates kuberest.Config object based on current environment
func GetConfig(configFilePath string) (*Config, error) {
	if len(configFilePath) > 0 {
		// config file specified as CLI flag
		return buildConfigFromFile(configFilePath)
	}

	if len(os.Getenv("CHOPCONFIG")) > 0 {
		// kube config file specified as ENV var
		return buildConfigFromFile(os.Getenv("CHOPCONFIG"))
	}

	usr, err := user.Current()
	if err == nil {
		// OS user found. Parse ~/.clickhouse-operator/config.yaml file
		conf, err := buildConfigFromFile(filepath.Join(usr.HomeDir, ".clickhouse-operator", "config.yaml"))
		if err == nil {
			// Able to build config, all is fine
			return conf, nil
		}
	}

	return buildConfigFromFile("/etc/clickhouse-oprator/config.yaml")
}

func buildConfigFromFile(configFilePath string) (*Config, error) {
	yamlFile, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		return nil, err
	}

	var config Config
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		return nil, err
	}

	if config.ConfigdPath == "" {

	}

	if config.ConfdPath == "" {

	}

	if config.UsersdPath == "" {

	}

	if config.StatefulSetUpdateWaitTime <= 0 {
		config.StatefulSetUpdateWaitTime = 3600
	}

	if strings.ToLower(config.OnStatefulSetUpdateFailureAction) == "revert" {
		config.OnStatefulSetUpdateFailureAction = OnStatefulSetUpdateFailureActionRevert
	} else {
		config.OnStatefulSetUpdateFailureAction = OnStatefulSetUpdateFailureActionAbort
	}
	
	return &config, nil
}
