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

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopclientset "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ConfigManager struct {
	chopClient *chopclientset.Clientset
	list       *chiv1.ClickHouseOperatorConfigurationList
}

func NewConfigManager(chopClient *chopclientset.Clientset) *ConfigManager {
	return &ConfigManager{
		chopClient: chopClient,
	}
}

func (cm *ConfigManager) GetChopConfigs(namespace string) {
	var err error
	cm.list, err = cm.chopClient.ClickhouseV1().ClickHouseOperatorConfigurations(namespace).List(metav1.ListOptions{})
	if err == nil {
		for i := range cm.list.Items {
			chOperatorConfiguration := &cm.list.Items[i]
			glog.V(1).Infof("Reading %s/%s config:", chOperatorConfiguration.Namespace, chOperatorConfiguration.Name)
			chOperatorConfiguration.Spec.WriteToLog()
		}
	}
}

func (cm *ConfigManager) IsConfigListed(config *chiv1.ClickHouseOperatorConfiguration) bool {
	for i := range cm.list.Items {
		chOperatorConfiguration := &cm.list.Items[i]

		if config.Namespace == chOperatorConfiguration.Namespace &&
			config.Name == chOperatorConfiguration.Name &&
			config.ResourceVersion == chOperatorConfiguration.ResourceVersion {
			return true
		}
	}

	return false
}

// GetConfig creates Config object based on current environment
func (cm *ConfigManager) GetConfig(configFilePath string) (*chiv1.Config, error) {
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
func buildConfigFromFile(configFilePath string) (*chiv1.Config, error) {
	// Read config file content
	yamlText, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		return nil, err
	}

	// Parse config file content into Config struct
	config := new(chiv1.Config)
	err = yaml.Unmarshal(yamlText, config)
	if err != nil {
		return nil, err
	}

	// Fill Config's paths
	config.ConfigFilePath, err = filepath.Abs(configFilePath)
	config.ConfigFolderPath = filepath.Dir(config.ConfigFilePath)

	// Normalize Config struct into fully-and-correctly filled Config struct
	config.Normalize()
	config.ReadChConfigFiles()
	config.ReadChiTemplateFiles()
	config.ProcessChiTemplateFiles()
	config.ApplyEnvVars()

	return config, nil

}

// buildDefaultConfig returns default Config
func buildDefaultConfig() (*chiv1.Config, error) {
	config := new(chiv1.Config)
	config.Normalize()
	config.ApplyEnvVars()

	return config, nil
}
