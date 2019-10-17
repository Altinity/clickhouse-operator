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
	"github.com/kubernetes-sigs/yaml"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"sort"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopclientset "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Manager struct {
	chopClient     *chopclientset.Clientset
	chopConfigList *chiv1.ClickHouseOperatorConfigurationList

	initConfigFilePath string

	// fileConfig is a file-based config
	fileConfig *chiv1.Config

	// crConfigs is a slice of Custom Resource based configs
	crConfigs []*chiv1.Config

	// config is the final, unified config
	config *chiv1.Config

	runtimeParams map[string]string
}

// NewConfigManager creates new Manager
func NewConfigManager(
	chopClient *chopclientset.Clientset,
	initConfigFilePath string,
) *Manager {
	return &Manager{
		chopClient:         chopClient,
		initConfigFilePath: initConfigFilePath,
	}
}

// Init reads config from all sources
func (cm *Manager) Init() error {
	var err error

	// Get ENV vars
	cm.runtimeParams = cm.getEnvVarParams()
	cm.logEnvVarParams()

	// Get initial config from file
	cm.fileConfig, err = cm.getFileBasedConfig(cm.initConfigFilePath)
	if err != nil {
		return err
	}
	glog.V(1).Info("File-based ClickHouseOperatorConfigurations")
	cm.fileConfig.WriteToLog()

	// Read config all Custom Resources
	watchedNamespace := cm.fileConfig.GetInformerNamespace()
	cm.getCRBasedConfigs(watchedNamespace)
	cm.logCRBasedConfigs()

	// Prepare one unified config
	cm.buildUnifiedConfig()
	// From now on we have one unified CHOP config
	glog.V(1).Info("Unified (but not post-processed yet) CHOP config")
	cm.config.WriteToLog()

	// Finalize config by post-processing
	cm.config.Postprocess()

	// Config is ready
	glog.V(1).Info("Final CHOP config")
	cm.config.WriteToLog()

	return nil
}

// Config is an access wrapper
func (cm *Manager) Config() *chiv1.Config {
	return cm.config
}

// getCRBasedConfigs reads all ClickHouseOperatorConfiguration objects in specified namespace
func (cm *Manager) getCRBasedConfigs(namespace string) {
	if cm.chopClient == nil {
		return
	}

	var err error
	if cm.chopConfigList, err = cm.chopClient.ClickhouseV1().ClickHouseOperatorConfigurations(namespace).List(metav1.ListOptions{}); err != nil {
		glog.V(1).Infof("Error read ClickHouseOperatorConfigurations %v", err)
		return
	}

	if cm.chopConfigList == nil {
		return
	}

	// Get sorted names of ClickHouseOperatorConfiguration object
	var names []string
	for i := range cm.chopConfigList.Items {
		chOperatorConfiguration := &cm.chopConfigList.Items[i]
		names = append(names, chOperatorConfiguration.Name)
	}
	sort.Strings(names)

	// Build sorted slice of configs
	for _, name := range names {
		for i := range cm.chopConfigList.Items {
			chOperatorConfiguration := &cm.chopConfigList.Items[i]
			if chOperatorConfiguration.Name == name {

				// Save location info into Config itself
				chOperatorConfiguration.Spec.ConfigFolderPath = namespace
				chOperatorConfiguration.Spec.ConfigFilePath = name

				cm.crConfigs = append(cm.crConfigs, &chOperatorConfiguration.Spec)
				continue
			}
		}
	}
}

// logCRBasedConfigs writes all ClickHouseOperatorConfiguration objects into log
func (cm *Manager) logCRBasedConfigs() {
	for _, chOperatorConfiguration := range cm.crConfigs {
		glog.V(1).Infof("chop config %s/%s :", chOperatorConfiguration.ConfigFolderPath, chOperatorConfiguration.ConfigFilePath)
		chOperatorConfiguration.WriteToLog()
	}
}

// buildUnifiedConfig prepares one config from all accumulated parts
func (cm *Manager) buildUnifiedConfig() {
	// TODO need to either
	// 1. mix
	// 2. overwrite
	// 3. skip

	// Start with file config
	cm.config = cm.fileConfig
	// Merge/unify with CR-based configs
	for _, chOperatorConfiguration := range cm.crConfigs {
		glog.V(1).Infof("chop config %s/%s :", chOperatorConfiguration.ConfigFolderPath, chOperatorConfiguration.ConfigFilePath)
		cm.config = chOperatorConfiguration
	}
}

// IsConfigListed checks whether specified ClickHouseOperatorConfiguration is listed in list of ClickHouseOperatorConfiguration(s)
func (cm *Manager) IsConfigListed(config *chiv1.ClickHouseOperatorConfiguration) bool {
	for i := range cm.chopConfigList.Items {
		chOperatorConfiguration := &cm.chopConfigList.Items[i]

		if config.Namespace == chOperatorConfiguration.Namespace &&
			config.Name == chOperatorConfiguration.Name &&
			config.ResourceVersion == chOperatorConfiguration.ResourceVersion {
			return true
		}
	}

	return false
}

// GetConfig creates Config object based on current environment
func (cm *Manager) getFileBasedConfig(configFilePath string) (*chiv1.Config, error) {
	// In case we have config file specified - that's it
	if len(configFilePath) > 0 {
		// Config file explicitly specified as CLI flag
		if conf, err := cm.buildConfigFromFile(configFilePath); err == nil {
			return conf, nil
		} else {
			return nil, err
		}
	}

	// No file specified - look for ENV var config file path specification
	if len(os.Getenv("CHOP_CONFIG")) > 0 {
		// Config file explicitly specified as ENV var
		if conf, err := cm.buildConfigFromFile(os.Getenv("CHOP_CONFIG")); err == nil {
			return conf, nil
		} else {
			return nil, err
		}
	}

	// No ENV var specified - look into user's homedir
	// Try to find ~/.clickhouse-operator/config.yaml
	usr, err := user.Current()
	if err == nil {
		// OS user found. Parse ~/.clickhouse-operator/config.yaml file
		if conf, err := cm.buildConfigFromFile(filepath.Join(usr.HomeDir, ".clickhouse-operator", "config.yaml")); err == nil {
			// Able to build config, all is fine
			return conf, nil
		}
	}

	// No config file in user's homedir - look for global config in /etc/
	// Try to find /etc/clickhouse-operator/config.yaml
	if conf, err := cm.buildConfigFromFile("/etc/clickhouse-operator/config.yaml"); err == nil {
		// Able to build config, all is fine
		return conf, nil
	}

	// No config file found, use default one
	return cm.buildDefaultConfig()
}

// buildConfigFromFile returns Config struct built out of specified file path
func (cm *Manager) buildConfigFromFile(configFilePath string) (*chiv1.Config, error) {
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

	return config, nil

}

// buildDefaultConfig returns default Config
func (cm *Manager) buildDefaultConfig() (*chiv1.Config, error) {
	config := new(chiv1.Config)

	return config, nil
}

// getEnvVarParamNames return list of ENV VARS parameter names
func (cm *Manager) getEnvVarParamNames() []string {
	// This list of ENV VARS is specified in operator .yaml manifest, section "kind: Deployment"
	return []string{
		// spec.nodeName: ip-172-20-52-62.ec2.internal
		"OPERATOR_POD_NODE_NAME",
		// metadata.name: clickhouse-operator-6f87589dbb-ftcsf
		"OPERATOR_POD_NAME",
		// metadata.namespace: kube-system
		"OPERATOR_POD_NAMESPACE",
		// status.podIP: 100.96.3.2
		"OPERATOR_POD_IP",
		// spec.serviceAccount: clickhouse-operator
		// spec.serviceAccountName: clickhouse-operator
		"OPERATOR_POD_SERVICE_ACCOUNT",

		// .containers.resources.requests.cpu
		"OPERATOR_CONTAINER_CPU_REQUEST",
		// .containers.resources.limits.cpu
		"OPERATOR_CONTAINER_CPU_LIMIT",
		// .containers.resources.requests.memory
		"OPERATOR_CONTAINER_MEM_REQUEST",
		// .containers.resources.limits.memory
		"OPERATOR_CONTAINER_MEM_LIMIT",

		// What namespaces to watch
		"WATCH_NAMESPACE",
		"WATCH_NAMESPACES",
	}
}

// getEnvVarParams returns map[string]string of ENV VARS with some runtime parameters
func (cm *Manager) getEnvVarParams() map[string]string {
	params := make(map[string]string)
	// Extract parameters from ENV VARS
	for _, varName := range cm.getEnvVarParamNames() {
		params[varName] = os.Getenv(varName)
	}

	return params
}

// logEnvVarParams writes runtime parameters into log
func (cm *Manager) logEnvVarParams() {
	// Log params according to sorted names
	// So we need to
	// 1. Extract and sort names aka keys
	// 2. Walk over keys and log params

	// Sort names aka keys
	var keys []string
	for k := range cm.runtimeParams {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Walk over sorted names aka keys
	glog.V(1).Infof("Parameters num: %d\n", len(cm.runtimeParams))
	for _, k := range keys {
		glog.V(1).Infof("%s=%s\n", k, cm.runtimeParams[k])
	}
}

// GetRuntimeParam gets specified runtime param
func (cm *Manager) GetRuntimeParam(name string) (string, bool) {
	_map := cm.getEnvVarParams()
	nm, ok := _map[name]
	return nm, ok
}
