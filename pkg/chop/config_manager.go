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

package chop

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/kubernetes-sigs/yaml"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	kube "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/deployment"
	chopClientSet "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"k8s.io/apimachinery/pkg/util/dump"
)

// ConfigChangeCallback defines the signature for configuration change callbacks
type ConfigChangeCallback func(*ConfigManager)

// ConfigManager specifies configuration manager in charge of operator's configuration
type ConfigManager struct {
	// kubeClient is a k8s client
	kubeClient *kube.Clientset

	// chopClient is a k8s client able to communicate with operator's Custom Resources
	chopClient *chopClientSet.Clientset

	// chopConfigList is a list of available operator configurations
	chopConfigList *api.ClickHouseOperatorConfigurationList

	// initConfigFilePath is a path to the configuration file, which will be used as initial/seed
	// to build final config, which will be used/consumed by users
	initConfigFilePath string

	// fileConfig is a prepared file-based config
	fileConfig *api.OperatorConfig

	// crConfigs is a slice of prepared Custom Resource based configs
	crConfigs []*api.OperatorConfig

	// config is the final config, built as merge of all available configs.
	// This config is ready to use/be consumed by users
	config *api.OperatorConfig

	// runtimeParams is set/map of runtime params, influencing configuration
	runtimeParams map[string]string

	// Secret watcher fields
	secretInformer cache.SharedIndexInformer
	secretMutex    sync.RWMutex
	stopCh         chan struct{}

	// Configuration change callbacks
	callbacks      []ConfigChangeCallback
	callbacksMutex sync.RWMutex
}

// newConfigManager creates new ConfigManager
func newConfigManager(
	kubeClient *kube.Clientset,
	chopClient *chopClientSet.Clientset,
	initConfigFilePath string,
) *ConfigManager {
	return &ConfigManager{
		kubeClient:         kubeClient,
		chopClient:         chopClient,
		initConfigFilePath: initConfigFilePath,
		stopCh:             make(chan struct{}),
	}
}

// Init reads config from all sources
func (cm *ConfigManager) Init() error {
	var err error

	// Get ENV vars
	cm.runtimeParams = cm.getEnvVarParams()
	cm.logEnvVarParams()

	// Get initial config from the file
	cm.fileConfig, err = cm.getFileBasedConfig(cm.initConfigFilePath)
	if err != nil {
		return err
	}
	log.V(1).Info("File-based CHOP config:")
	log.V(1).Info("\n" + cm.fileConfig.String(true))

	// Get configs from all config Custom Resources that are located in the namespace where Operator is running
	if namespace, ok := cm.GetRuntimeParam(deployment.OPERATOR_POD_NAMESPACE); ok {
		cm.getAllCRBasedConfigs(namespace)
		cm.logAllCRBasedConfigs()
	}

	// Prepare one unified config from all available config pieces
	cm.buildUnifiedConfig()

	cm.fetchSecretCredentials()

	// From now on we have one unified CHOP config
	log.V(1).Info("Unified CHOP config - with secret data fetched (but not post-processed yet):")
	log.V(1).Info("\n" + cm.config.String(true))

	// Finalize config by post-processing
	cm.Postprocess()

	// OperatorConfig is ready
	log.V(1).Info("Final CHOP config:")
	log.V(1).Info("\n" + cm.config.String(true))

	// Start secret watcher if secret is configured
	cm.startSecretWatcher()

	return nil
}

// Config is an access wrapper
func (cm *ConfigManager) Config() *api.OperatorConfig {
	return cm.config
}

// ChopClient returns the chopClient for accessing ClickHouse operator custom resources
func (cm *ConfigManager) ChopClient() *chopClientSet.Clientset {
	return cm.chopClient
}

// getAllCRBasedConfigs reads all ClickHouseOperatorConfiguration objects in specified namespace
func (cm *ConfigManager) getAllCRBasedConfigs(namespace string) {
	// We need to have chop kube client available in order to fetch ClickHouseOperatorConfiguration objects
	if cm.chopClient == nil {
		return
	}

	log.V(1).F().Info("Looking for ClickHouseOperatorConfigurations in namespace '%s'.", namespace)

	// Get list of ClickHouseOperatorConfiguration objects
	var err error
	if cm.chopConfigList, err = cm.chopClient.ClickhouseV1().ClickHouseOperatorConfigurations(namespace).List(context.TODO(), controller.NewListOptions()); err != nil {
		log.V(1).F().Error("Error read ClickHouseOperatorConfigurations in namespace '%s'. Err: %v", namespace, err)
		return
	}

	if cm.chopConfigList == nil {
		return
	}

	// Get sorted list of names of ClickHouseOperatorConfiguration objects located in specified namespace
	var names []string
	for i := range cm.chopConfigList.Items {
		chOperatorConfiguration := &cm.chopConfigList.Items[i]
		names = append(names, chOperatorConfiguration.Name)
	}
	sort.Strings(names)

	// Build sorted list of configs
	for _, name := range names {
		for i := range cm.chopConfigList.Items {
			// Convenience wrapper
			chOperatorConfiguration := &cm.chopConfigList.Items[i]
			if chOperatorConfiguration.Name == name {
				// Save location info into OperatorConfig itself
				chOperatorConfiguration.Spec.Runtime.ConfigCRNamespace = namespace
				chOperatorConfiguration.Spec.Runtime.ConfigCRName = name

				cm.crConfigs = append(cm.crConfigs, &chOperatorConfiguration.Spec)

				log.V(1).F().Error("Append ClickHouseOperatorConfigurations '%s/%s'.", namespace, name)
				continue
			}
		}
	}
}

// logAllCRBasedConfigs writes all ClickHouseOperatorConfiguration objects into log
func (cm *ConfigManager) logAllCRBasedConfigs() {
	for _, chOperatorConfiguration := range cm.crConfigs {
		log.V(1).Info("CR-based chop config: %s/%s :", chOperatorConfiguration.Runtime.ConfigCRNamespace, chOperatorConfiguration.Runtime.ConfigCRName)
		log.V(1).Info("\n" + chOperatorConfiguration.String(true))
	}
}

// buildUnifiedConfig prepares one config from all accumulated parts
func (cm *ConfigManager) buildUnifiedConfig() {
	// Start with file config as a base
	cm.config = cm.fileConfig
	cm.fileConfig = nil

	// Merge all the rest CR-based configs into base config
	for _, chOperatorConfiguration := range cm.crConfigs {
		_ = cm.config.MergeFrom(chOperatorConfiguration)
		cm.config.Runtime.ConfigCRSources = append(cm.config.Runtime.ConfigCRSources, api.ConfigCRSource{
			Namespace: chOperatorConfiguration.Runtime.ConfigCRNamespace,
			Name:      chOperatorConfiguration.Runtime.ConfigCRName,
		})
	}
}

// IsConfigListed checks whether specified ClickHouseOperatorConfiguration is listed in list of ClickHouseOperatorConfiguration(s)
func (cm *ConfigManager) IsConfigListed(config *api.ClickHouseOperatorConfiguration) bool {
	for i := range cm.chopConfigList.Items {
		chOperatorConfiguration := &cm.chopConfigList.Items[i]

		if config.Namespace == chOperatorConfiguration.Namespace &&
			config.Name == chOperatorConfiguration.Name &&
			config.ResourceVersion == chOperatorConfiguration.ResourceVersion {
			// Yes, this config already listed with the same resource version
			return true
		}
	}

	return false
}

const (
	fileIsOptional  = true
	fileIsMandatory = false
)

// getFileBasedConfig creates one OperatorConfig object based on the first available configuration file
func (cm *ConfigManager) getFileBasedConfig(configFilePath string) (*api.OperatorConfig, error) {
	// Check config files availability in the following order:
	// 1. Explicitly specified config file as CLI option
	// 2. Explicitly specified config file as ENV var
	// 3. Well-known config file in home dir
	// 4. Well-known config file in /etc
	// In case no file found fallback to the default config

	// 1. Check config file as explicitly specified CLI option
	if len(configFilePath) > 0 {
		// Config file is explicitly specified as a CLI flag, has to have this file.
		// Absence of the file is an error.
		conf, err := cm.buildConfigFromFile(configFilePath, fileIsMandatory)
		if err != nil {
			return nil, err
		}
		if conf != nil {
			return conf, nil
		}
		// Since this file is a mandatory one - has to fail
		return nil, fmt.Errorf("welcome Schrodinger: no conf, no err")
	}

	// 2. Check config file as explicitly specified ENV var
	configFilePath = os.Getenv(deployment.CHOP_CONFIG)
	if len(configFilePath) > 0 {
		// Config file is explicitly specified as an ENV var, has to have this file.
		// Absence of the file is an error.
		conf, err := cm.buildConfigFromFile(configFilePath, fileIsMandatory)
		if err != nil {
			return nil, err
		}
		if conf != nil {
			return conf, nil
		}
		// Since this file is a mandatory one - has to fail
		return nil, fmt.Errorf("welcome Schrodinger: no conf, no err")
	}

	// 3. Check config file as well-known config file in home dir
	// Try to find ~/.clickhouse-operator/config.yaml
	if usr, err := user.Current(); err == nil {
		// OS user found. Parse ~/.clickhouse-operator/config.yaml file
		// File is optional, absence of the file is not an error.
		configFilePath = filepath.Join(usr.HomeDir, ".clickhouse-operator", "config.yaml")
		conf, err := cm.buildConfigFromFile(configFilePath, fileIsOptional)
		if err != nil {
			return nil, err
		}
		if conf != nil {
			return conf, nil
		}
		// Since this file is an optional one - no return, continue to the next option
	}

	// 3. Check config file as well-known config file in /etc
	// Try to find /etc/clickhouse-operator/config.yaml
	{
		configFilePath = "/etc/clickhouse-operator/config.yaml"
		// File is optional, absence of the file is not an error
		conf, err := cm.buildConfigFromFile(configFilePath, fileIsOptional)
		if err != nil {
			return nil, err
		}
		if conf != nil {
			return conf, nil
		}
		// Since this file is an optional one - no return, continue to the next option
	}

	// No any config file found, fallback to default configuration
	return cm.buildDefaultConfig()
}

// buildConfigFromFile returns OperatorConfig struct built out of specified file path
func (cm *ConfigManager) buildConfigFromFile(configFilePath string, optional bool) (*api.OperatorConfig, error) {
	if _, err := os.Stat(configFilePath); errors.Is(err, os.ErrNotExist) && optional {
		// File does not exist, but it is optional. so there is not error per se
		return nil, nil
	}

	// Read config file content
	yamlText, err := os.ReadFile(filepath.Clean(configFilePath))
	if err != nil {
		return nil, err
	}

	// Parse config file content into OperatorConfig struct
	config := new(api.OperatorConfig)
	err = yaml.Unmarshal(yamlText, config)
	if err != nil {
		return nil, err
	}

	// Fill OperatorConfig's paths
	config.Runtime.ConfigFilePath, _ = filepath.Abs(configFilePath)
	config.Runtime.ConfigFolderPath = filepath.Dir(config.Runtime.ConfigFilePath)

	return config, nil
}

// buildDefaultConfig returns default OperatorConfig
func (cm *ConfigManager) buildDefaultConfig() (*api.OperatorConfig, error) {
	config := api.OperatorConfig{}

	return &config, nil
}

// listSupportedEnvVarNames return list of ENV vars that the operator supports
func (cm *ConfigManager) listSupportedEnvVarNames() []string {
	// This list of ENV VARS is specified in operator .yaml manifest, section "kind: Deployment"
	return []string{
		deployment.OPERATOR_POD_NODE_NAME,
		deployment.OPERATOR_POD_NAME,
		deployment.OPERATOR_POD_NAMESPACE,
		deployment.OPERATOR_POD_IP,
		deployment.OPERATOR_POD_SERVICE_ACCOUNT,

		deployment.OPERATOR_CONTAINER_CPU_REQUEST,
		deployment.OPERATOR_CONTAINER_CPU_LIMIT,
		deployment.OPERATOR_CONTAINER_MEM_REQUEST,
		deployment.OPERATOR_CONTAINER_MEM_LIMIT,

		deployment.WATCH_NAMESPACE,
		deployment.WATCH_NAMESPACES,
	}
}

// getEnvVarParams returns base set of runtime parameters filled by ENV vars
func (cm *ConfigManager) getEnvVarParams() map[string]string {
	params := make(map[string]string)
	// Extract parameters from ENV VARS
	for _, varName := range cm.listSupportedEnvVarNames() {
		params[varName] = os.Getenv(varName)
	}

	return params
}

// logEnvVarParams writes runtime parameters into log
func (cm *ConfigManager) logEnvVarParams() {
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
	log.V(1).Info("Parameters num: %d", len(cm.runtimeParams))
	for _, k := range keys {
		log.V(1).Info("%s=%s", k, cm.runtimeParams[k])
	}
}

// HasRuntimeParam checks whether specified runtime param exists
func (cm *ConfigManager) HasRuntimeParam(name string) bool {
	_map := cm.getEnvVarParams()
	_, ok := _map[name]
	return ok
}

// GetRuntimeParam gets specified runtime param
func (cm *ConfigManager) GetRuntimeParam(name string) (string, bool) {
	_map := cm.getEnvVarParams()
	value, ok := _map[name]
	return value, ok
}

// fetchSecretCredentials
func (cm *ConfigManager) fetchSecretCredentials() {
	// Secret name where to look for ClickHouse access credentials
	name := cm.config.ClickHouse.Access.Secret.Name

	// Do we need to fetch credentials from the secret?
	if name == "" {
		// No secret name specified, no need to read it
		return
	}

	// We have secret name specified, let's move on and read credentials

	// Figure out namespace where to look for the secret
	namespace := cm.config.ClickHouse.Access.Secret.Namespace
	if namespace == "" {
		// No namespace explicitly specified, let's look into namespace where pod is running
		if cm.HasRuntimeParam(deployment.OPERATOR_POD_NAMESPACE) {
			namespace, _ = cm.GetRuntimeParam(deployment.OPERATOR_POD_NAMESPACE)
		}
	}

	log.V(1).Info("Going to search for username/password in the secret '%s/%s'", namespace, name)

	// Sanity check
	if namespace == "" {
		// We've already checked that name is not empty
		cm.config.ClickHouse.Access.Secret.Runtime.Error = fmt.Sprintf("Still empty namespace for secret '%s'", name)
		return
	}

	secret, err := cm.kubeClient.CoreV1().Secrets(namespace).Get(context.TODO(), name, controller.NewGetOptions())
	if err != nil {
		cm.config.ClickHouse.Access.Secret.Runtime.Error = err.Error()
		log.V(1).Warning("Unable to fetch secret: '%s/%s'", namespace, name)
		return
	}

	cm.config.ClickHouse.Access.Secret.Runtime.Fetched = true
	log.V(1).Info("Secret fetched: '%s/%s'", namespace, name)

	// Find username and password from credentials
	for key, value := range secret.Data {
		switch key {
		case "username":
			cm.config.ClickHouse.Access.Secret.Runtime.Username = string(value)
			log.V(1).Info("Username read from the secret: '%s/%s'", namespace, name)
		case "password":
			cm.config.ClickHouse.Access.Secret.Runtime.Password = string(value)
			log.V(1).Info("Password read from the secret: '%s/%s'", namespace, name)
		}
	}
}

// Postprocess performs postprocessing of the configuration
func (cm *ConfigManager) Postprocess() {
	cm.config.Postprocess()
}

// startSecretWatcher initializes and starts the Secret informer to watch for changes
func (cm *ConfigManager) startSecretWatcher() {
	// Check if secret is configured
	secretName := cm.config.ClickHouse.Access.Secret.Name
	if secretName == "" {
		log.V(1).Info("No secret configured for ClickHouse access, skipping secret watcher")
		return
	}

	// Determine namespace
	namespace := cm.config.ClickHouse.Access.Secret.Namespace
	if namespace == "" {
		if cm.HasRuntimeParam(deployment.OPERATOR_POD_NAMESPACE) {
			namespace, _ = cm.GetRuntimeParam(deployment.OPERATOR_POD_NAMESPACE)
		}
	}

	if namespace == "" {
		log.V(1).Warning("Cannot determine namespace for secret watcher, skipping")
		return
	}

	log.V(1).Info("Starting secret watcher for secret '%s/%s'", namespace, secretName)

	// Create field selector to watch only the specific secret
	fieldSelector := fields.OneTermEqualSelector("metadata.name", secretName).String()

	// Create list/watch functions
	listWatch := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			options.FieldSelector = fieldSelector
			return cm.kubeClient.CoreV1().Secrets(namespace).List(context.TODO(), options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.FieldSelector = fieldSelector
			return cm.kubeClient.CoreV1().Secrets(namespace).Watch(context.TODO(), options)
		},
	}

	// Create the informer
	cm.secretInformer = cache.NewSharedIndexInformer(
		listWatch,
		&corev1.Secret{},
		time.Minute*5, // Resync period
		cache.Indexers{},
	)

	// Add event handlers
	cm.secretInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			secret := obj.(*corev1.Secret)
			if secret.Name == secretName {
				log.V(1).Info("Secret '%s/%s' added, updating configuration", namespace, secretName)
				cm.updateSecretCredentials(secret)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			secret := newObj.(*corev1.Secret)
			if secret.Name == secretName {
				log.V(1).Info("Secret '%s/%s' updated, updating configuration", namespace, secretName)
				cm.updateSecretCredentials(secret)
			}
		},
		DeleteFunc: func(obj interface{}) {
			secret := obj.(*corev1.Secret)
			if secret.Name == secretName {
				log.V(1).Warning("Secret '%s/%s' deleted, clearing credentials", namespace, secretName)
				cm.clearSecretCredentials()
			}
		},
	})

	// Start the informer
	go cm.secretInformer.Run(cm.stopCh)

	// Wait for cache to sync
	go func() {
		if !cache.WaitForCacheSync(cm.stopCh, cm.secretInformer.HasSynced) {
			log.V(1).Warning("Failed to sync secret informer cache")
		} else {
			log.V(1).Info("Secret informer cache synced successfully")
		}
	}()
}

// updateSecretCredentials updates the configuration with new secret data
func (cm *ConfigManager) updateSecretCredentials(secret *corev1.Secret) {
	cm.secretMutex.Lock()
	defer cm.secretMutex.Unlock()

	// Clear previous error state
	cm.config.ClickHouse.Access.Secret.Runtime.Error = ""
	cm.config.ClickHouse.Access.Secret.Runtime.Fetched = true

	// Extract username and password from secret
	for key, value := range secret.Data {
		switch key {
		case "username":
			cm.config.ClickHouse.Access.Secret.Runtime.Username = string(value)
		case "password":
			cm.config.ClickHouse.Access.Secret.Runtime.Password = string(value)
		}
	}

	// Post-process the configuration to apply the new credentials
	cm.config.Postprocess()

	sha256Sum := sha256.New()
	fmt.Fprintf(sha256Sum, "%v", dump.ForHash(secret))
	finalHash := fmt.Sprintf("%x", sha256Sum.Sum(nil))

	if cm.config.CHOPSecretHash != finalHash {
		cm.config.CHOPSecretHash = finalHash
		log.V(1).Info("Configuration updated with new secret credentials from '%s/%s'", secret.Namespace, secret.Name)
		// Invoke configuration change callbacks

		cm.invokeConfigChangeCallbacks()
	}
}

// clearSecretCredentials clears the secret credentials from configuration
func (cm *ConfigManager) clearSecretCredentials() {
	cm.secretMutex.Lock()
	defer cm.secretMutex.Unlock()

	// Clear credentials and mark as not fetched
	cm.config.ClickHouse.Access.Secret.Runtime.Username = ""
	cm.config.ClickHouse.Access.Secret.Runtime.Password = ""
	cm.config.ClickHouse.Access.Secret.Runtime.Fetched = false
	cm.config.ClickHouse.Access.Secret.Runtime.Error = "Secret was deleted"
	cm.config.CHOPSecretHash = "" // Clear the secret hash

	// Post-process the configuration
	cm.config.Postprocess()

	log.V(1).Warning("Secret credentials cleared from configuration")

	// Invoke configuration change callbacks
	cm.invokeConfigChangeCallbacks()
}

// StopSecretWatcher stops the secret watcher
func (cm *ConfigManager) StopSecretWatcher() {
	if cm.stopCh != nil {
		close(cm.stopCh)
		log.V(1).Info("Secret watcher stopped")
	}
}

// GetConfigWithLock returns the configuration with read lock protection
func (cm *ConfigManager) GetConfigWithLock() *api.OperatorConfig {
	cm.secretMutex.RLock()
	defer cm.secretMutex.RUnlock()
	return cm.config
}

// RegisterConfigChangeCallback registers a callback to be invoked when configuration changes
func (cm *ConfigManager) RegisterConfigChangeCallback(callback ConfigChangeCallback) {
	cm.callbacksMutex.Lock()
	defer cm.callbacksMutex.Unlock()
	cm.callbacks = append(cm.callbacks, callback)
	log.V(1).Info("Registered configuration change callback")
}

// invokeConfigChangeCallbacks invokes all registered configuration change callbacks
func (cm *ConfigManager) invokeConfigChangeCallbacks() {
	cm.callbacksMutex.RLock()
	callbacks := make([]ConfigChangeCallback, len(cm.callbacks))
	copy(callbacks, cm.callbacks)
	cm.callbacksMutex.RUnlock()

	log.V(1).Info("Invoking %d configuration change callbacks", len(callbacks))
	for i, callback := range callbacks {
		func() {
			defer func() {
				if r := recover(); r != nil {
					debug.PrintStack()
					log.V(1).Warning("Configuration change callback %d panicked: %v", i, r)
				}
			}()
			callback(cm)
		}()
	}
}
