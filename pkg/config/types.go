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
	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

type Config struct {
	// Full path to the config file and folder where this Config originates from
	ConfigFilePath   string
	ConfigFolderPath string

	// Namespaces where operator watches for events
	Namespaces []string `yaml:"namespaces"`

	// Paths where to look for additional ClickHouse config .xml files to be mounted into Pod
	// config.d
	// conf.d
	// users.d
	// respectively
	ChCommonConfigsPath string `yaml:"chCommonConfigsPath"`
	ChPodConfigsPath    string `yaml:"chPodConfigsPath"`
	ChUsersConfigsPath  string `yaml:"chUsersConfigsPath"`
	// Config files fetched from these paths. Maps "file name->file content"
	ChCommonConfigs map[string]string
	ChPodConfigs    map[string]string
	ChUsersConfigs  map[string]string

	// Path where to look for ClickHouseInstallation templates .yaml files
	ChiTemplatesPath string `yaml:"chiTemplatesPath"`
	// Chi templates fetched from this path. Maps "file name->file content"
	ChiTemplates map[string]string
	// ClickHouseInstallation template
	ChiTemplate *chiv1.ClickHouseInstallation

	// Create/Update StatefulSet behavior - for how long to wait for StatefulSet to reach new Generation
	StatefulSetUpdateTimeout uint64 `yaml:"statefulSetUpdateTimeout"`
	// Create/Update StatefulSet behavior - for how long to sleep while polling StatefulSet to reach new Generation
	StatefulSetUpdatePollPeriod uint64 `yaml:"statefulSetUpdatePollPeriod"`

	// Rolling Create/Update behavior
	// StatefulSet create behavior - what to do in case StatefulSet can't reach new Generation
	OnStatefulSetCreateFailureAction string `yaml:"onStatefulSetCreateFailureAction"`
	// StatefulSet update behavior - what to do in case StatefulSet can't reach new Generation
	OnStatefulSetUpdateFailureAction string `yaml:"onStatefulSetUpdateFailureAction"`

	// Default values for ClickHouse user configuration
	// 1. user/profile - string
	// 2. user/quota - string
	// 3. user/networks/ip - multiple strings
	// 4. user/password - string
	ChConfigUserDefaultProfile    string   `yaml:"chConfigUserDefaultProfile"`
	ChConfigUserDefaultQuota      string   `yaml:"chConfigUserDefaultQuota"`
	ChConfigUserDefaultNetworksIP []string `yaml:"chConfigUserDefaultNetworksIP"`
	ChConfigUserDefaultPassword   string   `yaml:"chConfigUserDefaultPassword"`
}

const (
	// What to do in case StatefulSet can't reach new Generation - abort rolling create
	OnStatefulSetCreateFailureActionAbort = "abort"

	// What to do in case StatefulSet can't reach new Generation - delete newly created problematic StatefulSet
	OnStatefulSetCreateFailureActionDelete = "delete"
)

const (
	// What to do in case StatefulSet can't reach new Generation - abort rolling update
	OnStatefulSetUpdateFailureActionAbort = "abort"

	// What to do in case StatefulSet can't reach new Generation - delete Pod and rollback StatefulSet to previous Generation
	// Pod would be recreated by StatefulSet based on rollback-ed configuration
	OnStatefulSetUpdateFailureActionRollback = "rollback"
)
