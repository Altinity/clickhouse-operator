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
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

type FilesGroupType string

const (
	FilesGroupCommon FilesGroupType = "FilesGroupType common"
	FilesGroupUsers  FilesGroupType = "FilesGroupType users"
	FilesGroupHost   FilesGroupType = "FilesGroupType host"
)

const (
	xmlTagYandex = "yandex"
)

const (
	configMacros        = "macros"
	configHostnamePorts = "hostname-ports"
	configProfiles      = "profiles"
	configQuotas        = "quotas"
	configRemoteServers = "remote_servers"
	configSettings      = "settings"
	configUsers         = "users"
	configZookeeper     = "zookeeper"
)

const (
	// DirPathCommonConfig specifies full path to folder, where generated common XML files for ClickHouse would be placed
	// for the following sections:
	// 1. remote servers
	// 2. operator-provided additional config files
	DirPathCommonConfig = "/etc/clickhouse-server/" + api.CommonConfigDir + "/"

	// DirPathUsersConfig specifies full path to folder, where generated users XML files for ClickHouse would be placed
	// for the following sections:
	// 1. users
	// 2. quotas
	// 3. profiles
	// 4. operator-provided additional config files
	DirPathUsersConfig = "/etc/clickhouse-server/" + api.UsersConfigDir + "/"

	// DirPathHostConfig specifies full path to folder, where generated host XML files for ClickHouse would be placed
	// for the following sections:
	// 1. macros
	// 2. zookeeper
	// 3. settings
	// 4. files
	// 5. operator-provided additional config files
	DirPathHostConfig = "/etc/clickhouse-server/" + api.HostConfigDir + "/"

	// DirPathSecretFilesConfig specifies full path to folder, where secrets are mounted
	DirPathSecretFilesConfig = "/etc/clickhouse-server/secrets.d/"

	// DirPathClickHouseData specifies full path of data folder where ClickHouse would place its data storage
	DirPathClickHouseData = "/var/lib/clickhouse"

	// DirPathClickHouseLog  specifies full path of data folder where ClickHouse would place its log files
	DirPathClickHouseLog = "/var/log/clickhouse-server"

	// DirPathDockerEntrypointInit specified full path of docker-entrypoint-initdb.d
	// For more details please check: https://github.com/ClickHouse/ClickHouse/issues/3319
	DirPathDockerEntrypointInit = "/docker-entrypoint-initdb.d"
)

const (
	// DefaultClickHouseDockerImage specifies default ClickHouse docker image to be used
	DefaultClickHouseDockerImage = "clickhouse/clickhouse-server:latest"

	// DefaultBusyBoxDockerImage specifies default BusyBox docker image to be used
	DefaultBusyBoxDockerImage = "busybox"

	// DefaultUbiDockerImage specifies default ubi docker image to be used
	DefaultUbiDockerImage = "registry.access.redhat.com/ubi8/ubi-minimal:latest"

	// Name of container within Pod with ClickHouse instance.
	// Pod may have other containers included, such as monitoring, logging

	// ClickHouseContainerName specifies name of the clickhouse container in the pod
	ClickHouseContainerName = "clickhouse"
	// ClickHouseLogContainerName specifies name of the logger container in the pod
	ClickHouseLogContainerName = "clickhouse-log"
)

const (
	// ZkDefaultPort specifies Zookeeper default port
	ZkDefaultPort = 2181
	// ZkDefaultRootTemplate specifies default ZK root - /clickhouse/{namespace}/{chi name}
	ZkDefaultRootTemplate = "/clickhouse/%s/%s"
)
