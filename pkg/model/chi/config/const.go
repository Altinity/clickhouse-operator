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

import api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"

const (
	// CommonConfigDir specifies folder's name, where generated common XML files for ClickHouse would be placed
	CommonConfigDir = api.CommonConfigDirClickHouse

	// UsersConfigDir specifies folder's name, where generated users XML files for ClickHouse would be placed
	UsersConfigDir = api.UsersConfigDirClickHouse

	// HostConfigDir specifies folder's name, where generated host XML files for ClickHouse would be placed
	HostConfigDir = api.HostConfigDirClickHouse

	// TemplatesDir specifies folder's name where ClickHouseInstallationTemplates are located
	TemplatesDir = api.TemplatesDirClickHouse
)

const (
	DirPathConfigRoot = "/etc/clickhouse-server"

	// DirPathConfigCommon specifies full path to folder,
	// where generated common XML files for the following sections would be placed:
	// 1. remote servers
	// 2. operator-provided additional config files
	DirPathConfigCommon = DirPathConfigRoot + "/" + CommonConfigDir + "/"

	// DirPathConfigUsers specifies full path to folder, where generated users XML files would be placed
	// for the following sections:
	// 1. users
	// 2. quotas
	// 3. profiles
	// 4. operator-provided additional config files
	DirPathConfigUsers = DirPathConfigRoot + "/" + UsersConfigDir + "/"

	// DirPathConfigHost specifies full path to folder, where generated host XML files would be placed
	// for the following sections:
	// 1. macros
	// 2. zookeeper
	// 3. settings
	// 4. files
	// 5. operator-provided additional config files
	DirPathConfigHost = DirPathConfigRoot + "/" + HostConfigDir + "/"

	// DirPathSecretFilesConfig specifies full path to folder, where secrets are mounted
	DirPathSecretFilesConfig = DirPathConfigRoot + "/" + "secrets.d" + "/"

	// DirPathDataStorage specifies full path of data folder where ClickHouse would place its data storage
	DirPathDataStorage = "/var/lib/clickhouse"

	// DirPathLogStorage  specifies full path of data folder where ClickHouse would place its log files
	DirPathLogStorage = "/var/log/clickhouse-server"
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
	// ZkDefaultPort specifies Zookeeper default port
	ZkDefaultPort = 2181
	// ZkDefaultRootTemplate specifies default ZK root - /clickhouse/{namespace}/{chi name}
	ZkDefaultRootTemplate = "/clickhouse/%s/%s"
)
