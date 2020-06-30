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

package model

import "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"

const (
	xmlTagYandex = "yandex"
)

const (
	configMacros        = "macros"
	configPorts         = "ports"
	configProfiles      = "profiles"
	configQuotas        = "quotas"
	configRemoteServers = "remote_servers"
	configSettings      = "settings"
	configUsers         = "users"
	configZookeeper     = "zookeeper"
)

const (
	// dirPathCommonConfig specifies full path to folder, where generated common XML files for ClickHouse would be placed
	// for the following sections:
	// 1. remote servers
	// 2. operator-provided additional config files
	dirPathCommonConfig = "/etc/clickhouse-server/" + v1.CommonConfigDir + "/"

	// dirPathUsersConfig specifies full path to folder, where generated users XML files for ClickHouse would be placed
	// for the following sections:
	// 1. users
	// 2. quotas
	// 3. profiles
	// 4. operator-provided additional config files
	dirPathUsersConfig = "/etc/clickhouse-server/" + v1.UsersConfigDir + "/"

	// dirPathHostConfig specifies full path to folder, where generated host XML files for ClickHouse would be placed
	// for the following sections:
	// 1. macros
	// 2. zookeeper
	// 3. settings
	// 4. files
	// 5. operator-provided additional config files
	dirPathHostConfig = "/etc/clickhouse-server/" + v1.HostConfigDir + "/"

	// dirPathClickHouseData specifies full path of data folder where ClickHouse would place its data storage
	dirPathClickHouseData = "/var/lib/clickhouse"

	// dirPathClickHouseLog  specifies full path of data folder where ClickHouse would place its log files
	dirPathClickHouseLog = "/var/log/clickhouse-server"
)

const (
	// Default ClickHouse docker image to be used
	defaultClickHouseDockerImage = "yandex/clickhouse-server:latest"

	// Default BusyBox docker image to be used
	defaultBusyBoxDockerImage = "busybox"

	// Name of container within Pod with ClickHouse instance. Pod may have other containers included, such as monitoring
	ClickHouseContainerName    = "clickhouse"
	ClickHouseLogContainerName = "clickhouse-log"
)

const (
	chPortNumberMustBeAssignedLater = 0

	// ClickHouse open ports
	chDefaultTCPPortName               = "tcp"
	chDefaultTCPPortNumber             = int32(9000)
	chDefaultHTTPPortName              = "http"
	chDefaultHTTPPortNumber            = int32(8123)
	chDefaultInterserverHTTPPortName   = "interserver"
	chDefaultInterserverHTTPPortNumber = int32(9009)
)
const (
	zkDefaultPort = 2181
	// zkDefaultRootTemplate specifies default ZK root - /clickhouse/{namespace}/{chi name}
	zkDefaultRootTemplate = "/clickhouse/%s/%s"
)
