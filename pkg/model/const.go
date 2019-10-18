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

import (
	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com"
)

const (
	// Kubernetes labels
	LabelApp      = clickhousealtinitycom.GroupName + "/app"
	LabelAppValue = "chop"
	LabelChop     = clickhousealtinitycom.GroupName + "/chop"
	LabelChi      = clickhousealtinitycom.GroupName + "/chi"
	LabelCluster  = clickhousealtinitycom.GroupName + "/cluster"
	LabelShard    = clickhousealtinitycom.GroupName + "/shard"
	LabelReplica  = clickhousealtinitycom.GroupName + "/replica"
	// Supplementary service labels - used to cooperate with k8s
	LabelZookeeperConfigVersion = clickhousealtinitycom.GroupName + "/zookeeper-version"
	LabelSettingsConfigVersion  = clickhousealtinitycom.GroupName + "/settings-version"
)

const (
	xmlTagYandex = "yandex"
)

const (
	configUsers         = "users"
	configProfiles      = "profiles"
	configQuotas        = "quotas"
	configSettings      = "settings"
	configRemoteServers = "remote_servers"
	configZookeeper     = "zookeeper"
	configMacros        = "macros"
)

const (
	dotXML = ".xml"

	// Filenames of the chopConfig files in /etc/clickhouse-server/config.d
	// These files would be created as ConfigMaps mapping if necessary
	filenameRemoteServersXML = configRemoteServers + dotXML
	filenameZookeeperXML     = configZookeeper + dotXML
	filenameUsersXML         = configUsers + dotXML
	filenameQuotasXML        = configQuotas + dotXML
	filenameProfilesXML      = configProfiles + dotXML
	filenameSettingsXML      = configSettings + dotXML
	filenameMacrosXML        = configMacros + dotXML
)

const (
	// fullPathConfigd specifies full path to folder, where generated XML chopConfig files for ClickHouse would be placed

	// dirPathConfigd specifies full path to folder, where generated XML chopConfig files for ClickHouse would be placed
	// for the following sections:
	// 1. remote servers
	// 2. zookeeper
	// 3. settings
	// 4. listen
	dirPathConfigd = "/etc/clickhouse-server/config.d/"

	// dirPathUsersd specifies full path to folder, where generated XML chopConfig files for ClickHouse would be placed
	// for the following sections:
	// 1. users
	// 2. quotas
	// 3. profiles
	dirPathUsersd = "/etc/clickhouse-server/users.d/"

	// dirPathConfd specifies full path to folder, where generated XML chopConfig files for ClickHouse would be placed
	// for the following sections:
	// 1. macros
	dirPathConfd = "/etc/clickhouse-server/conf.d/"

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
	// ClickHouse open ports
	chDefaultHTTPPortName          = "http"
	chDefaultHTTPPortNumber        = 8123
	chDefaultClientPortName        = "client"
	chDefaultClientPortNumber      = 9000
	chDefaultInterServerPortName   = "interserver"
	chDefaultInterServerPortNumber = 9009
)

const (
	// Default value for ClusterIP service
	templateDefaultsServiceClusterIP = "None"
)

const (
	podDistributionOnePerHost  = "OnePerHost"
	podDistributionUnspecified = "Unspecified"
)

const (
	zkDefaultPort = 2181
	// zkDefaultRootTemplate specifies default ZK root - /clickhouse/{namespace}/{chi name}
	zkDefaultRootTemplate = "/clickhouse/%s/%s"
)
