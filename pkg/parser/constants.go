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

package parser

import (
	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com"
)

const (
	// ObjectsConfigMaps defines a category of the ConfigMap objects list
	ObjectsConfigMaps ObjectKind = iota + 1
	// ObjectsStatefulSets defines a category of the StatefulSet objects list
	ObjectsStatefulSets
	// ObjectsServices defines a category of the Service objects list
	ObjectsServices
)

const (
	// ClusterwideLabel applied to all objects created by the ClickHouse Operator
	ClusterwideLabel = clickhousealtinitycom.GroupName + "/chi"
)

const (
	clusterLayoutTypeStandard = "Standard"
	clusterLayoutTypeAdvanced = "Advanced"
)

const (
	shardDefinitionTypeReplicasCount = "ReplicasCount"
	shardDefinitionTypeReplicas      = "Replicas"
)

const (
	deploymentScenarioDefault      = "Default"
	deploymentScenarioNodeMonopoly = "NodeMonopoly"
)

const (
	shardInternalReplicationDisabled = "Disabled"
	stringTrue                       = "true"
	stringFalse                      = "false"
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
	dotXML           = ".xml"
	remoteServersXML = configRemoteServers + dotXML
	zookeeperXML     = configZookeeper + dotXML
	usersXML         = configUsers + dotXML
	quotasXML        = configQuotas + dotXML
	profilesXML      = configProfiles + dotXML
	settingsXML      = configSettings + dotXML
	macrosXML        = configMacros + dotXML
)

const (
	ssNameIDPattern            = "d%si%d"
	ssNamePattern              = "ch-%s"
	svcNamePattern             = "%ss"
	domainPattern              = ".%s.svc.cluster.local"
	hostnamePattern            = ssNamePattern + "-0.%[1]ss%s"
	fqdnPattern                = "%s-0.%s%s"
	configMapNamePattern       = "chi-%s-configd"
	configMapMacrosNamePattern = configMapNamePattern + "-%s"
	distributedDDLPattern      = "/clickhouse/%s/task_queue/ddl"
)

const (
	chDefaultDockerImage         = "yandex/clickhouse-server:latest"
	chDefaultVolumeMountNameData = "clickhouse-data"
)

const (
	useDefaultNamePlaceholder = "USE_DEFAULT_NAME"
)

const (
	chDefaultRPCPortName           = "rpc"
	chDefaultRPCPortNumber         = 9000
	chDefaultInterServerPortName   = "interserver"
	chDefaultInterServerPortNumber = 9009
	chDefaultRestPortName          = "rest"
	chDefaultRestPortNumber        = 8123
	chDefaultAppLabel              = clickhousealtinitycom.GroupName + "/app"
)

const (
	configdPath              = "/etc/clickhouse-server/config.d/"
	fullPathRemoteServersXML = configdPath + remoteServersXML
	fullPathZookeeperXML     = configdPath + zookeeperXML
	fullPathMacrosXML        = configdPath + macrosXML
	fullPathUsersXML         = configdPath + usersXML
	fullPathQuotasXML        = configdPath + quotasXML
	fullPathProfilesXML      = configdPath + profilesXML
	fullPathSettingsXML      = configdPath + settingsXML
	fullPathClickHouseData   = "/var/lib/clickhouse"
)

const (
	templateDefaultsServiceClusterIP = "None"
)
