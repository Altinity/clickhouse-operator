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
	// ChopGeneratedLabel applied to all objects created by the ClickHouse Operator
	ChopGeneratedLabel = clickhousealtinitycom.GroupName + "/chop"
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
	// Full Deployment ID consists of two parts:
	// 1. "deployment id" (it should be derived from fingerprint) of each deployment in ClickHouseInstallation object.
	//    Some deployments may be the same and thus have the same "deployment id" (because of the same fingerprint)
	// 2. Sequential index of this "deployment id" in ClickHouseInstallation object.
	//    Some deployments may be the same and thus have the same "deployment id" (because of the same fingerprint),
	//    but they will have different sequentially increasing index of this "deployment id" in ClickHouseInstallation object
	// Ex.: two running instances of the same deployment
	// 1eb454-1
	// 1eb454-2
	fullDeploymentIDPattern = "%s-%d"

	// NAME                           READY   AGE   CONTAINERS    IMAGES
	// statefulset.apps/ss-1eb454-1   0/1     2s    ss-1eb454-1   yandex/clickhouse-server:latest
	statefulSetNamePattern     = "ss-%s"

	// NAME                  TYPE       CLUSTER-IP  EXTERNAL-IP  PORT(S)                     AGE  SELECTOR
	// service/svc-1eb454-1  ClusterIP  None        <none>       9000/TCP,9009/TCP,8123/TCP  2s   clickhouse.altinity.com/app=ss-1eb454-1
	// service/svc-1eb454-2  ClusterIP  None        <none>       9000/TCP,9009/TCP,8123/TCP  2s   clickhouse.altinity.com/app=ss-1eb454-2
	serviceNamePattern         = "svc-%s"

	domainPattern              = ".%s.svc.cluster.local"

	// ss-1eb454-1-0.1eb454-1s
	// ss-1eb454-1-0.svc-1eb454-1.dev.svc.cluster.local
	// TODO FIX IT
	// TODO WTF IS THIS?
	hostnamePattern            = statefulSetNamePattern + "-0.%[1]ss%s"

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
