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
	dotXML = ".xml"

	// Filenames of the config files in /etc/clickhouse-server/config.d
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
	// Full Deployment ID consists of two parts:
	// 1. "deployment id" (it should be derived from fingerprint) of each deployment in ClickHouseInstallation object.
	//    Some deployments may be the same and thus have the same "deployment id" (because of the same fingerprint)
	// 2. Sequential index of this "deployment id" in ClickHouseInstallation object.
	//    Some deployments may be the same and thus have the same "deployment id" (because of the same fingerprint),
	//    but they will have different sequentially increasing index of this "deployment id" in ClickHouseInstallation object
	// Ex.: two running instances of the same deployment will have full deployments ids
	// 1eb454-1
	// 1eb454-2
	fullDeploymentIDPattern = "%s-%d"

	// NAME                           READY   AGE   CONTAINERS    IMAGES
	// statefulset.apps/ss-1eb454-1   0/1     2s    ss-1eb454-1   yandex/clickhouse-server:latest
	statefulSetNamePattern = "chi-%s"

	// NAME                  TYPE       CLUSTER-IP  EXTERNAL-IP  PORT(S)                     AGE  SELECTOR
	// service/svc-1eb454-1  ClusterIP  None        <none>       9000/TCP,9009/TCP,8123/TCP  2s   clickhouse.altinity.com/app=ss-1eb454-1
	// service/svc-1eb454-2  ClusterIP  None        <none>       9000/TCP,9009/TCP,8123/TCP  2s   clickhouse.altinity.com/app=ss-1eb454-2
	// In this pattern "%s" is substituted with fullDeploymentIDPattern-generated value
	// Ex.: svc-1eb454-2
	serviceNamePattern = "chi-%s"

	// namespaceDomainPattern presents Domain Name pattern of a namespace
	// In this pattern "%s" is substituted namespace name's value
	// Ex.: my-dev-namespace.svc.cluster.local
	namespaceDomainPattern = "%s.svc.cluster.local"

	// NAME                READY   STATUS    RESTARTS   AGE   IP            NODE   NOMINATED NODE   READINESS GATES
	// pod/ss-1eb454-2-0   1/1     Running   0          11h   10.244.1.17   kub2   <none>           <none>
	// Ex.: ss-1eb454-2-0
	hostnamePattern = statefulSetNamePattern + "-0"

	// hostnamePlusServicePattern consists of 2 parts
	// 1. pod hostname
	// 2. nameless service of of stateful set
	// Ex.: ss-1eb454-2-0.svc-1eb454-2
	hostnamePlusServicePattern = hostnamePattern + "." + serviceNamePattern

	// podFQDNPattern consists of 3 parts:
	// 1. pod hostname
	// 2. nameless service of of stateful set
	// 3. namespace name
	// ss-1eb454-2-0.svc-1eb454-2.my-dev-domain.svc.cluster.local
	podFQDNPattern = hostnamePlusServicePattern + "." + namespaceDomainPattern

	// configMapNameXXXPattern is set of constants to describe
	// a .meta.name of a kind:ConfigMap based on .meta.name of a CHI object
	// configMapNamePattern is a common ConfigMap name prefix
	configMapNamePattern       = "chi-%s-configd"
	// configMapCommonNamePattern is a template of common for the CHI ConfigMap
	// Ex.: chi-example02-configd-common for chi named as 'example02'
	configMapCommonNamePattern = configMapNamePattern + "-common"
	// configMapMacrosNamePattern is a template of macros ConfigMap
	// Ex.: chi-example02-configd-33260f1800-2 for chi named as 'example02'
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
	fullPathConfigd          = "/etc/clickhouse-server/config.d/"
	fullPathRemoteServersXML = fullPathConfigd + filenameRemoteServersXML
	fullPathZookeeperXML     = fullPathConfigd + filenameZookeeperXML
	fullPathMacrosXML        = fullPathConfigd + filenameMacrosXML
	fullPathUsersXML         = fullPathConfigd + filenameUsersXML
	fullPathQuotasXML        = fullPathConfigd + filenameQuotasXML
	fullPathProfilesXML      = fullPathConfigd + filenameProfilesXML
	fullPathSettingsXML      = fullPathConfigd + filenameSettingsXML
	fullPathClickHouseData   = "/var/lib/clickhouse"
)

const (
	templateDefaultsServiceClusterIP = "None"
)
