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
	LabelApp             = clickhousealtinitycom.GroupName + "/app"
	LabelAppValue        = "chop"
	LabelChop            = clickhousealtinitycom.GroupName + "/chop"
	LabelChi             = clickhousealtinitycom.GroupName + "/chi"
	LabelCluster         = clickhousealtinitycom.GroupName + "/cluster"
	LabelShard           = clickhousealtinitycom.GroupName + "/shard"
	LabelReplica         = clickhousealtinitycom.GroupName + "/replica"
	LabelZkConfigVersion = clickhousealtinitycom.GroupName + "/zkv"
	LabelStatefulSet     = "StatefulSet"
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

	// dirPathClickHouseData specifies full path of data folder where ClickHouse would place its datastorage
	dirPathClickHouseData = "/var/lib/clickhouse"
)

const (
	// NAME                           READY   AGE   CONTAINERS    IMAGES
	// statefulset.apps/ss-1eb454-1   0/1     2s    ss-1eb454-1   yandex/clickhouse-server:latest
	statefulSetNamePattern = "chi-%s-%s-%s-%s"

	// NAME                  TYPE       CLUSTER-IP  EXTERNAL-IP  PORT(S)                     AGE  SELECTOR
	// service/svc-1eb454-1  ClusterIP  None        <none>       9000/TCP,9009/TCP,8123/TCP  2s   clickhouse.altinity.com/app=ss-1eb454-1
	// service/svc-1eb454-2  ClusterIP  None        <none>       9000/TCP,9009/TCP,8123/TCP  2s   clickhouse.altinity.com/app=ss-1eb454-2
	// In this pattern "%s" is substituted with fullDeploymentIDPattern-generated value
	// Ex.: svc-1eb454-2
	statefulSetServiceNamePattern = "chi-%s-%s-%s-%s"

	// namespaceDomainPattern presents Domain Name pattern of a namespace
	// In this pattern "%s" is substituted namespace name's value
	// Ex.: my-dev-namespace.svc.cluster.local
	namespaceDomainPattern = "%s.svc.cluster.local"

	// NAME                  TYPE       CLUSTER-IP  EXTERNAL-IP  PORT(S)                     AGE  SELECTOR
	// service/clickhouse-replcluster   ClusterIP   None         <none>        9000/TCP,9009/TCP,8123/TCP   1h
	// In this pattern "%s" is substituted with clickhouse installation name - 'replcluster' in this case
	// Ex.: test
	chiServiceNamePattern = "clickhouse-%s"

	// ServiceName.domain.name
	chiServiceFQDNPattern = "%s" + "." + namespaceDomainPattern

	// podFQDNPattern consists of 3 parts:
	// 1. nameless service of of stateful set
	// 2. namespace name
	// Hostname.domain.name
	podFQDNPattern = "%s" + "." + namespaceDomainPattern

	// podNamePattern is a name of a Pod as ServiceName-0
	podNamePattern = "%s-0"

	// NAME                                       DATA   AGE
	// chi-example-01-common-configd              2      2s
	// chi-example-01-common-usersd               0      2s
	// chi-example-01-deploy-confd-4a8ff63336-0   1      1s

	// configMapCommonNamePattern is a template of common settings for the CHI ConfigMap
	// Ex.: chi-example02-common-configd for chi named as 'example02'
	configMapCommonNamePattern = "chi-%s-common-configd"

	// configMapCommonUsersNamePattern is a template of common users settings for the CHI ConfigMap
	// Ex.: chi-example02-common-usersd for chi named as 'example02'
	configMapCommonUsersNamePattern = "chi-%s-common-usersd"

	// configMapDeploymentNamePattern is a template of macros ConfigMap
	// Ex.: chi-example02-deploy-confd-33260f1800-2 for chi named as 'example02'
	configMapDeploymentNamePattern = "chi-%s-deploy-confd-%s-%s-%s"

	distributedDDLPathPattern = "/clickhouse/%s/task_queue/ddl"
)

const (
	// Default docker image to be used
	defaultClickHouseDockerImage = "yandex/clickhouse-server:latest"

	// Index of container within Pod with ClickHouse instance. Pod may have other containers included, such as monitoring
	ClickHouseContainerIndex = 0
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
