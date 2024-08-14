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

package namer

import (
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/macro"
	macroCommon "github.com/altinity/clickhouse-operator/pkg/model/common/macro"
)

const (
	// patternConfigMapCommonName is a template of common settings for the CHI ConfigMap. "chi-{chi}-common-configd"
	patternConfigMapCommonName = "chi- + macro.List.Get(macroCommon.MacrosCRName) + -common-configd"

	// patternConfigMapCommonUsersName is a template of common users settings for the CHI ConfigMap. "chi-{chi}-common-usersd"
	patternConfigMapCommonUsersName = "chi- + macro.List.Get(macroCommon.MacrosCRName) + -common-usersd"

	// patternConfigMapHostName is a template of macros ConfigMap. "chi-{chi}-deploy-confd-{cluster}-{shard}-{host}"
	patternConfigMapHostName = "chi- + macro.List.Get(macroCommon.MacrosCRName) + -deploy-confd- + macro.List.Get(macroCommon.MacrosClusterName) + - + macro.List.Get(macroCommon.MacrosHostName)"

	// patternCRServiceName is a template of Custom Resource Service name. "clickhouse-{chi}"
	patternCRServiceName = "clickhouse- + macro.MacrosCRName"

	// patternClusterServiceName is a template of cluster Service name. "cluster-{chi}-{cluster}"
	patternClusterServiceName = "cluster- + macro.MacrosCRName + - + macro.MacrosClusterName"

	// patternShardServiceName is a template of shard Service name. "shard-{chi}-{cluster}-{shard}"
	patternShardServiceName = "shard- + macro.MacrosCRName + - + macro.MacrosClusterName + - + macro.MacrosShardName"

	// patternReplicaServiceName is a template of replica Service name. "shard-{chi}-{cluster}-{replica}"
	patternReplicaServiceName = "shard- + macro.MacrosCRName + - + macro.MacrosClusterName + - + macro.MacrosReplicaName"

	// patternStatefulSetName is a template of host StatefulSet's name. "chi-{chi}-{cluster}-{shard}-{host}"
	patternStatefulSetName = "sts chi- + macro.MacrosCRName + - + macro.MacrosClusterName + - + macro.MacrosHostName"

	// patternStatefulSetServiceName is a template of host StatefulSet's Service name. "chi-{chi}-{cluster}-{shard}-{host}"
	patternStatefulSetServiceName = "service chi- + macro.MacrosCRName + - + macro.MacrosClusterName + - + macro.MacrosHostName"
)

var patterns = types.List{
	// patternConfigMapCommonName is a template of common settings for the CHI ConfigMap. "chi-{chi}-common-configd"
	patternConfigMapCommonName: "chi-" + macro.List.Get(macroCommon.MacrosCRName) + "-common-configd",

	// patternConfigMapCommonUsersName is a template of common users settings for the CHI ConfigMap. "chi-{chi}-common-usersd"
	patternConfigMapCommonUsersName: "chi-" + macro.List.Get(macroCommon.MacrosCRName) + "-common-usersd",

	// patternConfigMapHostName is a template of macros ConfigMap. "chi-{chi}-deploy-confd-{cluster}-{shard}-{host}"
	patternConfigMapHostName: "chi-" + macro.List.Get(macroCommon.MacrosCRName) + "-deploy-confd-" + macro.List.Get(macroCommon.MacrosClusterName) + "-" + macro.List.Get(macroCommon.MacrosHostName),

	// patternCRServiceName is a template of Custom Resource Service name. "clickhouse-{chi}"
	patternCRServiceName: "clickhouse-" + macro.List.Get(macroCommon.MacrosCRName),

	// patternClusterServiceName is a template of cluster Service name. "cluster-{chi}-{cluster}"
	patternClusterServiceName: "cluster-" + macro.List.Get(macroCommon.MacrosCRName) + "-" + macro.List.Get(macroCommon.MacrosClusterName),

	// patternShardServiceName is a template of shard Service name. "shard-{chi}-{cluster}-{shard}"
	patternShardServiceName: "shard-" + macro.List.Get(macroCommon.MacrosCRName) + "-" + macro.List.Get(macroCommon.MacrosClusterName) + "-" + macro.List.Get(macroCommon.MacrosShardName),

	// patternReplicaServiceName is a template of replica Service name. "shard-{chi}-{cluster}-{replica}"
	patternReplicaServiceName: "shard-" + macro.List.Get(macroCommon.MacrosCRName) + "-" + macro.List.Get(macroCommon.MacrosClusterName) + "-" + macro.List.Get(macroCommon.MacrosReplicaName),

	// patternStatefulSetName is a template of host StatefulSet's name. "chi-{chi}-{cluster}-{shard}-{host}"
	patternStatefulSetName: "chi-" + macro.List.Get(macroCommon.MacrosCRName) + "-" + macro.List.Get(macroCommon.MacrosClusterName) + "-" + macro.List.Get(macroCommon.MacrosHostName),

	// patternStatefulSetServiceName is a template of host StatefulSet's Service name. "chi-{chi}-{cluster}-{shard}-{host}"
	patternStatefulSetServiceName: "chi-" + macro.List.Get(macroCommon.MacrosCRName) + "-" + macro.List.Get(macroCommon.MacrosClusterName) + "-" + macro.List.Get(macroCommon.MacrosHostName),
}

const (
	// patternPodName is a name of a Pod within StatefulSet. In our setup each StatefulSet has only 1 pod,
	// so all pods would have '-0' suffix after StatefulSet name
	// Ex.: StatefulSetName-0
	patternPodName = "%s-0"
)

const (
	// patternNamespaceDomain presents Domain Name pattern of a namespace
	// In this pattern "%s" is substituted namespace name's value
	// Ex.: my-dev-namespace.svc.cluster.local
	patternNamespaceDomain = "%s.svc.cluster.local"

	// ServiceName.domain.name
	patternServiceFQDN = "%s" + "." + patternNamespaceDomain

	// patternPodFQDN consists of 3 parts:
	// 1. nameless service of of stateful set
	// 2. namespace name
	// Hostname.domain.name
	patternPodFQDN = "%s" + "." + patternNamespaceDomain
)
