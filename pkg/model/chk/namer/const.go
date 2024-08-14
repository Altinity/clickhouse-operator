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
	"github.com/altinity/clickhouse-operator/pkg/model/chk/macro"
	macroCommon "github.com/altinity/clickhouse-operator/pkg/model/common/macro"
)

const (
	// namePatternConfigMapHost is a template of macros ConfigMap. "chi-{chi}-deploy-confd-{cluster}-{shard}-{host}"
	namePatternConfigMapHost = "chk- + macro.MacrosCRName + -deploy-confd- + macro.MacrosClusterName + - + macro.MacrosHostName"

	// namePatternCRService is a template of Custom Resource Service name. "clickhouse-{chi}"
	namePatternCRService = "clickhouse- + macro.MacrosCRName"

	// namePatternClusterService is a template of cluster Service name. "cluster-{chi}-{cluster}"
	namePatternClusterService = "cluster- + macro.MacrosCRName + - + macro.MacrosClusterName"

	// namePatternShardService is a template of shard Service name. "shard-{chi}-{cluster}-{shard}"
	namePatternShardService = "shard- + macro.MacrosCRName + - + macro.MacrosClusterName + - + macro.MacrosShardName"

	// namePatternReplicaService is a template of replica Service name. "shard-{chi}-{cluster}-{replica}"
	namePatternReplicaService = "shard- + macro.MacrosCRName + - + macro.MacrosClusterName + - + macro.MacrosReplicaName"

	// namePatternStatefulSet is a template of host StatefulSet's name. "chi-{chi}-{cluster}-{shard}-{host}"
	namePatternStatefulSet = "sts chk- + macro.MacrosCRName + - + macro.MacrosClusterName + - + macro.MacrosHostName"

	// namePatternStatefulSetService is a template of host StatefulSet's Service name. "chi-{chi}-{cluster}-{shard}-{host}"
	namePatternStatefulSetService = "service chk- + macro.MacrosCRName + - + macro.MacrosClusterName + - + macro.MacrosHostName"
)

var patterns = types.List{
	// namePatternConfigMapHost is a template of macros ConfigMap. "chi-{chi}-deploy-confd-{cluster}-{shard}-{host}"
	namePatternConfigMapHost: "chk-" + macro.List.Get(macroCommon.MacrosCRName) + "-deploy-confd-" + macro.List.Get(macroCommon.MacrosClusterName) + "-" + macro.List.Get(macroCommon.MacrosHostName),

	// namePatternCRService is a template of Custom Resource Service name. "clickhouse-{chi}"
	namePatternCRService: "clickhouse-" + macro.List.Get(macroCommon.MacrosCRName),

	// namePatternClusterService is a template of cluster Service name. "cluster-{chi}-{cluster}"
	namePatternClusterService: "cluster-" + macro.List.Get(macroCommon.MacrosCRName) + "-" + macro.List.Get(macroCommon.MacrosClusterName),

	// namePatternShardService is a template of shard Service name. "shard-{chi}-{cluster}-{shard}"
	namePatternShardService: "shard-" + macro.List.Get(macroCommon.MacrosCRName) + "-" + macro.List.Get(macroCommon.MacrosClusterName) + "-" + macro.List.Get(macroCommon.MacrosShardName),

	// namePatternReplicaService is a template of replica Service name. "shard-{chi}-{cluster}-{replica}"
	namePatternReplicaService: "shard-" + macro.List.Get(macroCommon.MacrosCRName) + "-" + macro.List.Get(macroCommon.MacrosClusterName) + "-" + macro.List.Get(macroCommon.MacrosReplicaName),

	// namePatternStatefulSet is a template of host StatefulSet's name. "chi-{chi}-{cluster}-{shard}-{host}"
	namePatternStatefulSet: "chk-" + macro.List.Get(macroCommon.MacrosCRName) + "-" + macro.List.Get(macroCommon.MacrosClusterName) + "-" + macro.List.Get(macroCommon.MacrosHostName),

	// namePatternStatefulSetService is a template of host StatefulSet's Service name. "chi-{chi}-{cluster}-{shard}-{host}"
	namePatternStatefulSetService: "chk-" + macro.List.Get(macroCommon.MacrosCRName) + "-" + macro.List.Get(macroCommon.MacrosClusterName) + "-" + macro.List.Get(macroCommon.MacrosHostName),
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
