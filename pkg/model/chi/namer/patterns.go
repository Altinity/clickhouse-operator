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
	macrosList "github.com/altinity/clickhouse-operator/pkg/model/chi/macro"
	"github.com/altinity/clickhouse-operator/pkg/model/common/macro"
)

var patterns = types.List{
	// patternConfigMapCommonName is a template of common settings for the CHI ConfigMap. "chi-{chi}-common-configd"
	patternConfigMapCommonName: "chi-" + macrosList.Get().Get(macro.MacrosCRName) + "-common-configd",

	// patternConfigMapCommonUsersName is a template of common users settings for the CHI ConfigMap. "chi-{chi}-common-usersd"
	patternConfigMapCommonUsersName: "chi-" + macrosList.Get().Get(macro.MacrosCRName) + "-common-usersd",

	// patternConfigMapHostName is a template of macros ConfigMap. "chi-{chi}-deploy-confd-{cluster}-{shard}-{host}"
	patternConfigMapHostName: "chi-" + macrosList.Get().Get(macro.MacrosCRName) + "-deploy-confd-" + macrosList.Get().Get(macro.MacrosClusterName) + "-" + macrosList.Get().Get(macro.MacrosHostName),

	// patternCRServiceName is a template of Custom Resource Service name. "clickhouse-{chi}"
	patternCRServiceName: "clickhouse-" + macrosList.Get().Get(macro.MacrosCRName),

	// patternClusterServiceName is a template of cluster Service name. "cluster-{chi}-{cluster}"
	patternClusterServiceName: "cluster-" + macrosList.Get().Get(macro.MacrosCRName) + "-" + macrosList.Get().Get(macro.MacrosClusterName),

	// patternShardServiceName is a template of shard Service name. "shard-{chi}-{cluster}-{shard}"
	patternShardServiceName: "shard-" + macrosList.Get().Get(macro.MacrosCRName) + "-" + macrosList.Get().Get(macro.MacrosClusterName) + "-" + macrosList.Get().Get(macro.MacrosShardName),

	// patternReplicaServiceName is a template of replica Service name. "shard-{chi}-{cluster}-{replica}"
	patternReplicaServiceName: "shard-" + macrosList.Get().Get(macro.MacrosCRName) + "-" + macrosList.Get().Get(macro.MacrosClusterName) + "-" + macrosList.Get().Get(macro.MacrosReplicaName),

	// patternStatefulSetName is a template of host StatefulSet's name. "chi-{chi}-{cluster}-{shard}-{host}"
	patternStatefulSetName: "chi-" + macrosList.Get().Get(macro.MacrosCRName) + "-" + macrosList.Get().Get(macro.MacrosClusterName) + "-" + macrosList.Get().Get(macro.MacrosHostName),

	// patternStatefulSetServiceName is a template of host StatefulSet's Service name. "chi-{chi}-{cluster}-{shard}-{host}"
	patternStatefulSetServiceName: "chi-" + macrosList.Get().Get(macro.MacrosCRName) + "-" + macrosList.Get().Get(macro.MacrosClusterName) + "-" + macrosList.Get().Get(macro.MacrosHostName),

	// patternClusterPDBName is a template of cluster scope PDB. "chi-{chi}-{cluster}"
	patternClusterPDBName: "chi-" + macrosList.Get().Get(macro.MacrosCRName) + "-" + macrosList.Get().Get(macro.MacrosClusterName),
}

const (
	// patternPodName is a name of a Pod within StatefulSet. In our setup each StatefulSet has only 1 pod,
	// so all pods would have '-0' suffix after StatefulSet name
	// Ex.: <StatefulSetName>-0
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
