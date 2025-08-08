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

const (
	// patternConfigMapCommonName is a template of common settings for the CHI ConfigMap. "chi-{chi}-common-configd"
	patternConfigMapCommonName = "chk- + macro.List.Get(macroCommon.MacrosCRName) + -common-configd"

	// patternConfigMapCommonUsersName is a template of common users settings for the CHI ConfigMap. "chi-{chi}-common-usersd"
	patternConfigMapCommonUsersName = "chk- + macro.List.Get(macroCommon.MacrosCRName) + -common-usersd"

	// patternConfigMapHostName is a template of macros ConfigMap. "chi-{chi}-deploy-confd-{cluster}-{shard}-{host}"
	patternConfigMapHostName = "chk- + macro.MacrosCRName + -deploy-confd- + macro.MacrosClusterName + - + macro.MacrosHostName"

	// patternCRServiceName is a template of Custom Resource Service name. "clickhouse-{chi}"
	patternCRServiceName = "keeper- + macro.MacrosCRName"

	// patternClusterServiceName is a template of cluster Service name. "cluster-{chi}-{cluster}"
	patternClusterServiceName = "cluster- + macro.MacrosCRName + - + macro.MacrosClusterName"

	// patternShardServiceName is a template of shard Service name. "shard-{chi}-{cluster}-{shard}"
	patternShardServiceName = "shard- + macro.MacrosCRName + - + macro.MacrosClusterName + - + macro.MacrosShardName"

	// patternReplicaServiceName is a template of replica Service name. "shard-{chi}-{cluster}-{replica}"
	patternReplicaServiceName = "shard- + macro.MacrosCRName + - + macro.MacrosClusterName + - + macro.MacrosReplicaName"

	// patternStatefulSetName is a template of host StatefulSet's name. "chi-{chi}-{cluster}-{shard}-{host}"
	patternStatefulSetName = "sts chk- + macro.MacrosCRName + - + macro.MacrosClusterName + - + macro.MacrosHostName"

	// patternStatefulSetServiceName is a template of host StatefulSet's Service name. "chi-{chi}-{cluster}-{shard}-{host}"
	patternStatefulSetServiceName = "service chk- + macro.MacrosCRName + - + macro.MacrosClusterName + - + macro.MacrosHostName"

	// patternClusterPDBName is a template of cluster scope PDB. "chi-{chi}-{cluster}"
	patternClusterPDBName = "pdb chk- + macrosList.Get().Get(macro.MacrosCRName) + - + macrosList.Get().Get(macro.MacrosClusterName)"
)
