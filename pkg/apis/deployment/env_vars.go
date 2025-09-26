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

package deployment

const (
	// OPERATOR_POD_NODE_NAME name from spec.nodeName Ex.: ip-172-20-52-62.ec2.internal
	OPERATOR_POD_NODE_NAME = "OPERATOR_POD_NODE_NAME"
	// OPERATOR_POD_NAME name from metadata.name Ex.: clickhouse-operator-6f87589dbb-ftcsf
	OPERATOR_POD_NAME = "OPERATOR_POD_NAME"
	// OPERATOR_POD_NAMESPACE name from metadata.namespace Ex.: kube-system
	OPERATOR_POD_NAMESPACE = "OPERATOR_POD_NAMESPACE"
	// OPERATOR_POD_IP ip from status.podIP Ex.: 100.96.3.2
	OPERATOR_POD_IP = "OPERATOR_POD_IP"
	// OPERATOR_POD_SERVICE_ACCOUNT service account
	// from
	// spec.serviceAccount
	// spec.serviceAccountName
	// Ex.: clickhouse-operator
	OPERATOR_POD_SERVICE_ACCOUNT = "OPERATOR_POD_SERVICE_ACCOUNT"

	// OPERATOR_CONTAINER_CPU_REQUEST from .containers.resources.requests.cpu
	OPERATOR_CONTAINER_CPU_REQUEST = "OPERATOR_CONTAINER_CPU_REQUEST"
	// OPERATOR_CONTAINER_CPU_LIMIT from .containers.resources.limits.cpu
	OPERATOR_CONTAINER_CPU_LIMIT = "OPERATOR_CONTAINER_CPU_LIMIT"
	// OPERATOR_CONTAINER_MEM_REQUEST from .containers.resources.requests.memory
	OPERATOR_CONTAINER_MEM_REQUEST = "OPERATOR_CONTAINER_MEM_REQUEST"
	// OPERATOR_CONTAINER_MEM_LIMIT from .containers.resources.limits.memory
	OPERATOR_CONTAINER_MEM_LIMIT = "OPERATOR_CONTAINER_MEM_LIMIT"

	// OPERATOR_K8S_CLIENT_QPS_LIMIT specifies an override for the default k8s client QPS rate limit.
	OPERATOR_K8S_CLIENT_QPS_LIMIT = "OPERATOR_K8S_CLIENT_QPS_LIMIT"
	// OPERATOR_K8S_CLIENT_BURST_LIMIT specifies an override for the default k8s client QPS burst limit.
	OPERATOR_K8S_CLIENT_BURST_LIMIT = "OPERATOR_K8S_CLIENT_BURST_LIMIT"

	// WATCH_NAMESPACE and WATCH_NAMESPACES specifies what namespaces to watch
	WATCH_NAMESPACE = "WATCH_NAMESPACE"
	// WATCH_NAMESPACES and WATCH_NAMESPACE specifies what namespaces to watch
	WATCH_NAMESPACES = "WATCH_NAMESPACES"
	// WATCH_NAMESPACES_EXCLUDE specifies namespaces that should be excluded from reconciliation
	WATCH_NAMESPACES_EXCLUDE = "WATCH_NAMESPACES_EXCLUDE"

	// CHOP_CONFIG path to clickhouse operator configuration file
	CHOP_CONFIG = "CHOP_CONFIG"
)
