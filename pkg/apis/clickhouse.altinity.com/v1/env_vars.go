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

// +k8s:deepcopy-gen=package,register
// +groupName=clickhouse.altinity.com

// Package v1 defines version 1 of the API used with ClickHouse Installation Custom Resources.
package v1

const (
	// spec.nodeName: ip-172-20-52-62.ec2.internal
	OPERATOR_POD_NODE_NAME = "OPERATOR_POD_NODE_NAME"
	// metadata.name: clickhouse-operator-6f87589dbb-ftcsf
	OPERATOR_POD_NAME = "OPERATOR_POD_NAME"
	// metadata.namespace: kube-system
	OPERATOR_POD_NAMESPACE = "OPERATOR_POD_NAMESPACE"
	// status.podIP: 100.96.3.2
	OPERATOR_POD_IP = "OPERATOR_POD_IP"
	// spec.serviceAccount: clickhouse-operator
	// spec.serviceAccountName: clickhouse-operator
	OPERATOR_POD_SERVICE_ACCOUNT = "OPERATOR_POD_SERVICE_ACCOUNT"

	// .containers.resources.requests.cpu
	OPERATOR_CONTAINER_CPU_REQUEST = "OPERATOR_CONTAINER_CPU_REQUEST"
	// .containers.resources.limits.cpu
	OPERATOR_CONTAINER_CPU_LIMIT = "OPERATOR_CONTAINER_CPU_LIMIT"
	// .containers.resources.requests.memory
	OPERATOR_CONTAINER_MEM_REQUEST = "OPERATOR_CONTAINER_MEM_REQUEST"
	// .containers.resources.limits.memory
	OPERATOR_CONTAINER_MEM_LIMIT = "OPERATOR_CONTAINER_MEM_LIMIT"

	// What namespaces to watch
	WATCH_NAMESPACE  = "WATCH_NAMESPACE"
	WATCH_NAMESPACES = "WATCH_NAMESPACES"

	CHOP_CONFIG = "CHOP_CONFIG"
)
