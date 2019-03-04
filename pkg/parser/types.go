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
	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"

	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

// ObjectKind defines k8s objects list kind
type ObjectKind uint8

// ObjectsMap defines map of a generated k8s objects
type ObjectsMap map[ObjectKind]interface{}

// ConfigMapList defines a list of the ConfigMap objects
type ConfigMapList []*corev1.ConfigMap

// StatefulSetList defines a list of the StatefulSet objects
type StatefulSetList []*apps.StatefulSet

// ServiceList defines a list of the Service objects
type ServiceList []*corev1.Service

type genOptions struct {
	ssNames            map[string]string
	ssDeployments      map[string]*chiv1.ChiDeployment

	// chiDeploymentCount struct with max values from all clusters
	deploymentCountMax chiDeploymentCount
	macrosDataIndex    map[string]shardsIndex

	// includeConfigSection specifies whether additional config files (such as zookeeper, macros) are configuared
	includeConfigSection map[string]bool
}

type includesObjects []struct {
	filename string
	fullpath string
}

type shardsIndex []*shardsIndexItem

type shardsIndexItem struct {
	cluster string
	index   int
}

type chiClusterAndDeploymentCount struct {
	cluster         *chiv1.ChiCluster
	deploymentCount chiDeploymentCount
}

// chiDeploymentCount maps Deployment fingerprint to its usage count
type chiDeploymentCount map[string]int

type vcTemplatesIndex map[string]*vcTemplatesIndexData

type vcTemplatesIndexData struct {
	useDefaultName bool
	template       *corev1.PersistentVolumeClaim
}

type podTemplatesIndex map[string]*podTemplatesIndexData

type podTemplatesIndexData struct {
	containers []corev1.Container
	volumes    []corev1.Volume
}
