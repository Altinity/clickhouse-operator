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
	// fullDeploymentIDToFingerprint[fullDeploymentID] = fingerprint
	fullDeploymentIDToFingerprint map[string]string

	// ssDeployments[fingerprint] = &replica.Deployment
	ssDeployments map[string]*chiv1.ChiDeployment

	// chiDeploymentCountMap struct with max values from all clusters
	deploymentCountMax chiDeploymentCountMap
	macrosData         map[string]macrosDataShardDescriptionList

	// configSection specifies whether additional config files (such as zookeeper, macros) are configuared
	configSection map[string]bool
}

type includesObjects []struct {
	filename string
	fullpath string
}

// macrosDataShardDescriptionList is a list of all shards descriptions
type macrosDataShardDescriptionList []*macrosDataShardDescription

// macrosDataShardDescription is used in generating macros config file
// and describes which cluster this shard belongs to and
// index (1-based, human-friendly and XML-config usable) of this shard within cluster
// Used to build this:
//<yandex>
//    <macros>
//        <installation>example-02</installation>
//        <2shard-1repl>example-02-2shard-1repl</02-2shard-1repl>
//        <2shard-1repl-shard>1 [OWN UNIQUE SHARD ID]</02-2shard-1repl-shard>
//        <replica>1eb454-1 [OWN UNIQUE ID]</replica>
//    </macros>
//</yandex>
type macrosDataShardDescription struct {
	clusterName string
	index       int
}

// chiDeploymentCountMap maps Deployment fingerprint to its usage count
type chiDeploymentCountMap map[string]int

// mergeInAndReplaceBiggerValues combines chiDeploymentCountMap object with another one
// and replaces local values with another one's values in case another's value is bigger
func (d chiDeploymentCountMap) mergeInAndReplaceBiggerValues(another chiDeploymentCountMap) {

	// Loop over another struct and bring in new OR bigger values
	for key, value := range another {
		_, ok := d[key]

		if !ok {
			// No such key - new key/value pair just - include/merge it in
			d[key] = value
		} else if value > d[key] {
			// Have such key, but "another"'s value is bigger, overwrite local value
			d[key] = value
		}
	}
}

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
