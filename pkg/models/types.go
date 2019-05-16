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

package models

import (
	"github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

// ConfigMapList defines a list of the ConfigMap objects
type ConfigMapList []*corev1.ConfigMap

// StatefulSetList defines a list of the StatefulSet objects
type StatefulSetList []*apps.StatefulSet

// ServiceList defines a list of the Service objects
type ServiceList []*corev1.Service

type configSections struct {
	// commonConfigSections maps section name to section XML chopConfig
	commonConfigSections map[string]string

	// commonUsersConfigSections maps section name to section XML chopConfig
	commonUsersConfigSections map[string]string
}

// volumeClaimTemplatesIndex maps volume claim template name - which
// is .spec.templates.volumeClaimTemplates.name to VolumeClaimTemplate itself
// Used to provide dictionary/index for templates
type volumeClaimTemplatesIndex map[string]*v1.ChiVolumeClaimTemplate

// podTemplatesIndex maps pod template name - which
// is .spec.templates.podTemplates.name to PodTemplate itself
// Used to provide dictionary/index for templates
type podTemplatesIndex map[string]*v1.ChiPodTemplate
