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

package kube

import (
	kube "k8s.io/client-go/kubernetes"

	chopClientSet "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	"github.com/altinity/clickhouse-operator/pkg/model/common/interfaces"
)

type KubeClickHouse struct {
	kubeClient kube.Interface
	namer      interfaces.INameManager

	deployment *KubeDeploymentClickHouse
	event      *KubeEventClickHouse
	pod        *KubePodClickHouse
	replicaSet *KubeReplicaSetClickHouse
	service    *KubeServiceClickHouse
	sts        *KubeSTSClickHouse
	crStatus   *KubeCRStatusClickHouse
	storage    *KubePVCClickHouse
}

func NewKubeClickHouse(kubeClient kube.Interface, chopClient chopClientSet.Interface, namer interfaces.INameManager) *KubeClickHouse {
	return &KubeClickHouse{
		kubeClient: kubeClient,
		namer:      namer,

		deployment: NewKubeDeploymentClickHouse(kubeClient, namer),
		event:      NewKubeEventClickHouse(kubeClient),
		pod:        NewKubePodClickHouse(kubeClient, namer),
		replicaSet: NewKubeReplicaSetClickHouse(kubeClient, namer),
		service:    NewKubeServiceClickHouse(kubeClient, namer),
		sts:        NewKubeSTSClickHouse(kubeClient, namer),
		crStatus:   NewKubeCRStatusClickHouse(chopClient),
		storage:    NewKubePVCClickHouse(kubeClient),
	}
}

func (k *KubeClickHouse) Deployment() interfaces.IKubeDeployment {
	return k.deployment
}
func (k *KubeClickHouse) Event() interfaces.IKubeEvent {
	return k.event
}
func (k *KubeClickHouse) Pod() interfaces.IKubePod {
	return k.pod
}
func (k *KubeClickHouse) ReplicaSet() interfaces.IKubeReplicaSet {
	return k.replicaSet
}
func (k *KubeClickHouse) Service() interfaces.IKubeService {
	return k.service
}
func (k *KubeClickHouse) STS() interfaces.IKubeSTS {
	return k.sts
}
func (k *KubeClickHouse) CRStatus() interfaces.IKubeCRStatus {
	return k.crStatus
}
func (k *KubeClickHouse) Storage() interfaces.IKubePVC {
	return k.storage
}
