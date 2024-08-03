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
	"github.com/altinity/clickhouse-operator/pkg/controller/common/storage"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

type ClickHouse struct {
	kubeClient kube.Interface
	namer      interfaces.INameManager

	deployment *DeploymentClickHouse
	event      *EventClickHouse
	pod        *PodClickHouse
	replicaSet *ReplicaSetClickHouse
	service    *ServiceClickHouse
	sts        *STSClickHouse
	crStatus   *CRStatusClickHouse
	pvc        *storage.PVC
}

func NewClickHouse(kubeClient kube.Interface, chopClient chopClientSet.Interface, namer interfaces.INameManager) *ClickHouse {
	return &ClickHouse{
		kubeClient: kubeClient,
		namer:      namer,

		deployment: NewDeploymentClickHouse(kubeClient),
		event:      NewEventClickHouse(kubeClient),
		pod:        NewPodClickHouse(kubeClient, namer),
		replicaSet: NewReplicaSetClickHouse(kubeClient),
		service:    NewServiceClickHouse(kubeClient, namer),
		sts:        NewSTSClickHouse(kubeClient, namer),
		crStatus:   NewCRStatusClickHouse(chopClient),
		pvc:        storage.NewStoragePVC(NewPVCClickHouse(kubeClient)),
	}
}

func (k *ClickHouse) Deployment() interfaces.IKubeDeployment {
	return k.deployment
}
func (k *ClickHouse) Event() interfaces.IKubeEvent {
	return k.event
}
func (k *ClickHouse) Pod() interfaces.IKubePod {
	return k.pod
}
func (k *ClickHouse) ReplicaSet() interfaces.IKubeReplicaSet {
	return k.replicaSet
}
func (k *ClickHouse) Service() interfaces.IKubeService {
	return k.service
}
func (k *ClickHouse) STS() interfaces.IKubeSTS {
	return k.sts
}
func (k *ClickHouse) CRStatus() interfaces.IKubeCRStatus {
	return k.crStatus
}
func (k *ClickHouse) Storage() interfaces.IKubeStoragePVC {
	return k.pvc
}
