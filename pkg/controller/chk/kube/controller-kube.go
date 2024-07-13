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
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/altinity/clickhouse-operator/pkg/controller/common/storage"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

type Keeper struct {
	kubeClient client.Client
	namer      interfaces.INameManager

	// Set of k8s components

	deployment *DeploymentKeeper
	event      *EventKeeper
	pod        *PodKeeper
	replicaSet *ReplicaSetKeeper
	service    *ServiceKeeper
	sts        *STSKeeper
	crStatus   *CRStatusKeeper
	pvc        *storage.PVC
}

func NewKeeper(kubeClient client.Client, namer interfaces.INameManager) *Keeper {
	return &Keeper{
		kubeClient: kubeClient,
		namer:      namer,

		// Set of k8s components

		deployment: NewDeploymentKeeper(kubeClient),
		event:      NewEventKeeper(kubeClient),
		pod:        NewPodKeeper(kubeClient, namer),
		replicaSet: NewReplicaSetKeeper(kubeClient),
		service:    NewServiceKeeper(kubeClient, namer),
		sts:        NewSTSKeeper(kubeClient, namer),
		crStatus:   NewCRStatusKeeper(kubeClient),
		pvc:        storage.NewStoragePVC(NewPVCKeeper(kubeClient)),
	}
}

// Deployment is a getter
func (k *Keeper) Deployment() interfaces.IKubeDeployment {
	return k.deployment
}

// Event is a getter
func (k *Keeper) Event() interfaces.IKubeEvent {
	return k.event
}

// Pod is a getter
func (k *Keeper) Pod() interfaces.IKubePod {
	return k.pod
}

// ReplicaSet is a getter
func (k *Keeper) ReplicaSet() interfaces.IKubeReplicaSet {
	return k.replicaSet
}

// Service is a getter
func (k *Keeper) Service() interfaces.IKubeService {
	return k.service
}

// STS is a getter
func (k *Keeper) STS() interfaces.IKubeSTS {
	return k.sts
}

// CRStatus is a getter
func (k *Keeper) CRStatus() interfaces.IKubeCRStatus {
	return k.crStatus
}

// Storage is a getter
func (k *Keeper) Storage() interfaces.IKubeStoragePVC {
	return k.pvc
}
