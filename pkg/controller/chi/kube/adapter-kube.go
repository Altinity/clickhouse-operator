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

type Adapter struct {
	kubeClient kube.Interface
	namer      interfaces.INameManager

	// Set of CR k8s components

	cr *CR

	// Set of k8s components

	configMap  *ConfigMap
	deployment *Deployment
	event      *Event
	pdb        *PDB
	pod        *Pod
	pvc        *storage.PVC
	replicaSet *ReplicaSet
	secret     *Secret
	service    *Service
	sts        *STS
}

func NewAdapter(kubeClient kube.Interface, chopClient chopClientSet.Interface, namer interfaces.INameManager) *Adapter {
	return &Adapter{
		kubeClient: kubeClient,
		namer:      namer,

		cr: NewCR(chopClient, kubeClient),

		configMap:  NewConfigMap(kubeClient),
		deployment: NewDeployment(kubeClient),
		event:      NewEvent(kubeClient),
		pdb:        NewPDB(kubeClient),
		pod:        NewPod(kubeClient, namer),
		pvc:        storage.NewStoragePVC(NewPVC(kubeClient)),
		replicaSet: NewReplicaSet(kubeClient),
		secret:     NewSecret(kubeClient, namer),
		service:    NewService(kubeClient, namer),
		sts:        NewSTS(kubeClient, namer),
	}
}

// CR is a getter
func (k *Adapter) CR() interfaces.IKubeCR {
	return k.cr
}

// ConfigMap is a getter
func (k *Adapter) ConfigMap() interfaces.IKubeConfigMap {
	return k.configMap
}

// Deployment is a getter
func (k *Adapter) Deployment() interfaces.IKubeDeployment {
	return k.deployment
}

// Event is a getter
func (k *Adapter) Event() interfaces.IKubeEvent {
	return k.event
}

// PDB is a getter
func (k *Adapter) PDB() interfaces.IKubePDB {
	return k.pdb
}

// Pod is a getter
func (k *Adapter) Pod() interfaces.IKubePod {
	return k.pod
}

// Storage is a getter
func (k *Adapter) Storage() interfaces.IKubeStoragePVC {
	return k.pvc
}

// ReplicaSet is a getter
func (k *Adapter) ReplicaSet() interfaces.IKubeReplicaSet {
	return k.replicaSet
}

// Secret is a getter
func (k *Adapter) Secret() interfaces.IKubeSecret {
	return k.secret
}

// Service is a getter
func (k *Adapter) Service() interfaces.IKubeService {
	return k.service
}

// STS is a getter
func (k *Adapter) STS() interfaces.IKubeSTS {
	return k.sts
}
