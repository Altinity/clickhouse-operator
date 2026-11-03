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

type Adapter struct {

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

// NewAdapter wires the two readers this package uses, and the split between them is the whole
// contract: LIST THROUGH THE CACHE, GET LIVE.
//
// The manager's cache is narrowed by label (see newKeeperCacheOptions), which is where the memory
// saving comes from - the informers hold only operator-generated objects. The cost of a filter is
// that a cached Get for an object outside it returns IsNotFound, indistinguishable from real
// absence. Acting on absence is a hazard the cache did not create - the ClickHouse controller has
// always read live and has the same fault - but a filter turns it from "the object is gone" into
// "the object is unlabelled", which is far easier to reach. These paths act on absence: a PVC read as missing drives a StatefulSet
// recreate, a ConfigMap or Service read as missing drives a create that then fails AlreadyExists.
// So every by-name Get of a NARROWED type goes to apiReader, which consults the API server
// directly. Three types read cached and are deliberately not narrowed, so no by-name Get of them
// can be filtered away: the ClickHouseKeeperInstallation, which is user-authored, carries no
// operator label, and is the reason the filter is per-type rather than cache-wide; and Deployment
// and ReplicaSet, which have Get methods here but no caller on the Keeper path at all.
//
// Lists stay on the cache. Every one is issued by the discoverer with SelectorCRScope, which
// constrains the same label plus namespace and CR name, so the cache filter is a strict superset
// and cannot change a List result. PVC.ListForHost reads live as well, but only for consistency
// with the Get beside it - its host-scoped selector already carries the same label pair, so the
// filter could not have changed its result either.
//
// StatefulSet had its own reason to read live before any of this. The cache is not write-through:
// Create and Update go straight to the API server and never touch it, and it catches up only when
// the watch delivers the change - so a Get immediately after an Update can still return the
// pre-update object and mislead a fingerprint comparison. Both reasons now apply to every
// narrowed type.
func NewAdapter(kubeClient client.Client, apiReader client.Reader, namer interfaces.INameManager) *Adapter {
	return &Adapter{
		cr: NewCR(kubeClient),

		configMap:  NewConfigMap(kubeClient, apiReader),
		deployment: NewDeployment(kubeClient),
		event:      NewEvent(kubeClient),
		pdb:        NewPDB(kubeClient, apiReader),
		pod:        NewPod(kubeClient, apiReader, namer),
		pvc:        storage.NewStoragePVC(NewPVC(kubeClient, apiReader)),
		replicaSet: NewReplicaSet(kubeClient),
		secret:     NewSecret(kubeClient, apiReader, namer),
		service:    NewService(kubeClient, apiReader, namer),
		sts:        NewSTS(kubeClient, apiReader, namer),
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
