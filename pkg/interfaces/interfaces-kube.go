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

package interfaces

import (
	"context"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

type IKube interface {
	Deployment() IKubeDeployment
	Event() IKubeEvent
	Pod() IKubePod
	ReplicaSet() IKubeReplicaSet
	Service() IKubeService
	STS() IKubeSTS
	CRStatus() IKubeCRStatus
	Storage() IKubeStoragePVC
}

type IKubePVC interface {
	Create(ctx context.Context, pvc *core.PersistentVolumeClaim) (*core.PersistentVolumeClaim, error)
	Get(ctx context.Context, namespace, name string) (*core.PersistentVolumeClaim, error)
	Update(ctx context.Context, pvc *core.PersistentVolumeClaim) (*core.PersistentVolumeClaim, error)
	Delete(ctx context.Context, namespace, name string) error
	ListForHost(ctx context.Context, host *api.Host) (*core.PersistentVolumeClaimList, error)
}
type IKubeStoragePVC interface {
	IKubePVC
	UpdateOrCreate(ctx context.Context, pvc *core.PersistentVolumeClaim) (*core.PersistentVolumeClaim, error)
}

type IKubeEvent interface {
	Create(ctx context.Context, event *core.Event) (*core.Event, error)
}

type IKubeCRStatus interface {
	Update(ctx context.Context, cr api.ICustomResource, opts UpdateStatusOptions) (err error)
}

type IKubeSTS interface {
	Get(obj any) (*apps.StatefulSet, error)
	Create(statefulSet *apps.StatefulSet) (*apps.StatefulSet, error)
	Update(sts *apps.StatefulSet) (*apps.StatefulSet, error)
	Delete(namespace, name string) error
}

type IKubeService interface {
	Get(obj any) (*core.Service, error)
	Update(svc *core.Service) (*core.Service, error)
}

type IKubePod interface {
	Get(params ...any) (*core.Pod, error)
	GetAll(obj any) []*core.Pod
	Update(ctx context.Context, pod *core.Pod) (*core.Pod, error)
}

type IKubeReplicaSet interface {
	Get(namespace, name string) (*apps.ReplicaSet, error)
	Update(replicaSet *apps.ReplicaSet) (*apps.ReplicaSet, error)
}

type IKubeDeployment interface {
	Get(namespace, name string) (*apps.Deployment, error)
	Update(deployment *apps.Deployment) (*apps.Deployment, error)
}
