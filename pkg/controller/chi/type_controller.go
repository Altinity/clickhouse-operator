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

package chi

import (
	"time"

	kube "k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	//"k8s.io/client-go/util/workqueue"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"

	"github.com/altinity/queue"

	chopclientset "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	choplisters "github.com/altinity/clickhouse-operator/pkg/client/listers/clickhouse.altinity.com/v1"
)

// Controller defines CRO controller
type Controller struct {
	// kubeClient used to Create() k8s resources as c.kubeClient.AppsV1().StatefulSets(namespace).Create(name)
	kubeClient kube.Interface
	extClient  apiextensions.Interface
	// chopClient used to Update() CRD k8s resource as c.chopClient.ClickhouseV1().ClickHouseInstallations(chi.Namespace).Update(chiCopy)
	chopClient chopclientset.Interface

	// chiLister used as chiLister.ClickHouseInstallations(namespace).Get(name)
	chiLister choplisters.ClickHouseInstallationLister
	// chiListerSynced used in waitForCacheSync()
	chiListerSynced cache.InformerSynced

	chitLister       choplisters.ClickHouseInstallationTemplateLister
	chitListerSynced cache.InformerSynced

	// serviceLister used as serviceLister.Services(namespace).Get(name)
	serviceLister corelisters.ServiceLister
	// serviceListerSynced used in waitForCacheSync()
	serviceListerSynced cache.InformerSynced
	// endpointsLister used as endpointsLister.Endpoints(namespace).Get(name)
	endpointsLister corelisters.EndpointsLister
	// endpointsListerSynced used in waitForCacheSync()
	endpointsListerSynced cache.InformerSynced
	// configMapLister used as configMapLister.ConfigMaps(namespace).Get(name)
	configMapLister corelisters.ConfigMapLister
	// configMapListerSynced used in waitForCacheSync()
	configMapListerSynced cache.InformerSynced
	// statefulSetLister used as statefulSetLister.StatefulSets(namespace).Get(name)
	statefulSetLister appslisters.StatefulSetLister
	// statefulSetListerSynced used in waitForCacheSync()
	statefulSetListerSynced cache.InformerSynced
	// podLister used as statefulSetLister.StatefulSets(namespace).Get(name)
	podLister corelisters.PodLister
	// podListerSynced used in waitForCacheSync()
	podListerSynced cache.InformerSynced

	// queues used to organize events queue processed by operator
	queues []queue.PriorityQueue
	// not used explicitly
	recorder record.EventRecorder
}

const (
	componentName   = "clickhouse-operator"
	runWorkerPeriod = time.Second
)

const (
	messageUnableToDecode = "unable to decode object (invalid type)"
	messageUnableToSync   = "unable to sync caches for %s controller"
)
