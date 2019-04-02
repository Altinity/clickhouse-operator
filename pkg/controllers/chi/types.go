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
	"github.com/altinity/clickhouse-operator/pkg/config"
	"time"

	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopmetrics "github.com/altinity/clickhouse-operator/pkg/apis/metrics"
	chopclientset "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	choplisters "github.com/altinity/clickhouse-operator/pkg/client/listers/clickhouse.altinity.com/v1"
	kube "k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

// Controller defines CRO controller
type Controller struct {
	chopConfig              *config.Config
	kubeClient              kube.Interface
	chopClient              chopclientset.Interface
	chiLister               choplisters.ClickHouseInstallationLister
	chiListerSynced         cache.InformerSynced
	serviceLister           corelisters.ServiceLister
	serviceListerSynced     cache.InformerSynced
	configMapLister         corelisters.ConfigMapLister
	configMapListerSynced   cache.InformerSynced
	statefulSetLister       appslisters.StatefulSetLister
	statefulSetListerSynced cache.InformerSynced
	podLister               corelisters.PodLister
	podListerSynced         cache.InformerSynced
	queue                   workqueue.RateLimitingInterface
	recorder                record.EventRecorder
	metricsExporter         *chopmetrics.Exporter
}

const (
	componentName   = "clickhouse-operator"
	runWorkerPeriod = time.Second
)

const (
	successSynced         = "Synced"
	errResourceExists     = "ErrResourceExists"
	messageResourceSynced = "ClickHouseInstallation synced successfully"
	messageResourceExists = "Resource %q already exists and is not managed by ClickHouseInstallation"
	messageUnableToDecode = "unable to decode object (invalid type)"
	messageUnableToSync   = "unable to sync caches for %s controller"
)

const (
	reconcileAdd    = "add"
	reconcileUpdate = "update"
	reconcileDelete = "delete"
)

type ReconcileChi struct {
	cmd string
	old *chop.ClickHouseInstallation
	new *chop.ClickHouseInstallation
}

func NewReconcileChi(cmd string, old, new *chop.ClickHouseInstallation) *ReconcileChi {
	return &ReconcileChi{
		cmd: cmd,
		old: old,
		new: new,
	}
}
