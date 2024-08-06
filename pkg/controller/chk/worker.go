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

package chk

import (
	"time"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/common"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/poller"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/statefulset"
	"github.com/altinity/clickhouse-operator/pkg/controller/common/storage"
	chkConfig "github.com/altinity/clickhouse-operator/pkg/model/chk/config"
	chkNormalizer "github.com/altinity/clickhouse-operator/pkg/model/chk/normalizer"
	commonCreator "github.com/altinity/clickhouse-operator/pkg/model/common/creator"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

// worker represents worker thread which runs reconcile tasks
type worker struct {
	c             *Controller
	a             common.Announcer
	normalizer    *chkNormalizer.Normalizer
	task          *common.Task
	stsReconciler *statefulset.StatefulSetReconciler

	start time.Time
}

const componentName = "clickhouse-operator"

// newWorker
func (c *Controller) newWorker() *worker {
	start := time.Now()
	//kind := "ClickHouseKeeperInstallation"
	//generateName := "chop-chk-"
	//component := componentName

	announcer := common.NewAnnouncer(
		//common.NewEventEmitter(c.kube.Event(), kind, generateName, component),
		nil,
		c.kube.CRStatus(),
	)

	return &worker{
		c:          c,
		a:          announcer,
		normalizer: chkNormalizer.New(),
		start:      start,
		task:       nil,
	}
}

func (w *worker) newTask(chk *apiChk.ClickHouseKeeperInstallation) {
	w.task = common.NewTask(
		commonCreator.NewCreator(
			chk,
			managers.NewConfigFilesGenerator(managers.FilesGeneratorTypeKeeper, chk, configGeneratorOptions(chk)),
			managers.NewContainerManager(managers.ContainerManagerTypeKeeper),
			managers.NewTagManager(managers.TagManagerTypeKeeper, chk),
			managers.NewProbeManager(managers.ProbeManagerTypeKeeper),
			managers.NewServiceManager(managers.ServiceManagerTypeKeeper),
			managers.NewVolumeManager(managers.VolumeManagerTypeKeeper),
			managers.NewConfigMapManager(managers.ConfigMapManagerTypeKeeper),
			managers.NewNameManager(managers.NameManagerTypeKeeper),
		),
	)

	w.stsReconciler = statefulset.NewStatefulSetReconciler(
		w.a,
		w.task,
		//poller.NewHostStatefulSetPoller(poller.NewStatefulSetPoller(w.c.kube), w.c.kube, w.c.labeler),
		poller.NewHostStatefulSetPoller(poller.NewStatefulSetPoller(w.c.kube), w.c.kube, nil),
		w.c.namer,
		storage.NewStorageReconciler(w.task, w.c.namer, w.c.kube.Storage()),
		w.c.kube,
		statefulset.NewDefaultFallback(),
	)
}

// normalize
func (w *worker) normalize(_chk *apiChk.ClickHouseKeeperInstallation) *apiChk.ClickHouseKeeperInstallation {
	chk, err := chkNormalizer.New().CreateTemplated(_chk, commonNormalizer.NewOptions())
	if err != nil {
		log.V(1).
			M(chk).F().
			Error("FAILED to normalize CHK: %v", err)
	}
	return chk
}

func configGeneratorOptions(chk *apiChk.ClickHouseKeeperInstallation) *chkConfig.GeneratorOptions {
	return &chkConfig.GeneratorOptions{
		RaftPort:      chk.GetSpecT().GetRaftPort(),
		ReplicasCount: 1,
	}
}
