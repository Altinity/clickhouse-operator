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

package common

import (
	"context"
	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
	"time"

	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model"
)

// task represents context of a worker. This also can be called "a reconcile task"
type Task struct {
	creatorNew         interfaces.ICreator
	creatorOld         interfaces.ICreator
	registryReconciled *model.Registry
	registryFailed     *model.Registry
	cmUpdate           time.Time
	start              time.Time
}

// NewTask creates new context
func NewTask(creatorNew, creatorOld interfaces.ICreator) *Task {
	return &Task{
		creatorNew:         creatorNew,
		creatorOld:         creatorOld,
		registryReconciled: model.NewRegistry(),
		registryFailed:     model.NewRegistry(),
		cmUpdate:           time.Time{},
		start:              time.Now(),
	}
}

func (t *Task) Creator() interfaces.ICreator {
	return t.creatorNew
}

func (t *Task) CreatorPrev() interfaces.ICreator {
	return t.creatorOld
}

func (t *Task) RegistryReconciled() *model.Registry {
	return t.registryReconciled
}

func (t *Task) RegistryFailed() *model.Registry {
	return t.registryFailed
}

func (t *Task) CmUpdate() time.Time {
	return t.cmUpdate
}

func (t *Task) SetCmUpdate(update time.Time) {
	t.cmUpdate = update
}

func (t *Task) WaitForConfigMapPropagation(ctx context.Context, host *api.Host) bool {
	// No need to wait for ConfigMap propagation on stopped host
	if host.IsStopped() {
		log.V(1).M(host).F().Info("No need to wait for ConfigMap propagation - host is stopped")
		return false
	}

	// No need to wait on unchanged ConfigMap
	if t.CmUpdate().IsZero() {
		log.V(1).M(host).F().Info("No need to wait for ConfigMap propagation - no changes in ConfigMap")
		return false
	}

	// What timeout is expected to be enough for ConfigMap propagation?
	// In case timeout is not specified, no need to wait
	if !host.GetCR().GetReconciling().HasConfigMapPropagationTimeout() {
		log.V(1).M(host).F().Info("No need to wait for ConfigMap propagation - not applicable due to missing timeout value")
		return false
	}

	timeout := host.GetCR().GetReconciling().GetConfigMapPropagationTimeoutDuration()

	// How much time has elapsed since last ConfigMap update?
	// May be there is no need to wait already
	elapsed := time.Now().Sub(t.CmUpdate())
	if elapsed >= timeout {
		log.V(1).M(host).F().Info("No need to wait for ConfigMap propagation - already elapsed. [elapsed/timeout: %s/%s]", elapsed, timeout)
		return false
	}

	// Looks like we need to wait for Configmap propagation, after all
	wait := timeout - elapsed
	log.V(1).M(host).F().Info("Going to wait for ConfigMap propagation for: %s [elapsed/timeout: %s/%s]", wait, elapsed, timeout)
	if util.WaitContextDoneOrTimeout(ctx, wait) {
		log.V(2).Info("task is done")
		return true
	}

	log.V(1).M(host).F().Info("Wait completed for: %s  of timeout: %s]", wait, timeout)
	return false
}
