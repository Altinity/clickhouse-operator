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
	"time"

	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model"
)

// task represents context of a worker. This also can be called "a reconcile task"
type Task struct {
	creator            interfaces.ICreator
	registryReconciled *model.Registry
	registryFailed     *model.Registry
	cmUpdate           time.Time
	start              time.Time
}

// NewTask creates new context
func NewTask(creator interfaces.ICreator) *Task {
	return &Task{
		creator:            creator,
		registryReconciled: model.NewRegistry(),
		registryFailed:     model.NewRegistry(),
		cmUpdate:           time.Time{},
		start:              time.Now(),
	}
}

func (t *Task) Creator() interfaces.ICreator {
	return t.creator
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
