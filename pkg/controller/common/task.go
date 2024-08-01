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
	Creator            interfaces.ICreator
	RegistryReconciled *model.Registry
	RegistryFailed     *model.Registry
	CmUpdate           time.Time
	Start              time.Time
}

// NewTask creates new context
func NewTask(creator interfaces.ICreator) *Task {
	return &Task{
		Creator:            creator,
		RegistryReconciled: model.NewRegistry(),
		RegistryFailed:     model.NewRegistry(),
		CmUpdate:           time.Time{},
		Start:              time.Now(),
	}
}

func (t *Task) GetCreator() interfaces.ICreator {
	return t.Creator
}
