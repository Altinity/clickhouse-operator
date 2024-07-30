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

package cmd_queue

import (
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/altinity/queue"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

const (
	ReconcileAdd    = "add"
	ReconcileUpdate = "update"
	ReconcileDelete = "delete"
)

// PriorityQueueItem specifies item of the priority queue
type PriorityQueueItem struct {
	priority int
}

// Priority gets priority of the queue item
func (i PriorityQueueItem) Priority() int {
	return i.priority
}

const (
	priorityReconcileCHI        int = 10
	priorityReconcileCHIT       int = 5
	priorityReconcileChopConfig int = 3
	priorityReconcileEndpoints  int = 15
	priorityDropDNS             int = 7
)

// ReconcileCHI specifies reconcile request queue item
type ReconcileCHI struct {
	PriorityQueueItem
	Cmd string
	Old *api.ClickHouseInstallation
	New *api.ClickHouseInstallation
}

var _ queue.PriorityQueueItem = &ReconcileCHI{}

// Handle returns handle of the queue item
func (r ReconcileCHI) Handle() queue.T {
	if r.New != nil {
		return "ReconcileCHI" + ":" + r.New.Namespace + "/" + r.New.Name
	}
	if r.Old != nil {
		return "ReconcileCHI" + ":" + r.Old.Namespace + "/" + r.Old.Name
	}
	return ""
}

// NewReconcileCHI creates new reconcile request queue item
func NewReconcileCHI(cmd string, old, new *api.ClickHouseInstallation) *ReconcileCHI {
	return &ReconcileCHI{
		PriorityQueueItem: PriorityQueueItem{
			priority: priorityReconcileCHI,
		},
		Cmd: cmd,
		Old: old,
		New: new,
	}

	/*
		if old != nil {
			js, _ := json.Marshal(old)
			_copy := chi.ClickHouseInstallation{}
			json.Unmarshal(js, &_copy)
			c.old = &_copy
		}

		if new != nil {
			js, _ := json.Marshal(new)
			_copy := chi.ClickHouseInstallation{}
			json.Unmarshal(js, &_copy)
			c.new = &_copy
		}

		return c
	*/
}

// ReconcileCHIT specifies reconcile CHI template queue item
type ReconcileCHIT struct {
	PriorityQueueItem
	Cmd string
	Old *api.ClickHouseInstallationTemplate
	New *api.ClickHouseInstallationTemplate
}

var _ queue.PriorityQueueItem = &ReconcileCHIT{}

// Handle returns handle of the queue item
func (r ReconcileCHIT) Handle() queue.T {
	if r.New != nil {
		return "ReconcileCHIT" + ":" + r.New.Namespace + "/" + r.New.Name
	}
	if r.Old != nil {
		return "ReconcileCHIT" + ":" + r.Old.Namespace + "/" + r.Old.Name
	}
	return ""
}

// NewReconcileCHIT creates new reconcile CHI template queue item
func NewReconcileCHIT(cmd string, old, new *api.ClickHouseInstallationTemplate) *ReconcileCHIT {
	return &ReconcileCHIT{
		PriorityQueueItem: PriorityQueueItem{
			priority: priorityReconcileCHIT,
		},
		Cmd: cmd,
		Old: old,
		New: new,
	}
}

// ReconcileChopConfig specifies CHOp config queue item
type ReconcileChopConfig struct {
	PriorityQueueItem
	Cmd string
	Old *api.ClickHouseOperatorConfiguration
	New *api.ClickHouseOperatorConfiguration
}

var _ queue.PriorityQueueItem = &ReconcileChopConfig{}

// Handle returns handle of the queue item
func (r ReconcileChopConfig) Handle() queue.T {
	if r.New != nil {
		return "ReconcileChopConfig" + ":" + r.New.Namespace + "/" + r.New.Name
	}
	if r.Old != nil {
		return "ReconcileChopConfig" + ":" + r.Old.Namespace + "/" + r.Old.Name
	}
	return ""
}

// NewReconcileChopConfig creates new CHOp config queue item
func NewReconcileChopConfig(cmd string, old, new *api.ClickHouseOperatorConfiguration) *ReconcileChopConfig {
	return &ReconcileChopConfig{
		PriorityQueueItem: PriorityQueueItem{
			priority: priorityReconcileChopConfig,
		},
		Cmd: cmd,
		Old: old,
		New: new,
	}
}

// ReconcileEndpoints specifies endpoint
type ReconcileEndpoints struct {
	PriorityQueueItem
	Cmd string
	Old *core.Endpoints
	New *core.Endpoints
}

var _ queue.PriorityQueueItem = &ReconcileEndpoints{}

// Handle returns handle of the queue item
func (r ReconcileEndpoints) Handle() queue.T {
	if r.New != nil {
		return "ReconcileEndpoints" + ":" + r.New.Namespace + "/" + r.New.Name
	}
	if r.Old != nil {
		return "ReconcileEndpoints" + ":" + r.Old.Namespace + "/" + r.Old.Name
	}
	return ""
}

// NewReconcileEndpoints creates new reconcile endpoints queue item
func NewReconcileEndpoints(cmd string, old, new *core.Endpoints) *ReconcileEndpoints {
	return &ReconcileEndpoints{
		PriorityQueueItem: PriorityQueueItem{
			priority: priorityReconcileEndpoints,
		},
		Cmd: cmd,
		Old: old,
		New: new,
	}
}

// DropDns specifies drop dns queue item
type DropDns struct {
	PriorityQueueItem
	Initiator meta.Object
}

var _ queue.PriorityQueueItem = &DropDns{}

// Handle returns handle of the queue item
func (r DropDns) Handle() queue.T {
	if r.Initiator != nil {
		return "DropDNS" + ":" +r.Initiator.GetNamespace() + "/" + r.Initiator.GetName()
	}
	return ""
}

// NewDropDns creates new drop dns queue item
func NewDropDns(initiator meta.Object) *DropDns {
	return &DropDns{
		PriorityQueueItem: PriorityQueueItem{
			priority: priorityDropDNS,
		},
		Initiator: initiator,
	}
}

// ReconcilePod specifies pod reconcile
type ReconcilePod struct {
	PriorityQueueItem
	Cmd string
	Old *core.Pod
	New *core.Pod
}

var _ queue.PriorityQueueItem = &ReconcileEndpoints{}

// Handle returns handle of the queue item
func (r ReconcilePod) Handle() queue.T {
	if r.New != nil {
		return "ReconcilePod" + ":" + r.New.Namespace + "/" + r.New.Name
	}
	if r.Old != nil {
		return "ReconcilePod" + ":" + r.Old.Namespace + "/" + r.Old.Name
	}
	return ""
}

// NewReconcilePod creates new reconcile endpoints queue item
func NewReconcilePod(cmd string, old, new *core.Pod) *ReconcilePod {
	return &ReconcilePod{
		Cmd: cmd,
		Old: old,
		New: new,
	}
}
