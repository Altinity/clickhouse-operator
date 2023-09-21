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
	coreV1 "k8s.io/api/core/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/altinity/queue"

	chiV1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

const (
	reconcileAdd    = "add"
	reconcileUpdate = "update"
	reconcileDelete = "delete"
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
	cmd string
	old *chiV1.ClickHouseInstallation
	new *chiV1.ClickHouseInstallation
}

var _ queue.PriorityQueueItem = &ReconcileCHI{}

// Handle returns handle of the queue item
func (r ReconcileCHI) Handle() queue.T {
	if r.new != nil {
		return "ReconcileCHI" + ":" + r.new.Namespace + "/" + r.new.Name
	}
	if r.old != nil {
		return "ReconcileCHI" + ":" + r.old.Namespace + "/" + r.old.Name
	}
	return ""
}

// NewReconcileCHI creates new reconcile request queue item
func NewReconcileCHI(cmd string, old, new *chiV1.ClickHouseInstallation) *ReconcileCHI {
	return &ReconcileCHI{
		PriorityQueueItem: PriorityQueueItem{
			priority: priorityReconcileCHI,
		},
		cmd: cmd,
		old: old,
		new: new,
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
	cmd string
	old *chiV1.ClickHouseInstallationTemplate
	new *chiV1.ClickHouseInstallationTemplate
}

var _ queue.PriorityQueueItem = &ReconcileCHIT{}

// Handle returns handle of the queue item
func (r ReconcileCHIT) Handle() queue.T {
	if r.new != nil {
		return "ReconcileCHIT" + ":" + r.new.Namespace + "/" + r.new.Name
	}
	if r.old != nil {
		return "ReconcileCHIT" + ":" + r.old.Namespace + "/" + r.old.Name
	}
	return ""
}

// NewReconcileCHIT creates new reconcile CHI template queue item
func NewReconcileCHIT(cmd string, old, new *chiV1.ClickHouseInstallationTemplate) *ReconcileCHIT {
	return &ReconcileCHIT{
		PriorityQueueItem: PriorityQueueItem{
			priority: priorityReconcileCHIT,
		},
		cmd: cmd,
		old: old,
		new: new,
	}
}

// ReconcileChopConfig specifies CHOp config queue item
type ReconcileChopConfig struct {
	PriorityQueueItem
	cmd string
	old *chiV1.ClickHouseOperatorConfiguration
	new *chiV1.ClickHouseOperatorConfiguration
}

var _ queue.PriorityQueueItem = &ReconcileChopConfig{}

// Handle returns handle of the queue item
func (r ReconcileChopConfig) Handle() queue.T {
	if r.new != nil {
		return "ReconcileChopConfig" + ":" + r.new.Namespace + "/" + r.new.Name
	}
	if r.old != nil {
		return "ReconcileChopConfig" + ":" + r.old.Namespace + "/" + r.old.Name
	}
	return ""
}

// NewReconcileChopConfig creates new CHOp config queue item
func NewReconcileChopConfig(cmd string, old, new *chiV1.ClickHouseOperatorConfiguration) *ReconcileChopConfig {
	return &ReconcileChopConfig{
		PriorityQueueItem: PriorityQueueItem{
			priority: priorityReconcileChopConfig,
		},
		cmd: cmd,
		old: old,
		new: new,
	}
}

// ReconcileEndpoints specifies endpoint
type ReconcileEndpoints struct {
	PriorityQueueItem
	cmd string
	old *coreV1.Endpoints
	new *coreV1.Endpoints
}

var _ queue.PriorityQueueItem = &ReconcileEndpoints{}

// Handle returns handle of the queue item
func (r ReconcileEndpoints) Handle() queue.T {
	if r.new != nil {
		return "ReconcileEndpoints" + ":" + r.new.Namespace + "/" + r.new.Name
	}
	if r.old != nil {
		return "ReconcileEndpoints" + ":" + r.old.Namespace + "/" + r.old.Name
	}
	return ""
}

// NewReconcileEndpoints creates new reconcile endpoints queue item
func NewReconcileEndpoints(cmd string, old, new *coreV1.Endpoints) *ReconcileEndpoints {
	return &ReconcileEndpoints{
		PriorityQueueItem: PriorityQueueItem{
			priority: priorityReconcileEndpoints,
		},
		cmd: cmd,
		old: old,
		new: new,
	}
}

// DropDns specifies drop dns queue item
type DropDns struct {
	PriorityQueueItem
	initiator *metaV1.ObjectMeta
}

var _ queue.PriorityQueueItem = &DropDns{}

// Handle returns handle of the queue item
func (r DropDns) Handle() queue.T {
	if r.initiator != nil {
		return "DropDNS" + ":" + r.initiator.Namespace + "/" + r.initiator.Name
	}
	return ""
}

// NewDropDns creates new drop dns queue item
func NewDropDns(initiator *metaV1.ObjectMeta) *DropDns {
	return &DropDns{
		PriorityQueueItem: PriorityQueueItem{
			priority: priorityDropDNS,
		},
		initiator: initiator,
	}
}

// ReconcilePod specifies pod reconcile
type ReconcilePod struct {
	PriorityQueueItem
	cmd string
	old *coreV1.Pod
	new *coreV1.Pod
}

var _ queue.PriorityQueueItem = &ReconcileEndpoints{}

// Handle returns handle of the queue item
func (r ReconcilePod) Handle() queue.T {
	if r.new != nil {
		return "ReconcilePod" + ":" + r.new.Namespace + "/" + r.new.Name
	}
	if r.old != nil {
		return "ReconcilePod" + ":" + r.old.Namespace + "/" + r.old.Name
	}
	return ""
}

// NewReconcilePod creates new reconcile endpoints queue item
func NewReconcilePod(cmd string, old, new *coreV1.Pod) *ReconcilePod {
	return &ReconcilePod{
		cmd: cmd,
		old: old,
		new: new,
	}
}
