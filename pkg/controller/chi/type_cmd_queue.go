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
	chi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	reconcileAdd    = "add"
	reconcileUpdate = "update"
	reconcileDelete = "delete"
)

type Prioritier interface {
	Priority() int
}

type Handler interface {
	Handle() string
}

type PriorityQueueItem struct {
	priority int
}

func (i PriorityQueueItem) Priority() int {
	return i.priority
}

const (
	priorityReconcileCHI        int = 10
	priorityReconcileCHIT       int = 5
	priorityReconcileChopConfig int = 3
	priorityDropDNS             int = 7
)

type ReconcileCHI struct {
	PriorityQueueItem
	cmd string
	old *chi.ClickHouseInstallation
	new *chi.ClickHouseInstallation
}

func (r ReconcileCHI) Handle() string {
	if r.new != nil {
		return "ReconcileCHI" + ":" + r.new.Namespace + "/" + r.new.Name
	}
	if r.old != nil {
		return "ReconcileCHI" + ":" + r.old.Namespace + "/" + r.old.Name
	}
	return ""
}

func NewReconcileCHI(cmd string, old, new *chi.ClickHouseInstallation) *ReconcileCHI {
	return &ReconcileCHI{
		PriorityQueueItem: PriorityQueueItem{
			priority: priorityReconcileCHI,
		},
		cmd: cmd,
		old: old,
		new: new,
	}
}

type ReconcileCHIT struct {
	PriorityQueueItem
	cmd string
	old *chi.ClickHouseInstallationTemplate
	new *chi.ClickHouseInstallationTemplate
}

func (r ReconcileCHIT) Handle() string {
	if r.new != nil {
		return "ReconcileCHIT" + ":" + r.new.Namespace + "/" + r.new.Name
	}
	if r.old != nil {
		return "ReconcileCHIT" + ":" + r.old.Namespace + "/" + r.old.Name
	}
	return ""
}

func NewReconcileCHIT(cmd string, old, new *chi.ClickHouseInstallationTemplate) *ReconcileCHIT {
	return &ReconcileCHIT{
		PriorityQueueItem: PriorityQueueItem{
			priority: priorityReconcileCHIT,
		},
		cmd: cmd,
		old: old,
		new: new,
	}
}

type ReconcileChopConfig struct {
	PriorityQueueItem
	cmd string
	old *chi.ClickHouseOperatorConfiguration
	new *chi.ClickHouseOperatorConfiguration
}

func (r ReconcileChopConfig) Handle() string {
	if r.new != nil {
		return "ReconcileChopConfig" + ":" + r.new.Namespace + "/" + r.new.Name
	}
	if r.old != nil {
		return "ReconcileChopConfig" + ":" + r.old.Namespace + "/" + r.old.Name
	}
	return ""
}

func NewReconcileChopConfig(cmd string, old, new *chi.ClickHouseOperatorConfiguration) *ReconcileChopConfig {
	return &ReconcileChopConfig{
		PriorityQueueItem: PriorityQueueItem{
			priority: priorityReconcileChopConfig,
		},
		cmd: cmd,
		old: old,
		new: new,
	}
}

type DropDns struct {
	PriorityQueueItem
	initiator *v1.ObjectMeta
}

func (r DropDns) Handle() string {
	if r.initiator != nil {
		return "DropDNS" + ":" + r.initiator.Namespace + "/" + r.initiator.Name
	}
	return ""
}

func NewDropDns(initiator *v1.ObjectMeta) *DropDns {
	return &DropDns{
		PriorityQueueItem: PriorityQueueItem{
			priority: priorityDropDNS,
		},
		initiator: initiator,
	}
}
