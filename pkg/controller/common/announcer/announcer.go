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

package announcer

import (
	"context"
	"fmt"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"time"

	log "github.com/golang/glog"

	a "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
)

// Announcer handler all log/event/status messages going outside of controller/worker
type Announcer struct {
	a.Announcer

	eventEmitter  interfaces.IEventEmitter
	statusUpdater interfaces.IKubeCR
	cr            api.ICustomResource

	// writeEvent specifies whether to produce k8s event into chi, therefore requires chi to be specified
	// See k8s event for details.
	// https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/event-v1/
	writeEvent bool
	// eventAction specifies k8s event action
	eventAction string
	// event reason specifies k8s event reason
	eventReason string

	// writeAction specifies whether to produce action into `ClickHouseInstallation.Status.Action` of chi,
	// therefore requires chi to be specified
	writeAction bool
	// writeActions specifies whether to produce action into `ClickHouseInstallation.Status.Actions` of chi,
	// therefore requires chi to be specified
	writeActions bool
	// writeError specifies whether to produce action into `ClickHouseInstallation.Status.Error` of chi,
	// therefore requires chi to be specified
	writeError bool
}

// NewAnnouncer creates new announcer
func NewAnnouncer(eventEmitter interfaces.IEventEmitter, statusUpdater interfaces.IKubeCR) Announcer {
	return Announcer{
		Announcer:     a.New(),
		eventEmitter:  eventEmitter,
		statusUpdater: statusUpdater,
	}
}

// Silence produces silent announcer
func (a Announcer) Silence() Announcer {
	b := a
	b.Announcer = b.Announcer.Silence()
	return b
}

// V is inspired by log.V()
func (a Announcer) V(level log.Level) Announcer {
	b := a
	b.Announcer = b.Announcer.V(level)
	return b
}

// F adds function name
func (a Announcer) F() Announcer {
	b := a
	b.Announcer = b.Announcer.F()
	return b
}

// L adds line number
func (a Announcer) L() Announcer {
	b := a
	b.Announcer = b.Announcer.L()
	return b
}

// FL adds filename
func (a Announcer) FL() Announcer {
	b := a
	b.Announcer = b.Announcer.FL()
	return b
}

// A adds full code address as 'file:line:function'
func (a Announcer) A() Announcer {
	b := a
	b.Announcer = b.Announcer.A()
	return b
}

// S adds 'start of the function' tag
func (a Announcer) S() Announcer {
	b := a
	b.Announcer = b.Announcer.S()
	return b
}

// E adds 'end of the function' tag
func (a Announcer) E() Announcer {
	b := a
	b.Announcer = b.Announcer.E()
	return b
}

// M adds object meta as 'namespace/name'
func (a Announcer) M(m ...interface{}) Announcer {
	b := a
	b.Announcer = b.Announcer.M(m...)
	return b
}

// P triggers log to print line
func (a Announcer) P() {
	a.Info("")
}

// Info is inspired by log.Infof()
func (a Announcer) Info(format string, args ...interface{}) {
	// Produce classic log line
	a.Announcer.Info(format, args...)

	// Produce k8s event
	a.emitEvent("info", format, args...)

	// Produce chi status record
	a.writeStatus(format, args...)
}

// Warning is inspired by log.Warningf()
func (a Announcer) Warning(format string, args ...interface{}) {
	// Produce classic log line
	a.Announcer.Warning(format, args...)

	// Produce k8s event
	a.emitEvent("warning", format, args...)

	// Produce chi status record
	a.writeStatus(format, args...)
}

// Error is inspired by log.Errorf()
func (a Announcer) Error(format string, args ...interface{}) {
	// Produce classic log line
	a.Announcer.Error(format, args...)

	// Produce k8s event
	a.emitEvent("error", format, args...)

	// Produce chi status record
	a.writeStatus(format, args...)
}

// Fatal is inspired by log.Fatalf()
func (a Announcer) Fatal(format string, args ...interface{}) {
	// Produce k8s event
	a.emitEvent("error", format, args...)

	// Produce chi status record
	a.writeStatus(format, args...)

	// Write and exit
	a.Announcer.Fatal(format, args...)
}

// WithEvent is used in chained calls in order to produce event into `chi`
func (a Announcer) WithEvent(cr api.ICustomResource, action string, reason string) Announcer {
	b := a
	if cr == nil {
		b.writeEvent = false
		b.cr = nil
		b.eventAction = ""
		b.eventReason = ""
	} else {
		b.writeEvent = true
		b.cr = cr
		b.eventAction = action
		b.eventReason = reason
	}
	return b
}

// WithAction is used in chained calls in order to produce action into `ClickHouseInstallation.Status.Action`
func (a Announcer) WithAction(cr api.ICustomResource) Announcer {
	b := a
	if cr == nil {
		b.cr = nil
		b.writeAction = false
	} else {
		b.cr = cr
		b.writeAction = true
	}
	return b
}

// WithActions is used in chained calls in order to produce action in ClickHouseInstallation.Status.Actions
func (a Announcer) WithActions(cr api.ICustomResource) Announcer {
	b := a
	if cr == nil {
		b.cr = nil
		b.writeActions = false
	} else {
		b.cr = cr
		b.writeActions = true
	}
	return b
}

// WithError is used in chained calls in order to produce error in ClickHouseInstallation.Status.Error
func (a Announcer) WithError(cr api.ICustomResource) Announcer {
	b := a
	if cr == nil {
		b.cr = nil
		b.writeError = false
	} else {
		b.cr = cr
		b.writeError = true
	}
	return b
}

// capable checks whether announcer is capable to produce chi-based announcements
func (a Announcer) capable() bool {
	return (a.eventEmitter != nil) && (a.cr != nil)
}

func (a Announcer) emitEvent(level string, format string, args ...interface{}) {
	if !a.capable() {
		return
	}
	if !a.writeEvent {
		return
	}
	if len(args) > 0 {
		a.eventEmitter.Event(level, a.cr, a.eventAction, a.eventReason, fmt.Sprintf(format, args...))
	} else {
		a.eventEmitter.Event(level, a.cr, a.eventAction, a.eventReason, fmt.Sprint(format))
	}
}

// writeStatus is internal function which writes ClickHouseInstallation.Status
func (a Announcer) writeStatus(format string, args ...interface{}) {
	if !a.capable() {
		return
	}

	now := time.Now()
	prefix := now.Format(time.RFC3339Nano) + " "
	shouldUpdateStatus := false

	shouldUpdateStatus = shouldUpdateStatus || a._writeActions(prefix, format, args...)
	shouldUpdateStatus = shouldUpdateStatus || a._writeErrors(prefix, format, args...)

	// Propagate status updates into object
	if shouldUpdateStatus {
		_ = a.statusUpdater.StatusUpdate(context.Background(), a.cr, types.UpdateStatusOptions{
			TolerateAbsence: true,
			CopyStatusOptions: types.CopyStatusOptions{
				CopyStatusFieldGroup: types.CopyStatusFieldGroup{
					FieldGroupActions: true,
					FieldGroupErrors:  true,
				},
			},
		})
	}
}

func (a Announcer) _writeActions(prefix, format string, args ...interface{}) (shouldUpdateStatus bool) {
	if a.writeAction {
		if chop.Config().Status.Fields.Action.IsTrue() {
			shouldUpdateStatus = true
			if len(args) > 0 {
				a.cr.IEnsureStatus().SetAction(fmt.Sprintf(format, args...))
			} else {
				a.cr.IEnsureStatus().SetAction(fmt.Sprint(format))
			}
		}
	}
	if a.writeActions {
		if chop.Config().Status.Fields.Actions.IsTrue() {
			shouldUpdateStatus = true
			if len(args) > 0 {
				a.cr.IEnsureStatus().PushAction(prefix + fmt.Sprintf(format, args...))
			} else {
				a.cr.IEnsureStatus().PushAction(prefix + fmt.Sprint(format))
			}
		}
	}
	return
}

func (a Announcer) _writeErrors(prefix, format string, args ...interface{}) (shouldUpdateStatus bool) {
	if a.writeError {
		if chop.Config().Status.Fields.Error.IsTrue() {
			shouldUpdateStatus = true
			if len(args) > 0 {
				// PR review question: should we prefix the string in the SetError call? If so, we can SetAndPushError.
				a.cr.IEnsureStatus().SetError(fmt.Sprintf(format, args...))
			} else {
				a.cr.IEnsureStatus().SetError(fmt.Sprint(format))
			}
		}
	}
	if a.writeError {
		if chop.Config().Status.Fields.Errors.IsTrue() {
			shouldUpdateStatus = true
			if len(args) > 0 {
				a.cr.IEnsureStatus().PushError(prefix + fmt.Sprintf(format, args...))
			} else {
				a.cr.IEnsureStatus().PushError(prefix + fmt.Sprint(format))
			}
		}
	}
	return
}
