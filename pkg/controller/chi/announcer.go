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
	"fmt"

	log "github.com/golang/glog"
	// log "k8s.io/klog"

	chop "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// Announcer handler all log/event/status messages going outside of controller/worker
type Announcer struct {
	c                  *Controller
	chi                *chop.ClickHouseInstallation
	v                  log.Level
	writeLog           bool
	writeEvent         bool
	eventAction        string
	eventReason        string
	writeStatusAction  bool
	writeStatusActions bool
	writeStatusError   bool
}

// NewAnnouncer creates new announcer
func NewAnnouncer(c *Controller) Announcer {
	return Announcer{
		c:        c,
		writeLog: true,
	}
}

// V is inspired by log.V()
func (a Announcer) V(level log.Level) Announcer {
	b := a
	b.v = level
	b.writeLog = true
	return b
}

// WithEvent is used in chained calls in order to produce event
func (a Announcer) WithEvent(
	chi *chop.ClickHouseInstallation,
	action string,
	reason string,
) Announcer {
	b := a
	b.writeEvent = true
	b.chi = chi
	b.eventAction = action
	b.eventReason = reason
	return b
}

// WithStatusAction is used in chained calls in order to produce action in ClickHouseInstallation.Status.Action
func (a Announcer) WithStatusAction(chi *chop.ClickHouseInstallation) Announcer {
	b := a
	b.writeStatusAction = true
	b.writeStatusActions = true
	b.chi = chi
	return b
}

// WithStatusActions is used in chained calls in order to produce action in ClickHouseInstallation.Status.Actions
func (a Announcer) WithStatusActions(chi *chop.ClickHouseInstallation) Announcer {
	b := a
	b.writeStatusActions = true
	b.chi = chi
	return b
}

// WithStatusAction is used in chained calls in order to produce error in ClickHouseInstallation.Status.Error
func (a Announcer) WithStatusError(chi *chop.ClickHouseInstallation) Announcer {
	b := a
	b.writeStatusError = true
	b.chi = chi
	return b
}

// Info is inspired by log.Infof()
func (a Announcer) Info(format string, args ...interface{}) {
	if a.writeLog {
		if a.v > 0 {
			log.V(a.v).Infof(format, args...)
		} else {
			log.Infof(format, args...)
		}
	}
	if a.writeEvent {
		a.c.eventInfo(a.chi, a.eventAction, a.eventReason, fmt.Sprintf(format, args...))
	}
	a.writeCHIStatus(format, args...)
}

// Warning is inspired by log.Warningf()
func (a Announcer) Warning(format string, args ...interface{}) {
	if a.writeLog {
		log.Warningf(format, args...)
	}
	if a.writeEvent {
		a.c.eventWarning(a.chi, a.eventAction, a.eventReason, fmt.Sprintf(format, args...))
	}
	a.writeCHIStatus(format, args...)
}

// Error is inspired by log.Errorf()
func (a Announcer) Error(format string, args ...interface{}) {
	if a.writeLog {
		log.Errorf(format, args...)
	}
	if a.writeEvent {
		a.c.eventError(a.chi, a.eventAction, a.eventReason, fmt.Sprintf(format, args...))
	}
	a.writeCHIStatus(format, args...)
}

// writeCHIStatus is internal function which writes ClickHouseInstallation.Status
func (a Announcer) writeCHIStatus(format string, args ...interface{}) {
	if a.writeStatusAction {
		a.chi.Status.Action = fmt.Sprintf(format, args...)
	}
	if a.writeStatusActions {
		(&a.chi.Status).PushAction(fmt.Sprintf(format, args...))
	}
	if a.writeStatusError {
		(&a.chi.Status).SetAndPushError(fmt.Sprintf(format, args...))
	}

	// Propagate status updates into object
	if a.writeStatusAction || a.writeStatusActions || a.writeStatusError {
		_ = a.c.updateCHIObjectStatus(a.chi, true)
	}
}
