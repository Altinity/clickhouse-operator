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
	"strconv"

	log "github.com/golang/glog"

	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Announcer handler all log/event/status messages going outside of controller/worker
type Announcer struct {
	v log.Level

	// writeLog specifies whether to write log file
	writeLog bool

	// file specifies file where logger is called from
	file string
	// line specifies line where logger is called from
	line int
	// function specifies function where logger is called from
	function string

	// prefix specifies prefix used by logger
	prefix string
}

// announcer which would be used in top-level functions, can be called as default
var announcer Announcer

// init creates default announcer
func init() {
	announcer = New()
}

const skip = "announcer.go"

// New creates new announcer
func New() Announcer {
	return Announcer{
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

// V is inspired by log.V()
func V(level log.Level) Announcer {
	return announcer.V(level)
}

func (a Announcer) F() Announcer {
	b := a
	b.writeLog = true
	_, _, b.function = util.Caller(skip)
	return b
}

func F() Announcer {
	return announcer.F()
}

func (a Announcer) L() Announcer {
	b := a
	b.writeLog = true
	_, b.line, _ = util.Caller(skip)
	return b
}

func L() Announcer {
	return announcer.L()
}

func (a Announcer) FL() Announcer {
	b := a
	b.writeLog = true
	b.file, _, _ = util.Caller(skip)
	return b
}

func FL() Announcer {
	return announcer.FL()
}

func (a Announcer) A() Announcer {
	b := a
	b.writeLog = true
	b.file, b.line, b.function = util.Caller(skip)
	return b
}

func A() Announcer {
	return announcer.A()
}

func (a Announcer) S() Announcer {
	b := a
	b.writeLog = true
	b.prefix = "start"
	b.file, b.line, b.function = util.Caller(skip)
	return b
}

func S() Announcer {
	return announcer.S()
}

func (a Announcer) E() Announcer {
	b := a
	b.writeLog = true
	b.prefix = "end"
	b.file, b.line, b.function = util.Caller(skip)
	return b
}

func E() Announcer {
	return announcer.E()
}

func (a Announcer) prependFormat(format string) string {
	if a.prefix != "" {
		format = a.prefix + ":" + format
	}
	if a.function != "" {
		format = a.function + ":" + format
	}
	if a.line != 0 {
		format = strconv.Itoa(a.line) + ":" + format
	}
	if a.file != "" {
		format = a.file + ":" + format
	}
	return format
}

// Info is inspired by log.Infof()
func (a Announcer) Info(format string, args ...interface{}) {
	// Produce classic log line
	if !a.writeLog {
		return
	}

	format = a.prependFormat(format)
	if a.v > 0 {
		if len(args) > 0 {
			log.V(a.v).Infof(format, args...)
		} else {
			log.V(a.v).Info(format)
		}
	} else {
		if len(args) > 0 {
			log.Infof(format, args...)
		} else {
			log.Info(format)
		}
	}
}

// Info is inspired by log.Infof()
func Info(format string, args ...interface{}) {
	announcer.Info(format, args...)
}

// Warning is inspired by log.Warningf()
func (a Announcer) Warning(format string, args ...interface{}) {
	// Produce classic log line
	if !a.writeLog {
		return
	}

	format = a.prependFormat(format)
	if len(args) > 0 {
		log.Warningf(format, args...)
	} else {
		log.Warning(format)
	}
}

// Warning is inspired by log.Warningf()
func Warning(format string, args ...interface{}) {
	announcer.Warning(format, args...)
}

// Error is inspired by log.Errorf()
func (a Announcer) Error(format string, args ...interface{}) {
	// Produce classic log line
	if !a.writeLog {
		return
	}

	format = a.prependFormat(format)
	if len(args) > 0 {
		log.Errorf(format, args...)
	} else {
		log.Error(format)
	}
}

// Error is inspired by log.Errorf()
func Error(format string, args ...interface{}) {
	announcer.Error(format, args...)
}

// Fatal is inspired by log.Fatalf()
func (a Announcer) Fatal(format string, args ...interface{}) {
	format = a.prependFormat(format)
	// Write and exit
	if len(args) > 0 {
		log.Fatalf(format, args...)
	} else {
		log.Fatal(format)
	}
}

// Fatal is inspired by log.Fatalf()
func Fatal(format string, args ...interface{}) {
	announcer.Fatal(format, args...)
}
