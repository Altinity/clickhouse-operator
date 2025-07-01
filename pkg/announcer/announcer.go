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
	"fmt"
	"strconv"

	log "github.com/golang/glog"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util/runtime"
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
	// meta specifies meta-information of the object, if required
	meta string
}

// skip specifies file name which to be skipped from address
const skip = "announcer.go"

// New creates new announcer
func New() Announcer {
	return Announcer{
		writeLog: true,
	}
}

// Silence produces silent announcer
func (a Announcer) Silence() Announcer {
	b := a
	b.writeLog = false
	return b
}

// V is inspired by log.V()
func (a Announcer) V(level log.Level) Announcer {
	b := a
	b.v = level
	return b
}

// F adds function name
func (a Announcer) F() Announcer {
	b := a
	_, _, b.function = runtime.Caller(skip)
	return b
}

// L adds line number
func (a Announcer) L() Announcer {
	b := a
	_, b.line, _ = runtime.Caller(skip)
	return b
}

// FL adds filename
func (a Announcer) FL() Announcer {
	b := a
	b.file, _, _ = runtime.Caller(skip)
	return b
}

// A adds full code address as 'file:line:function'
func (a Announcer) A() Announcer {
	b := a
	b.file, b.line, b.function = runtime.Caller(skip)
	return b
}

// S adds 'start of the function' tag, which includes:
// file, line, function and start prefix
func (a Announcer) S() Announcer {
	b := a
	b.prefix = "start"
	b.file, b.line, b.function = runtime.Caller(skip)
	return b
}

// E adds 'end of the function' tag, which includes:
// file, line, function and start prefix
func (a Announcer) E() Announcer {
	b := a
	b.prefix = "end"
	b.file, b.line, b.function = runtime.Caller(skip)
	return b
}

// M adds object meta as 'namespace/name'
func (a Announcer) M(m ...interface{}) Announcer {
	if len(m) == 0 {
		return a
	}

	b := a
	switch len(m) {
	case 1:
		switch typed := m[0].(type) {
		case string:
			b.meta = typed
			return b
		case *api.ClickHouseInstallation:
			if typed == nil {
				return a
			}
		case *api.Cluster:
			if typed == nil {
				return a
			}
		case *api.ChiShard:
			if typed == nil {
				return a
			}
		case *api.ChiReplica:
			if typed == nil {
				return a
			}
		case *api.Host:
			if typed == nil {
				return a
			}
		case *api.ClickHouseOperatorConfiguration:
			if typed == nil {
				return a
			}
		case *api.ClickHouseInstallationTemplate:
			if typed == nil {
				return a
			}
		}

		switch typed := m[0].(type) {
		case *api.ClickHouseInstallation:
			b.meta = fmt.Sprintf("CHI:%s/%s", typed.GetNamespace(), typed.GetName())
		case *api.Cluster:
			b.meta = fmt.Sprintf("Cluster:%s[%d]:%s/%s",
				typed.GetRuntime().GetAddress().GetClusterName(),
				typed.GetRuntime().GetAddress().GetClusterIndex(),
				typed.GetRuntime().GetAddress().GetNamespace(),
				typed.GetRuntime().GetAddress().GetCRName())
		case *api.ChiShard:
			b.meta = fmt.Sprintf("Shard:%s[%d]:%s/%s",
				typed.GetRuntime().GetAddress().GetShardName(),
				typed.GetRuntime().GetAddress().GetShardIndex(),
				typed.GetRuntime().GetAddress().GetNamespace(),
				typed.GetRuntime().GetAddress().GetCRName())
		case *api.ChiReplica:
			b.meta = fmt.Sprintf("Replica:%s[%d]:%s/%s",
				typed.GetRuntime().GetAddress().GetReplicaName(),
				typed.GetRuntime().GetAddress().GetReplicaIndex(),
				typed.GetRuntime().GetAddress().GetNamespace(),
				typed.GetRuntime().GetAddress().GetCRName())
		case *api.Host:
			b.meta = fmt.Sprintf("Host:%s[%d/%d]:%s/%s",
				typed.GetRuntime().GetAddress().GetHostName(),
				typed.GetRuntime().GetAddress().GetShardIndex(),
				typed.GetRuntime().GetAddress().GetReplicaIndex(),
				typed.GetRuntime().GetAddress().GetNamespace(),
				typed.GetRuntime().GetAddress().GetCRName())
		case *api.ClickHouseOperatorConfiguration:
			b.meta = fmt.Sprintf("ChopConfig:%s/%s", typed.GetNamespace(), typed.GetName())
		case *api.ClickHouseInstallationTemplate:
			b.meta = fmt.Sprintf("CHIT:%s/%s", typed.GetNamespace(), typed.GetName())
		default:
			b.meta = fmt.Sprintf("unknown")
		}
	case 2:
		namespace, _ := m[0].(string)
		name, _ := m[1].(string)
		b.meta = fmt.Sprintf("%s/%s", namespace, name)
	}
	return b
}

// P triggers log to print line
func (a Announcer) P() {
	a.Info("")
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

// prependFormat
func (a Announcer) prependFormat(format string) string {
	// Result format is expected to be 'file:line:function:prefix:meta:_start_format_'
	// Prepend each component in reverse order
	if a.meta != "" {
		if format == "" {
			format = a.meta
		} else {
			format = a.meta + ":" + format
		}
	}
	if a.prefix != "" {
		if format == "" {
			format = a.prefix
		} else {
			format = a.prefix + ":" + format
		}
	}
	if a.function != "" {
		if format == "" {
			format = a.function + "()"
		} else {
			format = a.function + "()" + ":" + format
		}
	}
	if a.line != 0 {
		if format == "" {
			format = strconv.Itoa(a.line)
		} else {
			format = strconv.Itoa(a.line) + ":" + format
		}
	}
	if a.file != "" {
		if format == "" {
			format = a.file
		} else {
			format = a.file + ":" + format
		}
	}
	return format
}
