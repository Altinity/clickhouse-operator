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
	"reflect"
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

// announcer which would be used in top-level functions, can be called as a 'default announcer'
var announcer Announcer

// init creates default announcer
func init() {
	announcer = New()
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

// Silence produces silent announcer
func Silence() Announcer {
	return announcer.Silence()
}

// V is inspired by log.V()
func (a Announcer) V(level log.Level) Announcer {
	b := a
	b.v = level
	return b
}

// V is inspired by log.V()
func V(level log.Level) Announcer {
	return announcer.V(level)
}

// F adds function name
func (a Announcer) F() Announcer {
	b := a
	_, _, b.function = runtime.Caller(skip)
	return b
}

// F adds function name
func F() Announcer {
	return announcer.F()
}

// L adds line number
func (a Announcer) L() Announcer {
	b := a
	_, b.line, _ = runtime.Caller(skip)
	return b
}

// L adds line number
func L() Announcer {
	return announcer.L()
}

// FL adds filename
func (a Announcer) FL() Announcer {
	b := a
	b.file, _, _ = runtime.Caller(skip)
	return b
}

// FL adds filename
func FL() Announcer {
	return announcer.FL()
}

// A adds full code address as 'file:line:function'
func (a Announcer) A() Announcer {
	b := a
	b.file, b.line, b.function = runtime.Caller(skip)
	return b
}

// A adds full code address as 'file:line:function'
func A() Announcer {
	return announcer.A()
}

// S adds 'start of the function' tag, which includes:
// file, line, function and start prefix
func (a Announcer) S() Announcer {
	b := a
	b.prefix = "start"
	b.file, b.line, b.function = runtime.Caller(skip)
	return b
}

// S adds 'start of the function' tag, which includes:
// file, line, function and start prefix
func S() Announcer {
	return announcer.S()
}

// E adds 'end of the function' tag, which includes:
// file, line, function and start prefix
func (a Announcer) E() Announcer {
	b := a
	b.prefix = "end"
	b.file, b.line, b.function = runtime.Caller(skip)
	return b
}

// E adds 'end of the function' tag, which includes:
// file, line, function and start prefix
func E() Announcer {
	return announcer.E()
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
		case *api.ClickHouseInstallation:
			if typed == nil {
				return a
			}
			b.meta = typed.Namespace + "/" + typed.Name
			if typed.Spec.HasTaskID() {
				b.meta += "/" + typed.Spec.GetTaskID()
			}
		default:
			if meta, ok := a.tryToFindNamespaceNameEverywhere(m[0]); ok {
				b.meta = meta
			} else {
				return a
			}
		}
	case 2:
		namespace, _ := m[0].(string)
		name, _ := m[1].(string)
		b.meta = namespace + "/" + name
	}
	return b
}

// M adds object meta as 'namespace/name'
func M(m ...interface{}) Announcer {
	return announcer.M(m...)
}

// P triggers log to print line
func (a Announcer) P() {
	a.Info("")
}

// P triggers log to print line
func P() {
	announcer.P()
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

// tryToFindNamespaceNameEverywhere
func (a Announcer) tryToFindNamespaceNameEverywhere(m interface{}) (string, bool) {
	if meta, ok := a.findNamespaceName(m); ok {
		return meta, ok
	}
	if meta, ok := a.findCHI(m); ok {
		return meta, ok
	}
	return "", false
}

// findInObjectMeta
func (a Announcer) findNamespaceName(m interface{}) (string, bool) {
	if m == nil {
		return "", false
	}
	value := reflect.ValueOf(m)
	if !value.IsValid() || value.IsZero() || ((value.Kind() == reflect.Ptr) && value.IsNil()) {
		return "", false
	}
	var namespace, name reflect.Value
	if value.Kind() == reflect.Ptr {
		namespace = value.Elem().FieldByName("Namespace")
		name = value.Elem().FieldByName("Name")
	} else {
		namespace = value.FieldByName("Namespace")
		name = value.FieldByName("Name")
	}
	if !namespace.IsValid() {
		return "", false
	}
	if !name.IsValid() {
		return "", false
	}
	return namespace.String() + "/" + name.String(), true
}

// findCHI
func (a Announcer) findCHI(m interface{}) (string, bool) {
	if m == nil {
		return "", false
	}
	value := reflect.ValueOf(m)
	if !value.IsValid() || value.IsZero() || ((value.Kind() == reflect.Ptr) && value.IsNil()) {
		return "", false
	}
	// Find CHI
 var _chi reflect.Value
	if value.Kind() == reflect.Ptr {
		_chi = value.Elem().FieldByName("CHI")
	} else {
		_chi = value.FieldByName("CHI")
	}
	if !_chi.IsValid() || _chi.IsZero() || ((_chi.Kind() == reflect.Ptr) && _chi.IsNil()) {
		return "", false
	}

	// Cast to CHI
	chi, ok := _chi.Interface().(api.ClickHouseInstallation)
	if !ok {
		return "", false
	}
	res := chi.Namespace + "/" + chi.Name
	if chi.Spec.HasTaskID() {
		res += "/" + chi.Spec.GetTaskID()
	}
	return res, true
}
