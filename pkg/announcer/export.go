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
	log "github.com/golang/glog"
)

// announcer which would be used in top-level functions, can be called as a 'default announcer'
var announcer Announcer

// init creates default announcer
func init() {
	announcer = New()
}

// Silence produces silent announcer
func Silence() Announcer {
	return announcer.Silence()
}

// V is inspired by log.V()
func V(level log.Level) Announcer {
	return announcer.V(level)
}

// F adds function name
func F() Announcer {
	return announcer.F()
}

// L adds line number
func L() Announcer {
	return announcer.L()
}

// FL adds filename
func FL() Announcer {
	return announcer.FL()
}

// A adds full code address as 'file:line:function'
func A() Announcer {
	return announcer.A()
}

// S adds 'start of the function' tag, which includes:
// file, line, function and start prefix
func S() Announcer {
	return announcer.S()
}

// E adds 'end of the function' tag, which includes:
// file, line, function and start prefix
func E() Announcer {
	return announcer.E()
}

// M adds object meta as 'namespace/name'
func M(m ...interface{}) Announcer {
	return announcer.M(m...)
}

// P triggers log to print line
func P() {
	announcer.P()
}

// Info is inspired by log.Infof()
func Info(format string, args ...interface{}) {
	announcer.Info(format, args...)
}

// Warning is inspired by log.Warningf()
func Warning(format string, args ...interface{}) {
	announcer.Warning(format, args...)
}

// Error is inspired by log.Errorf()
func Error(format string, args ...interface{}) {
	announcer.Error(format, args...)
}

// Fatal is inspired by log.Fatalf()
func Fatal(format string, args ...interface{}) {
	announcer.Fatal(format, args...)
}
