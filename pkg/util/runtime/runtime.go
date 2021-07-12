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

package runtime

import (
	"path"
	"runtime"
	"strings"
)

// Caller returns triplet: file name, line number, function name of a caller
func Caller(skip string) (string, int, string) {
	pc := make([]uintptr, 7)
	n := runtime.Callers(2, pc)
	frames := runtime.CallersFrames(pc[:n])
	for {
		frame, more := frames.Next()
		// frame.File = /tmp/sandbox469341579/prog.go
		// frame.Line = 28
		// frame.Function = main.Announcer.Info

		// file = prog.go
		file := path.Base(frame.File)
		// function = Info
		function := path.Base(strings.Replace(frame.Function, ".", "/", -1))

		if file != skip {
			return file, frame.Line, function
		}

		if !more {
			break
		}
	}
	return "", 0, ""
}
