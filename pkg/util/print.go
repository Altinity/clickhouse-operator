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

package util

import (
	"fmt"
	"io"
	"strconv"
)

// Iline writes indented line with \n into provided io.Writer
func Iline(w io.Writer, indent int, format string, a ...interface{}) {
	if indent > 0 {
		// Prepare indentation template %16s
		template := "%" + strconv.Itoa(indent) + "s"
		Fprintf(w, template, " ")
	}
	Fprintf(w, format, a...)
	Fprintf(w, "\n")
}

// Fprintf suppresses warning for unused returns of fmt.Fprintf()
func Fprintf(w io.Writer, format string, a ...interface{}) {
	_, _ = fmt.Fprintf(w, format, a...)
}
