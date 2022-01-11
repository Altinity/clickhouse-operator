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

	"gopkg.in/d4l3k/messagediff.v1"
)

// MessageDiffString stringifies message diff
func MessageDiffString(diff *messagediff.Diff, equal bool) string {
	if equal {
		return ""
	}

	str := ""

	if len(diff.Added) > 0 {
		// Something added
		str += MessageDiffItemString("added items", diff.Added)
	}

	if len(diff.Removed) > 0 {
		// Something removed
		str += MessageDiffItemString("removed items", diff.Removed)
	}

	if len(diff.Modified) > 0 {
		// Something modified
		str += MessageDiffItemString("modified spec items", diff.Modified)
	}

	return str
}

// MessageDiffItemString stringifies one map[*messagediff.Path]interface{} item
func MessageDiffItemString(banner string, items map[*messagediff.Path]interface{}) string {
	var str string
	str += fmt.Sprintf("AP item start -------------------------\n")
	str += fmt.Sprintf("%s: %d\n", banner, len(items))
	i := 0
	for pathPtr := range items {
		str += fmt.Sprintf("ap item path [%d]:", i)
		for _, pathNode := range *pathPtr {
			// Format
			//	.Template
			//	.Spec
			//	.Containers
			//	[0]
			//	.Ports
			//	[1]
			//	.Protocol
			// as
			//	.Template.Spec.Containers[0].Ports[1].Protocol
			str += fmt.Sprintf("%v", pathNode)
		}
		str += fmt.Sprintf("\n")
		str += fmt.Sprintf("ap item value[%d]:", i)
		str += fmt.Sprintf("'%+v'\n", items[pathPtr])
		i++
	}
	str += fmt.Sprintf("AP item end -------------------------\n")

	return str
}
