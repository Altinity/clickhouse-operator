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
		str += MessageDiffItemString("added items", "none", "", diff.Added)
	}

	if len(diff.Removed) > 0 {
		// Something removed
		str += MessageDiffItemString("removed items", "none", "", diff.Removed)
	}

	if len(diff.Modified) > 0 {
		// Something modified
		str += MessageDiffItemString("modified spec items", "none", "", diff.Modified)
	}

	return str
}

// MessageDiffItemString stringifies one map[*messagediff.Path]interface{} item
func MessageDiffItemString(bannerForDiff, bannerForNoDiff, defaultPath string, items map[*messagediff.Path]interface{}) (str string) {
	if len(items) == 0 {
		return bannerForNoDiff
	}

	// Have modified items
	str += fmt.Sprintf("Diff start -------------------------\n")
	str += fmt.Sprintf("%s num: %d\n", bannerForDiff, len(items))

	i := 0
	for pathPtr := range items {
		path := ""
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
			path += fmt.Sprintf("%v", pathNode)
		}
		if path == "" {
			path = defaultPath
		}

		valueShort := fmt.Sprintf("%+v", items[pathPtr])
		valueFull := fmt.Sprintf("%s", Dump(items[pathPtr]))
		value := ""
		if len(valueFull) < 300 {
			value = valueFull
		} else {
			value = valueShort
		}

		//str += fmt.Sprintf("diff item path [%d]:'%s'\n", i, path)
		//str += fmt.Sprintf("diff item value[%d]:'%s'\n", i, value)

		str += fmt.Sprintf("diff item [%d]:'%s' = '%s'\n", i, path, value)

		i++
	}
	str += fmt.Sprintf("Diff end -------------------------\n")

	return str
}
