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

package operator

import (
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func GetLabelsFromSource(src labelsSource) (labels map[string]string) {
	return util.MergeStringMapsOverwrite(
		util.MergeStringMapsOverwrite(
			util.MergeStringMapsOverwrite(labels, getLabelsFromName(src)),
			getLabelsFromLabels(src),
		),
		getLabelsFromAnnotations(src),
	)
}

func getLabelsFromName(chi labelsSource) map[string]string {
	return map[string]string{
		"chi":       chi.GetName(),
		"namespace": chi.GetNamespace(),
	}
}

func getLabelsFromLabels(chi labelsSource) map[string]string {
	return chi.GetLabels()
}

func getLabelsFromAnnotations(chi labelsSource) map[string]string {
	// Exclude skipped annotations
	return util.CopyMapFilter(
		chi.GetAnnotations(),
		nil,
		util.ListSkippedAnnotations(),
	)
}
