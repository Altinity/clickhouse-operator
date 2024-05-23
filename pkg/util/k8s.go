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
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NamespaceName returns namespace and anme from the meta
func NamespaceName(meta meta.Object) (string, string) {
	return meta.GetNamespace(), meta.GetName()
}

// NamespaceNameString returns namespace and name as one string
func NamespaceNameString(meta meta.Object) string {
	return meta.GetNamespace() + "/" + meta.GetName()
}

// AnnotationsToBeSkipped kubectl service annotation that we'd like to skip
var AnnotationsToBeSkipped = []string{
	"kubectl.kubernetes.io/last-applied-configuration",
}

// IsAnnotationToBeSkipped checks whether an annotation should be skipped
func IsAnnotationToBeSkipped(annotation string) bool {
	for _, a := range AnnotationsToBeSkipped {
		if a == annotation {
			return true
		}
	}
	return false
}

// ListSkippedAnnotations provides list of annotations that should be skipped
func ListSkippedAnnotations() []string {
	return AnnotationsToBeSkipped
}
