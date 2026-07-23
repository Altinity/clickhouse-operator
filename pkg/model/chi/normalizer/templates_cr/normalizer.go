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

package templates_cr

import (
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// NormalizeTemplateRefList normalizes list of templates use specifications
func NormalizeTemplateRefList(templates []*api.TemplateRef) []*api.TemplateRef {
	for i := range templates {
		templates[i] = normalizeTemplateRef(templates[i])
	}
	return templates
}

// normalizeTemplateRef normalizes TemplateRef
func normalizeTemplateRef(templateRef *api.TemplateRef) *api.TemplateRef {
	// Check Name
	if templateRef.Name == "" {
		// This is strange, don't know what to do in this case
	}

	// Check Namespace
	if templateRef.Namespace == "" {
		// So far do nothing with empty namespace
	}

	// Ensure UseType — fold any accepted casing to the canonical const; unknown values
	// fall back to the default. Kept as a switch so future use types are just new cases.
	switch util.FoldEnum(templateRef.UseType, UseTypeMerge) {
	case UseTypeMerge:
		templateRef.UseType = UseTypeMerge
	default:
		// Unknown use type - overwrite with default value
		templateRef.UseType = UseTypeMerge
	}

	return templateRef
}
