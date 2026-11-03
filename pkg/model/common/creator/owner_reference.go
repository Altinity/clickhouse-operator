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

package creator

import (
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type OwnerReferencer struct {
	APIVersion string
	Kind       string
}

func NewOwnerReferencer(apiVersion, kind string) *OwnerReferencer {
	return &OwnerReferencer{
		APIVersion: apiVersion,
		Kind:       kind,
	}
}

// CreateOwnerReferences gets MULTIPLE owner references
func (r *OwnerReferencer) CreateOwnerReferences(owner api.ICustomResource) []meta.OwnerReference {
	if owner.GetRuntime().GetAttributes().GetSkipOwnerRef() {
		return nil
	}
	return []meta.OwnerReference{
		r.createOwnerReference(owner),
	}
}

// createOwnerReference gets ONE owner reference
func (r *OwnerReferencer) createOwnerReference(m meta.Object) meta.OwnerReference {
	controller := true
	block := true
	return meta.OwnerReference{
		APIVersion:         r.APIVersion,
		Kind:               r.Kind,
		Name:               m.GetName(),
		UID:                m.GetUID(),
		Controller:         &controller,
		BlockOwnerDeletion: &block,
	}
}
