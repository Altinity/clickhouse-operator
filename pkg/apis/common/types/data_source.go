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

package types

import (
	"strings"

	core "k8s.io/api/core/v1"
)

const (
	// MappingTypeVariable injects the Secret key as a container env var.
	// ClickHouse reads it with from_env. This is the default.
	MappingTypeVariable = "variable"
	// MappingTypeFile projects the Secret key into users.d as a ClickHouse users
	// XML document. The kubelet refreshes that file when the Secret changes, so
	// ClickHouse can reload it without a pod restart. Settings do not use this.
	MappingTypeFile = "file"
)

// DataSource is a set of possible data sources.
// mappingType selects how a secretKeyRef is presented to ClickHouse.
// It is a property of the reference, not of the Secret.
type DataSource struct {
	SecretKeyRef *core.SecretKeySelector `json:"secretKeyRef,omitempty" yaml:"secretKeyRef,omitempty"`
	MappingType  string                  `json:"mappingType,omitempty"  yaml:"mappingType,omitempty"`
}

// IsFile reports whether mappingType is file.
func (d *DataSource) IsFile() bool {
	if d == nil {
		return false
	}
	return strings.EqualFold(d.MappingType, MappingTypeFile)
}

func (in *DataSource) DeepCopy() *DataSource {
	if in == nil {
		return nil
	}
	out := new(DataSource)
	in.DeepCopyInto(out)
	return out
}

func (in *DataSource) DeepCopyInto(out *DataSource) {
	*out = *in
	if in.SecretKeyRef != nil {
		in, out := &in.SecretKeyRef, &out.SecretKeyRef
		*out = new(core.SecretKeySelector)
		(*in).DeepCopyInto(*out)
	}
}
