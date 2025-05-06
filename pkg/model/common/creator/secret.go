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

package creator

import (
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/altinity/clickhouse-operator/pkg/util"
)

// CreateClusterSecret creates cluster secret
func (c *Creator) CreateClusterSecret(cluster api.ICluster) *core.Secret {
	return &core.Secret{
		ObjectMeta: meta.ObjectMeta{
			Namespace:       c.cr.GetNamespace(),
			Name:            c.nm.Name(interfaces.NameClusterAutoSecret, cluster),
			Labels:          c.macro.Scope(c.cr).Map(c.tagger.Label(interfaces.LabelSecret, cluster)),
			Annotations:     c.macro.Scope(c.cr).Map(c.tagger.Annotate(interfaces.AnnotateSecret, cluster)),
			OwnerReferences: c.or.CreateOwnerReferences(c.cr),
		},
		StringData: map[string]string{
			"secret": util.RandStringRange(10, 20),
		},
		Type: core.SecretTypeOpaque,
	}
}
