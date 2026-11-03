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

package chk

import (
	"context"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// getConfigMap gets ConfigMap
func (c *Controller) getConfigMap(ctx context.Context, meta meta.Object) (*core.ConfigMap, error) {
	return c.kube.ConfigMap().Get(ctx, meta.GetNamespace(), meta.GetName())
}

func (c *Controller) createConfigMap(ctx context.Context, cm *core.ConfigMap) error {
	_, err := c.kube.ConfigMap().Create(ctx, cm)

	return err
}

func (c *Controller) updateConfigMap(ctx context.Context, cm *core.ConfigMap) (*core.ConfigMap, error) {
	return c.kube.ConfigMap().Update(ctx, cm)
}
