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

package chi

import (
	"context"

	core "k8s.io/api/core/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// getSecret gets secret
func (c *Controller) getSecret(ctx context.Context, secret *core.Secret) (*core.Secret, error) {
	return c.kube.Secret().Get(ctx, secret)
}

func (c *Controller) createSecret(ctx context.Context, secret *core.Secret) error {
	log.V(1).M(secret).F().P()

	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	log.V(1).Info("Create Secret %s", util.NamespacedName(secret))
	if _, err := c.kube.Secret().Create(ctx, secret); err != nil {
		// Unable to create StatefulSet at all
		log.V(1).Error("Create Secret %s failed err: %v", util.NamespacedName(secret), err)
		return err
	}

	return nil
}
