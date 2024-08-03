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

	policy "k8s.io/api/policy/v1"
	core "k8s.io/api/core/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func (c *Controller) createSecret(ctx context.Context, secret *core.Secret) error {
	log.V(1).M(secret).F().P()

	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	log.V(1).Info("Create Secret %s/%s", secret.Namespace, secret.Name)
	if _, err := c.kubeClient.CoreV1().Secrets(secret.Namespace).Create(ctx, secret, controller.NewCreateOptions()); err != nil {
		// Unable to create StatefulSet at all
		log.V(1).Error("Create Secret %s/%s failed err:%v", secret.Namespace, secret.Name, err)
		return err
	}

	return nil
}

func (c *Controller) createConfigMap(ctx context.Context, cm *core.ConfigMap) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	_, err := c.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Create(ctx, cm, controller.NewCreateOptions())

	return err
}

func (c *Controller) updateConfigMap(ctx context.Context, cm *core.ConfigMap) (*core.ConfigMap, error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil, nil
	}

	return c.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, controller.NewUpdateOptions())
}

func (c *Controller) createPDB(ctx context.Context, pdb *policy.PodDisruptionBudget) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	_, err := c.kubeClient.PolicyV1().PodDisruptionBudgets(pdb.Namespace).Update(ctx, pdb, controller.NewUpdateOptions())

	return err
}

func (c *Controller) updatePDB(ctx context.Context, pdb *policy.PodDisruptionBudget) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	_, err := c.kubeClient.PolicyV1().PodDisruptionBudgets(pdb.Namespace).Update(ctx, pdb, controller.NewUpdateOptions())

	return err
}

func (c *Controller) createService( ctx context.Context, service *core.Service) error {
	_, err := c.kubeClient.CoreV1().Services(service.Namespace).Create(ctx, service, controller.NewCreateOptions())
	return err
}

func (c *Controller) updateService( ctx context.Context, service *core.Service) error {
	_, err := c.kubeClient.CoreV1().Services(service.GetNamespace()).Update(ctx, service, controller.NewUpdateOptions())
	return err
}
