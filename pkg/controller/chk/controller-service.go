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

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

func (c *Controller) getService(ctx context.Context, service *core.Service) (*core.Service, error) {
	return c.kube.Service().Get(ctx, service)
}

func (c *Controller) createService(ctx context.Context, service *core.Service) error {
	_, err := c.kube.Service().Create(ctx, service)
	return err
}

func (c *Controller) updateService(ctx context.Context, service *core.Service) error {
	_, err := c.kube.Service().Update(ctx, service)
	return err
}

// deleteServiceIfExists deletes Service in case it does not exist
func (c *Controller) deleteServiceIfExists(ctx context.Context, namespace, name string) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	// Check specified service exists
	_, err := c.kube.Service().Get(ctx, &core.Service{
		ObjectMeta: meta.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
	})

	if err != nil {
		// No such a service, nothing to delete
		log.V(1).M(namespace, name).F().Info("Not Found Service: %s/%s err: %v", namespace, name, err)
		return nil
	}

	// Delete service
	err = c.kube.Service().Delete(ctx, namespace, name)
	if err == nil {
		log.V(1).M(namespace, name).F().Info("OK delete Service: %s/%s", namespace, name)
	} else {
		log.V(1).M(namespace, name).F().Error("FAIL delete Service: %s/%s err:%v", namespace, name, err)
	}

	return err
}
