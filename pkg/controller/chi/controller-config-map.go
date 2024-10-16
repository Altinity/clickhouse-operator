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
	"fmt"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	chiLabeler "github.com/altinity/clickhouse-operator/pkg/model/chi/tags/labeler"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// getConfigMap gets ConfigMap either by namespaced name or by labels
// TODO review byNameOnly params
func (c *Controller) getConfigMap(ctx context.Context, meta meta.Object, byNameOnly bool) (*core.ConfigMap, error) {
	// Check whether object with such name already exists
	configMap, err := c.kube.ConfigMap().Get(ctx, meta.GetNamespace(), meta.GetName())

	if (configMap != nil) && (err == nil) {
		// Object found by name
		return configMap, nil
	}

	if !apiErrors.IsNotFound(err) {
		// Error, which is not related to "Object not found"
		return nil, err
	}

	// Object not found by name

	if byNameOnly {
		return nil, err
	}

	// Try to find by labels

	set, err := chiLabeler.New(nil).MakeSetFromObjectMeta(meta)
	if err != nil {
		return nil, err
	}
	opts := controller.NewListOptions(set)

	configMaps, err := c.kube.ConfigMap().List(ctx, meta.GetNamespace(), opts)
	if err != nil {
		return nil, err
	}

	if len(configMaps) == 0 {
		return nil, apiErrors.NewNotFound(apps.Resource("ConfigMap"), meta.GetName())
	}

	if len(configMaps) == 1 {
		// Exactly one object found by labels
		return &configMaps[0], nil
	}

	// Too much objects found by labels
	return nil, fmt.Errorf("too much objects found %d expecting 1", len(configMaps))
}

func (c *Controller) createConfigMap(ctx context.Context, cm *core.ConfigMap) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	_, err := c.kube.ConfigMap().Create(ctx, cm)
	return err
}

func (c *Controller) updateConfigMap(ctx context.Context, cm *core.ConfigMap) (*core.ConfigMap, error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil, nil
	}

	return c.kube.ConfigMap().Update(ctx, cm)
}
