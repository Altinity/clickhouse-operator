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

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// deleteServiceCR
func (c *Controller) deleteServiceCR(ctx context.Context, cr api.ICustomResource) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	if templates, ok := cr.GetRootServiceTemplates(); ok {
		for _, template := range templates {
			serviceName := c.namer.Name(interfaces.NameCRService, cr, template)
			namespace := cr.GetNamespace()
			log.V(1).M(cr).F().Info("%s/%s", namespace, serviceName)
			c.deleteServiceIfExists(ctx, namespace, serviceName)
		}
	} else {
		serviceName := c.namer.Name(interfaces.NameCRService, cr)
		namespace := cr.GetNamespace()
		log.V(1).M(cr).F().Info("%s/%s", namespace, serviceName)
		c.deleteServiceIfExists(ctx, namespace, serviceName)
	}

	return nil
}
