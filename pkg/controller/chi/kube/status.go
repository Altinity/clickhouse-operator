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

package kube

import (
	"context"
	"fmt"
	"time"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	chopClientSet "github.com/altinity/clickhouse-operator/pkg/client/clientset/versioned"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/model/common/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type CRStatusClickHouse struct {
	chopClient chopClientSet.Interface
}

func NewCRStatusClickHouse(chopClient chopClientSet.Interface) *CRStatusClickHouse {
	return &CRStatusClickHouse{
		chopClient: chopClient,
	}
}

// updateCHIObjectStatus updates ClickHouseInstallation object's Status
func (c *CRStatusClickHouse) Update(ctx context.Context, cr api.ICustomResource, opts interfaces.UpdateStatusOptions) (err error) {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	for retry, attempt := true, 1; retry; attempt++ {
		if attempt > 60 {
			retry = false
		}

		err = c.doUpdateCRStatus(ctx, cr, opts)
		if err == nil {
			return nil
		}

		if retry {
			log.V(2).M(cr).F().Warning("got error, will retry. err: %q", err)
			time.Sleep(1 * time.Second)
		} else {
			log.V(1).M(cr).F().Error("got error, all retries are exhausted. err: %q", err)
		}
	}
	return
}

// doUpdateCRStatus updates ClickHouseInstallation object's Status
func (c *CRStatusClickHouse) doUpdateCRStatus(ctx context.Context, cr api.ICustomResource, opts interfaces.UpdateStatusOptions) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	chi := cr.(*api.ClickHouseInstallation)
	namespace, name := util.NamespaceName(chi)
	log.V(3).M(chi).F().Info("Update CHI status")

	cur, err := c.chopClient.ClickhouseV1().ClickHouseInstallations(namespace).Get(ctx, name, controller.NewGetOptions())
	if err != nil {
		if opts.TolerateAbsence {
			return nil
		}
		log.V(1).M(chi).F().Error("%q", err)
		return err
	}
	if cur == nil {
		if opts.TolerateAbsence {
			return nil
		}
		log.V(1).M(chi).F().Error("NULL returned")
		return fmt.Errorf("ERROR GetCR (%s/%s): NULL returned", namespace, name)
	}

	// Update status of a real object.
	cur.EnsureStatus().CopyFrom(chi.Status, opts.CopyStatusOptions)

	_new, err := c.chopClient.ClickhouseV1().ClickHouseInstallations(chi.Namespace).UpdateStatus(ctx, cur, controller.NewUpdateOptions())
	if err != nil {
		// Error update
		log.V(2).M(chi).F().Info("Got error upon update, may retry. err: %q", err)
		return err
	}

	// Propagate updated ResourceVersion into chi
	if chi.GetResourceVersion() != _new.GetResourceVersion() {
		log.V(3).M(chi).F().Info("ResourceVersion change: %s to %s", chi.GetResourceVersion(), _new.GetResourceVersion())
		chi.SetResourceVersion(_new.GetResourceVersion())
		return nil
	}

	// ResourceVersion not changed - no update performed?

	return nil
}
