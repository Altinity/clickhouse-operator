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

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	apiChk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	commonTypes "github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

type CR struct {
	kubeClient client.Client
}

func NewCR(kubeClient client.Client) *CR {
	return &CR{
		kubeClient: kubeClient,
	}
}

func (c *CR) Get(ctx context.Context, namespace, name string) (api.ICustomResource, error) {
	cm := &apiChk.ClickHouseKeeperInstallation{}
	err := c.kubeClient.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, cm)
	if err == nil {
		return cm, nil
	} else {
		return nil, err
	}
}

// StatusUpdate updates CR object's Status
func (c *CR) StatusUpdate(ctx context.Context, cr api.ICustomResource, opts commonTypes.UpdateStatusOptions) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	return c.statusUpdateRetry(ctx, cr, opts)
}

func (c *CR) statusUpdateRetry(ctx context.Context, cr api.ICustomResource, opts commonTypes.UpdateStatusOptions) (err error) {
	for retry, attempt := true, 1; retry; attempt++ {
		if attempt > 60 {
			retry = false
		}

		err = c.statusUpdateProcess(ctx, cr, opts)
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

// statusUpdateProcess updates CR object's Status
func (c *CR) statusUpdateProcess(ctx context.Context, icr api.ICustomResource, opts commonTypes.UpdateStatusOptions) error {
	if util.IsContextDone(ctx) {
		log.V(2).Info("task is done")
		return nil
	}

	cr := icr.(*apiChk.ClickHouseKeeperInstallation)
	namespace, name := cr.NamespaceName()
	log.V(3).M(cr).F().Info("Update CR status")

	_cur, err := c.Get(ctx, namespace, name)
	cur := _cur.(*apiChk.ClickHouseKeeperInstallation)
	if err != nil {
		if opts.TolerateAbsence {
			return nil
		}
		log.V(1).M(cr).F().Error("%q", err)
		return err
	}
	if cur == nil {
		if opts.TolerateAbsence {
			return nil
		}
		log.V(1).M(cr).F().Error("NULL returned")
		return fmt.Errorf("ERROR GetCR (%s/%s): NULL returned", namespace, name)
	}

	// Update status of a real object.
	cur.EnsureStatus().CopyFrom(cr.Status, opts.CopyStatusOptions)

	err = c.statusUpdate(ctx, cur)
	if err != nil {
		// Error update
		log.V(2).M(cr).F().Info("Got error upon update, may retry. err: %q", err)
		return err
	}

	_cur, err = c.Get(ctx, namespace, name)
	cur = _cur.(*apiChk.ClickHouseKeeperInstallation)

	// Propagate updated ResourceVersion into chi
	if cr.GetResourceVersion() != cur.GetResourceVersion() {
		log.V(3).M(cr).F().Info("ResourceVersion change: %s to %s", cr.GetResourceVersion(), cur.GetResourceVersion())
		cr.SetResourceVersion(cur.GetResourceVersion())
		return nil
	}

	// ResourceVersion not changed - no update performed?

	return nil
}

func (c *CR) statusUpdate(ctx context.Context, chk *apiChk.ClickHouseKeeperInstallation) error {
	err := c.kubeClient.Status().Update(ctx, chk)
	return err
}
